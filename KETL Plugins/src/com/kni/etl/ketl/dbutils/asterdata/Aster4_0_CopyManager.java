package com.kni.etl.ketl.dbutils.asterdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;

import com.asterdata.ncluster.Driver;
import com.asterdata.ncluster.core.BaseConnection;
import com.asterdata.ncluster.core.Encoding;
import com.asterdata.ncluster.core.Logger;
import com.asterdata.ncluster.core.Notification;
import com.asterdata.ncluster.core.PGStream;
import com.asterdata.ncluster.core.QueryExecutor;
import com.asterdata.ncluster.core.v3.QueryExecutorImpl;
import com.asterdata.ncluster.util.GT;
import com.asterdata.ncluster.util.NClusterException;
import com.asterdata.ncluster.util.NClusterState;
import com.asterdata.ncluster.util.NClusterWarning;
import com.asterdata.ncluster.util.ServerErrorMessage;

/**
 * Implement COPY support in the JDBC driver. This requires a 7.4 server and a connection with the V3 protocol. Previous
 * versions could not recover from errors and the connection had to be abandoned which was not acceptable.
 */

public class Aster4_0_CopyManager implements CopyManagerInterface {

	private BaseConnection pgConn;

	private PGStream pgStream;

	private Logger logger;

	private SQLWarning warnings;

	private final ArrayList notifications = new ArrayList();

	private Encoding encoder;

	private int bufSize = 8192;

	private byte buf[] = new byte[this.bufSize];

	public Aster4_0_CopyManager(Connection con) throws SQLException {
		BaseConnection pgConn = (BaseConnection) con;
		QueryExecutor executor = pgConn.getQueryExecutor();
		Field field;
		try {
			field = QueryExecutorImpl.class.getDeclaredField("pgStream");

			field.setAccessible(true);

			this.pgConn = pgConn;
			this.pgStream = (PGStream) field.get(executor);
			this.logger = pgConn.getLogger();
			this.encoder = this.pgStream.getEncoding();
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#setCopyBufferSize(int)
	 */
	public void setCopyBufferSize(int arg0) {
		this.bufSize = arg0;
		buf = new byte[this.bufSize];
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#encodeString(java.lang.String)
	 */
	public byte[] encodeString(String arg0) throws IOException {
		return this.encoder.encode(arg0);
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#getEncoding()
	 */
	public String getEncoding() {
		return this.encoder.name();
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyIn(java.lang.String, java.io.InputStream)
	 */
	public void copyIn(String table, InputStream is) throws SQLException {
		copyInQuery("COPY " + table + " FROM STDIN", is);
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyInQuery(java.lang.String, java.io.InputStream)
	 */
	public void copyInQuery(String query, InputStream is) throws SQLException {

		synchronized (pgStream) {

			sendQuery(query);
			try {
				copyResultLoop(is, null);
			} catch (IOException ex) {
				throw new NClusterException(GT.tr("postgresql.copy.type"), NClusterState.COMMUNICATION_ERROR, ex);

			}
		}

	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyOut(java.lang.String, java.io.OutputStream)
	 */
	public void copyOut(String table, OutputStream os) throws SQLException {
		copyOutQuery("COPY " + table + " TO STDOUT", os);
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyOutQuery(java.lang.String, java.io.OutputStream)
	 */
	public void copyOutQuery(String query, OutputStream os) throws SQLException {
		synchronized (pgStream) {
			sendQuery(query);
			try {
				copyResultLoop(null, os);
			} catch (IOException ex) {
				throw new NClusterException(GT.tr("postgresql.copy.type"), NClusterState.COMMUNICATION_ERROR, ex);

			}
		}
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#getWarnings()
	 */
	public synchronized SQLWarning getWarnings() {
		return this.warnings;
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#clearWarnings()
	 */
	public synchronized void clearWarnings() {
		warnings = null;
	}

	private String receiveCommandStatus() throws IOException {
		// TODO: better handle the msg len
		int l_len = pgStream.ReceiveInteger4();

		// read l_len -5 bytes (-4 for l_len and -1 for trailing \0)
		String status = pgStream.ReceiveString(l_len - 5);

		// now read and discard the trailing \0
		pgStream.Receive(1);

		if (logger.getLogLevel() == Driver.DEBUG)
			logger.debug(" <=BE CommandStatus(" + status + ")");

		return status;
	}

	private SQLException receiveErrorResponse() throws IOException {
		// it's possible to get more than one error message for a query
		// see libpq comments wrt backend closing a connection
		// so, append messages to a string buffer and keep processing
		// check at the bottom to see if we need to throw an exception

		int elen = pgStream.ReceiveInteger4();
		String totalMessage = pgStream.ReceiveString(elen - 4);
		ServerErrorMessage errorMsg = new ServerErrorMessage(totalMessage, logger.getLogLevel());

		if (logger.getLogLevel() == Driver.DEBUG)
			logger.debug(" <=BE ErrorMessage(" + errorMsg.toString() + ")");

		return new NClusterException(errorMsg);
	}

	private SQLWarning receiveNoticeResponse() throws IOException {
		int nlen = pgStream.ReceiveInteger4();
		ServerErrorMessage warnMsg = new ServerErrorMessage(pgStream.ReceiveString(nlen - 4), logger.getLogLevel());

		if (logger.getLogLevel() == Driver.DEBUG)
			logger.debug(" <=BE NoticeResponse(" + warnMsg.toString() + ")");

		return new NClusterWarning(warnMsg);
	}

	/**
	 * After the copy query has been go through the possible responses. The flag which tells us whether we are doing
	 * copy in or out is simply where the InputStream or OutputStream is null. This is much like the loop in
	 * QueryExecutor, it could be merged into that, but it would require some generalization of its current specific
	 * tasks. Right now it has its query in m_binds[] form and expects to return a ResultSet. A more pluggable network
	 * layer would be nice so we could support the V2 and V3 protocols more cleanly and consider a SPI based layer for
	 * an in server pl/java. In general I think it's a bad idea for PGStream to be seen anywhere outside of the
	 * QueryExecutor.
	 */
	private void copyResultLoop(InputStream is, OutputStream os) throws SQLException, IOException {

		NClusterException topLevelError = null;
		boolean queryDone = false;
		while (!queryDone) {
			int c = pgStream.ReceiveChar();
			switch (c) {
			case 'A': // Asynch Notify
				int pid = pgStream.ReceiveInteger4();
				String msg = pgStream.ReceiveString();
				notifications.add(new Notification(msg, pid));
				break;

			case 'C': // Command Complete
				receiveCommandStatus();
				break;
			case 'E': // Error Message
				SQLException error = receiveErrorResponse();
				throw error;
				// handler.handleError(error);

				// keep processing
				// break;
			case 'N': // Error Notification
				SQLWarning warning = receiveNoticeResponse();
				if (this.warnings == null)
					this.warnings = warning;
				else
					this.warnings.setNextWarning(warning);
				break;
			case 'G': // CopyInResponse
				if (is == null)
					throw new NClusterException(GT.tr("postgresql.copy.type"), NClusterState.COMMUNICATION_ERROR);
				receiveCopyInOutResponse();
				sendCopyData(is);
				break;
			case 'H': // CopyOutResponse
				if (os == null)
					throw new NClusterException(GT.tr("postgresql.copy.type"), NClusterState.COMMUNICATION_ERROR);
				receiveCopyInOutResponse();
				break;
			case 'd': // CopyData
				if (os == null)
					throw new NClusterException(GT.tr("postgresql.copy.type"), NClusterState.COMMUNICATION_ERROR);
				receiveCopyData(os);
				break;
			case 'c': // CopyDone
				int copyDoneLength = pgStream.ReceiveInteger4();
				break;
			case 'Z': // ReadyForQuery
				int messageLength = pgStream.ReceiveInteger4();
				char messageStatus = (char) pgStream.ReceiveChar();
				queryDone = true;
				break;
			default:
				throw new NClusterException(GT.tr("postgresql.copy.type"), NClusterState.COMMUNICATION_ERROR);
			}
		}

		if (topLevelError != null)
			throw topLevelError;

	}

	synchronized void addWarning(SQLWarning newWarning) {
		if (warnings == null)
			warnings = newWarning;
		else
			warnings.setNextWarning(newWarning);
	}

	private void sendQuery(String query) throws SQLException {
		Encoding encoding = pgConn.getEncoding();
		try {
			pgStream.SendChar('Q');
			byte message[] = encoding.encode(query);
			int messageSize = 4 + message.length + 1;
			pgStream.SendInteger4(messageSize);
			pgStream.Send(message);
			pgStream.SendChar(0);
			pgStream.flush();
		} catch (IOException ioe) {
			throw new NClusterException("postgresql.copy.ioerror", NClusterState.CONNECTION_FAILURE_DURING_TRANSACTION,
					ioe);
		}
	}

	private void sendCopyData(InputStream is) throws SQLException {
		int read = 0;
		int charIndex = 0;
		while (read >= 0) {
			int absorb = 0;
			try {
				while (absorb < this.bufSize && read != -1) {
					read = is.read(buf, absorb, this.bufSize - absorb);
					absorb += read;
				}
			} catch (IOException ioe) {
				throw new NClusterException("postgresql.copy.inputsource, check data at byte position " + charIndex,
						NClusterState.DATA_ERROR, ioe);
			}

			if (absorb > 0) {
				try {
					pgStream.SendChar('d');
					int messageSize = absorb + 4;
					pgStream.SendInteger4(messageSize);
					pgStream.Send(buf, absorb);
					charIndex += absorb;
				} catch (IOException ioe) {					
					throw new NClusterException("postgresql.copy.ioerror, check data at byte position " + charIndex,
							NClusterState.CONNECTION_FAILURE_DURING_TRANSACTION, ioe);
				}
			}
		}

		// Send the CopyDone message
		try {
			pgStream.SendChar('c');
			pgStream.SendInteger4(4);
			pgStream.flush();
		} catch (IOException ioe) {
			throw new NClusterException("postgresql.copy.ioerror", NClusterState.CONNECTION_FAILURE_DURING_TRANSACTION,
					ioe);
		}
	}

	/**
	 * CopyInResponse and CopyOutResponse have the same field layouts and we simply discard the results.
	 */
	private void receiveCopyInOutResponse() throws SQLException {
		try {
			int messageLength = pgStream.ReceiveInteger4();
			int copyFormat = pgStream.ReceiveChar();

			int numColumns = pgStream.ReceiveInteger2();
			for (int i = 0; i < numColumns; i++) {
				int copyColumnFormat = pgStream.ReceiveInteger2();
			}
		} catch (IOException ex) {
			throw new NClusterException("postgresql.copy.ioerror", NClusterState.CONNECTION_FAILURE_DURING_TRANSACTION,
					ex);
		}
	}

	private void receiveCopyData(OutputStream os) throws SQLException {
		try {
			int messageLength = pgStream.ReceiveInteger4();
			byte data[] = pgStream.Receive(messageLength - 4);
			os.write(data);
		} catch (IOException ioe) {
			throw new NClusterException("postgresql.copy.outputsource", NClusterState.DATA_ERROR, ioe);
		}
	}

}
