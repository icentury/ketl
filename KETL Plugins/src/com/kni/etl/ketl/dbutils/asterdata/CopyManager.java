package com.kni.etl.ketl.dbutils.asterdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;

import com.asterdata.ncluster.core.BaseConnection;
import com.asterdata.ncluster.core.Encoding;

/**
 * Implement COPY support in the JDBC driver. This requires a 7.4 server and a
 * connection with the V3 protocol. Previous versions could not recover from
 * errors and the connection had to be abandoned which was not acceptable.
 */

public class CopyManager implements CopyManagerInterface {

	private SQLWarning warnings;

	private final Encoding encoder;

	private final BaseConnection pgConnection;

	private int bufferSize = 16384;

	public CopyManager(Connection con) throws SQLException {
		pgConnection = (BaseConnection) con;

		/*
		 * QueryExecutor executor = pgConnection.getQueryExecutor(); Field
		 * field; //UTF-8 try { field =
		 * QueryExecutorImpl.class.getDeclaredField("pgStream");
		 * field.setAccessible(true); PGStream pgStream = (PGStream)
		 * field.get(executor); this.encoder = pgStream.getEncoding(); } catch
		 * (Exception e) { throw new SQLException(e); }
		 */
		this.encoder = Encoding.getDatabaseEncoding("UTF-8");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#encodeString(
	 * java.lang.String)
	 */
	public byte[] encodeString(String arg0) throws IOException {
		return this.encoder.encode(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#getEncoding()
	 */
	public String getEncoding() {
		return this.encoder.name();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyIn(java.lang
	 * .String, java.io.InputStream)
	 */
	public void copyIn(String table, InputStream is) throws SQLException {
		copyInQuery("COPY " + table + " FROM STDIN", is);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyInQuery(java
	 * .lang.String, java.io.InputStream)
	 */
	public void copyInQuery(String query, InputStream is) throws SQLException {
		this.pgConnection.getCopyAPI().copyIntoDB(query, is, this.bufferSize);// ,
		// this.bufferSize);
		// this.pgConnection.commit();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyOut(java.
	 * lang.String, java.io.OutputStream)
	 */
	public void copyOut(String table, OutputStream os) throws SQLException {
		copyOutQuery("COPY " + table + " TO STDOUT", os);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#copyOutQuery(
	 * java.lang.String, java.io.OutputStream)
	 */
	public void copyOutQuery(String query, OutputStream os) throws SQLException {
		try {
			this.pgConnection.getCopyAPI().copyFromDB(query, os);
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#getWarnings()
	 */
	public synchronized SQLWarning getWarnings() {
		return this.warnings;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.dbutils.asterdata.CopyManagerInterface#clearWarnings()
	 */
	public synchronized void clearWarnings() {
		warnings = null;
	}

	synchronized void addWarning(SQLWarning newWarning) {
		if (warnings == null)
			warnings = newWarning;
		else
			warnings.setNextWarning(newWarning);
	}

	public void setCopyBufferSize(int arg0) {
		this.bufferSize = arg0;
	}

	@Override
	public void commit() throws SQLException {
		this.pgConnection.commit();
	}

}
