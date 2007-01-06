package com.kni.etl.ketl.dbutils.netezza;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.BulkLoaderStatementWrapper;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.exceptions.KETLWriteException;

final public class NetezzaStatementWrapper extends BulkLoaderStatementWrapper {

	private static final String BOOL_STYLE = "1_0";

	private static final String DATESTYLE = "yyyy-MM-dd";

	private static final String DELIMITER = "|";

	private static final String ESCAPE_CHAR = "\\";

	private static final int EX_SUCC = 0;

	private static final int EX_WARN = 2;

	private static final String NULL_VALUE = "''";

	private static final String TIMESTYLE = "HH:mm:ss";

	private static String ENCODING = null;

	public static StatementWrapper prepareStatement(Connection mcDBConnection,
			String encoding, String pTableName, String loadStatement,
			DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper,
			boolean pipe) throws SQLException {
		try {
			ENCODING = encoding;

			return new NetezzaStatementWrapper(mcDBConnection, pTableName,
					loadStatement, madcdColumns, helper, pipe);
		} catch (IOException e) {
			throw new SQLException(e.toString());
		}
	}

	private final String ENCODER = java.nio.charset.Charset.forName(ENCODING)
			.name();

	private final byte[] DELIMITER_AS_BYTES = DELIMITER.getBytes(ENCODER);

	private NumberFormat doubleFormatter;

	private final byte[] FALSE = "0".getBytes(ENCODER);

	private int mAllCols;

	private DatabaseColumnDefinition[] mColumnDetails;

	private String mControlFile;

	private byte[][] mDatums;

	private boolean[] mDatumTrueSQLTimestamp;

	private int[] mItemOrderMap;

	private String mLoadStatement;

	private final byte[] RECORD_DELIMITER_AS_BYTES = "\n".getBytes(ENCODER);

	private StringBuffer sb = new StringBuffer();

	private SimpleDateFormat sqlDateFormatter;

	private SimpleDateFormat sqlTimeFormatter;

	private SimpleDateFormat sqlTimestampFormatter;

	private final byte[] TRUE = "1".getBytes(ENCODER);

	public NetezzaStatementWrapper(Connection connection, String pTableName,
			String loadStatement, DatabaseColumnDefinition[] madcdColumns,
			JDBCItemHelper helper, boolean pipe) throws IOException,
			SQLException {
		super(pipe);

		this.mColumnDetails = madcdColumns;
		this.mDatums = new byte[madcdColumns.length][];
		this.mDatumTrueSQLTimestamp = new boolean[madcdColumns.length];
		this.sqlTimeFormatter = new SimpleDateFormat(TIMESTYLE);
		this.sqlTimestampFormatter = new SimpleDateFormat(DATESTYLE + TIMESTYLE);
		this.sqlDateFormatter = new SimpleDateFormat(DATESTYLE);
		this.doubleFormatter = new DecimalFormat();
		this.doubleFormatter.setGroupingUsed(false);

		String template = "Datafile " + getDataFile() + " {\nTableName "
				+ pTableName + "\nDelimiter '" + DELIMITER + "'\n}";

		File file = File.createTempFile("KETL", ".def");
		mControlFile = file.getAbsolutePath();
		Writer out = new FileWriter(file);
		out.write(template);
		out.close();

		Statement stmt = connection.createStatement();
		ResultSet rsCols = stmt.executeQuery("select * from " + pTableName
				+ " where 1=0");
		ResultSetMetaData rmd = rsCols.getMetaData();

		mAllCols = rmd.getColumnCount();
		mItemOrderMap = new int[mAllCols];
		for (int i = 0; i < mAllCols; i++) {
			String name = rmd.getColumnName(i + 1);
			mItemOrderMap[i] = -1;
			for (int p = 0; p < mColumnDetails.length; p++) {
				if (this.mColumnDetails[p].getColumnName(null, -1)
						.equalsIgnoreCase(name)) {
					mItemOrderMap[i] = p;
				}
			}
		}

		stmt.close();

		this.mLoadStatement = EngineConstants.replaceParameterV2(loadStatement,
				"BOOLSTYLE", BOOL_STYLE);
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "DATESTYLE", "YMD");
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "ENCODING", ENCODING);
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "ESCAPECHAR", ESCAPE_CHAR);
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "TIMESTYLE", "24HOUR");
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "NULLVALUE", NULL_VALUE);
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "LOGFILE", this.mControlFile + ".log");
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "BADFILE", this.mControlFile + ".bad");
		this.mLoadStatement = EngineConstants.replaceParameterV2(
				mLoadStatement, "CONTROLFILE", mControlFile);
		ResourcePool.LogMessage(Thread.currentThread(),
				ResourcePool.DEBUG_MESSAGE, this.mLoadStatement);
	}

	final public void reclaim(String username, String password,
			String tablename, String database, String host) throws IOException {
		String command = "nzreclaim -quit -blocks -db " + database + " -u "
				+ username + " -pw " + password + " -host " + host + " -t"
				+ tablename;

		this.executeCommand(command);

	}

	final public void close() throws SQLException {
		super.close();
		// delete control file
		File fl = new File(mControlFile);
		fl.delete();
	}

	private String escape(String mString) {

		// check for escaping needed
		if ((mString.indexOf('\\') != -1) || (mString.indexOf('|') != -1)
				|| (mString.indexOf('\n') != -1)
				|| (mString.indexOf('\r') != -1)
				|| mString.indexOf((char) 0) != -1
				|| mString.indexOf('\b') != -1 || mString.indexOf('\t') != -1
				|| mString.indexOf('\f') != -1) {
		} else {
			return mString;
		}

		// escape string if needed
		int len = mString.length();

		sb.setLength(0);

		for (int i = 0; i < len; i++) {
			char c = mString.charAt(i);

			switch (c) {
			case 0:
				System.out.println("Removing null in string: " + mString);
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\b':
				sb.append("\\b");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\\':
			case '|':
				sb.append("\\");

			default:
				sb.append(c);
			}
		}

		return sb.toString();
	}

	@Override
	protected String getLoadStatement() {
		return this.mLoadStatement;
	}

	@Override
	protected SQLException handleLoaderStatus(int finalStatus, Thread thread)
			throws InterruptedException {
		switch (finalStatus) {
		case EX_SUCC:
			break;
		case EX_WARN:
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.WARNING_MESSAGE,
					"nzload reported a warning, check the log " + mControlFile
							+ ".log");
			break;
		default:
			Thread.sleep(1000);
			if (filesOpen() == false) {
				// this is here in the event that the pipe fails and hangs the
				// other thread
				ResourcePool.LogException(new KETLWriteException("STDERROR:"
						+ this.getStandardErrorMessage() + "\nSTDOUT:"
						+ this.getStandardOutMessage()), thread);
				if (thread != Thread.currentThread())
					thread.interrupt();
			}
			return new SQLException("nzload Failed\nExtended Log: "
					+ mControlFile + ".log\nError code: " + finalStatus
					+ "\nSTDERROR:" + this.getStandardErrorMessage()
					+ "\nSTDOUT:" + this.getStandardOutMessage());
		}

		return null;
	}

	public void setBoolean(int pos, boolean arg0) {
		this.setObject(pos, arg0 ? TRUE : FALSE);
	}

	public void setByteArrayValue(int pos, byte[] arg0) {
		this.setObject(pos, arg0);
	}

	public void setDouble(int pos, Double arg0) throws SQLException {
		try {
			this.setObject(pos, Double.toString(arg0).getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setFloat(int pos, Float arg0) throws SQLException {
		try {
			this.setObject(pos, Float.toString(arg0).getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setInt(int pos, Integer arg0) throws SQLException {
		try {
			this.setObject(pos, Integer.toString(arg0).getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setLong(int pos, Long arg0) throws SQLException {
		try {
			this.setObject(pos, Long.toString(arg0).getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setNull(int pos, int dataType) {
		this.setObject(pos, null);
	}

	public void setObject(int pos, byte[] arg0) {
		mDatums[pos - 1] = arg0;
	}

	public void setShort(int pos, Short arg0) throws SQLException {
		try {
			this.setObject(pos, Short.toString(arg0).getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setSQLDate(int pos, java.sql.Date arg0) throws SQLException {
		try {
			this.setObject(pos, this.sqlDateFormatter.format(arg0).getBytes(
					ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setSQLTime(int pos, java.sql.Time arg0) throws SQLException {
		try {
			this.setObject(pos, this.sqlTimeFormatter.format(arg0).getBytes(
					ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setSQLTimestamp(int pos, java.sql.Timestamp arg0)
			throws SQLException {

		try {
			if (mDatumTrueSQLTimestamp[pos - 1] == false) {
				this.setTimestamp(pos, arg0);
				return;
			}
			int nanos = arg0.getNanos();
			String zeros = "000000000";

			String nanosString = Integer.toString(nanos);

			// Add leading zeros
			nanosString = zeros.substring(0, (9 - nanosString.length()))
					+ nanosString;

			// Truncate trailing zeros
			char[] nanosChar = new char[nanosString.length()];
			nanosString.getChars(0, nanosString.length(), nanosChar, 0);
			int truncIndex = 8;
			while (nanosChar[truncIndex] == '0') {
				truncIndex--;
			}

			nanosString = new String(nanosChar, 0, truncIndex + 1);

			String data = (this.sqlTimestampFormatter.format(arg0) + nanosString);
			this.setObject(pos, data.getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setString(int pos, String arg0) throws SQLException {
		try {
			this.setObject(pos, escape(arg0).getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public void setTimestamp(int pos, Date arg0) throws SQLException {
		try {
			this.setObject(pos, this.sqlTimestampFormatter.format(arg0)
					.getBytes(ENCODER));
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	protected void writeRecord() throws IOException, Error {

		for (int i = 0; i < mAllCols; i++) {

			if (i > 0) {
				this.mWriter.write(DELIMITER_AS_BYTES);
			}

			int pos = this.mItemOrderMap[i];

			if (pos >= 0) {
				if (this.mDatums[pos] != null)
					this.mWriter.write((byte[]) this.mDatums[pos]);
				this.mDatums[pos] = null;
			}
		}
		this.mWriter.write(RECORD_DELIMITER_AS_BYTES);
	}
}
