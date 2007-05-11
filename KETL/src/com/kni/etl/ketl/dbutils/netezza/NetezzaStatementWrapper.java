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

    public static StatementWrapper prepareStatement(Connection mcDBConnection, String encoding, String pTableName,
            String loadStatement, DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper, boolean pipe)
            throws SQLException {
        try {
            NetezzaStatementWrapper.ENCODING = encoding;

            return new NetezzaStatementWrapper(mcDBConnection, pTableName, loadStatement, madcdColumns, helper, pipe);
        } catch (IOException e) {
            throw new SQLException(e.toString());
        }
    }

    private final String ENCODER = java.nio.charset.Charset.forName(NetezzaStatementWrapper.ENCODING).name();

    private final byte[] DELIMITER_AS_BYTES = NetezzaStatementWrapper.DELIMITER.getBytes(this.ENCODER);

    private NumberFormat doubleFormatter;

    private final byte[] FALSE = "0".getBytes(this.ENCODER);

    private int mAllCols;

    private DatabaseColumnDefinition[] mColumnDetails;

    private String mControlFile;

    private byte[][] mDatums;

    private boolean[] mDatumTrueSQLTimestamp;

    private int[] mItemOrderMap;

    private String mLoadStatement;

    private final byte[] RECORD_DELIMITER_AS_BYTES = "\n".getBytes(this.ENCODER);

    private StringBuffer sb = new StringBuffer();

    private SimpleDateFormat sqlDateFormatter;

    private SimpleDateFormat sqlTimeFormatter;

    private SimpleDateFormat sqlTimestampFormatter;

    private final byte[] TRUE = "1".getBytes(this.ENCODER);

    public NetezzaStatementWrapper(Connection connection, String pTableName, String loadStatement,
            DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper, boolean pipe) throws IOException,
            SQLException {
        super(pipe);

        this.mColumnDetails = madcdColumns;
        this.mDatums = new byte[madcdColumns.length][];
        this.mDatumTrueSQLTimestamp = new boolean[madcdColumns.length];
        this.sqlTimeFormatter = new SimpleDateFormat(NetezzaStatementWrapper.TIMESTYLE);
        this.sqlTimestampFormatter = new SimpleDateFormat(NetezzaStatementWrapper.DATESTYLE
                + NetezzaStatementWrapper.TIMESTYLE);
        this.sqlDateFormatter = new SimpleDateFormat(NetezzaStatementWrapper.DATESTYLE);
        this.doubleFormatter = new DecimalFormat();
        this.doubleFormatter.setGroupingUsed(false);

        String template = "Datafile " + this.getDataFile() + " {\nTableName " + pTableName + "\nDelimiter '"
                + NetezzaStatementWrapper.DELIMITER + "'\n}";

        File file = File.createTempFile("KETL", ".def");
        this.mControlFile = file.getAbsolutePath();
        Writer out = new FileWriter(file);
        out.write(template);
        out.close();

        Statement stmt = connection.createStatement();
        ResultSet rsCols = stmt.executeQuery("select * from " + pTableName + " where 1=0");
        ResultSetMetaData rmd = rsCols.getMetaData();

        this.mAllCols = rmd.getColumnCount();
        this.mItemOrderMap = new int[this.mAllCols];
        for (int i = 0; i < this.mAllCols; i++) {
            String name = rmd.getColumnName(i + 1);
            this.mItemOrderMap[i] = -1;
            for (int p = 0; p < this.mColumnDetails.length; p++) {
                if (this.mColumnDetails[p].getColumnName(null, -1).equalsIgnoreCase(name)) {
                    this.mItemOrderMap[i] = p;
                }
            }
        }

        stmt.close();

        this.mLoadStatement = EngineConstants.replaceParameterV2(loadStatement, "BOOLSTYLE",
                NetezzaStatementWrapper.BOOL_STYLE);
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "DATESTYLE", "YMD");
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "ENCODING",
                NetezzaStatementWrapper.ENCODING);
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "ESCAPECHAR",
                NetezzaStatementWrapper.ESCAPE_CHAR);
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "TIMESTYLE", "24HOUR");
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "NULLVALUE",
                NetezzaStatementWrapper.NULL_VALUE);
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "LOGFILE", this.mControlFile
                + ".log");
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "BADFILE", this.mControlFile
                + ".bad");
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "CONTROLFILE", this.mControlFile);
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, this.mLoadStatement);
    }

    final public void reclaim(String username, String password, String tablename, String database, String host)
            throws IOException {
        String command = "nzreclaim -quit -blocks -db " + database + " -u " + username + " -pw " + password + " -host "
                + host + " -t" + tablename;

        this.executeCommand(command);

    }

    @Override
    final public void close() throws SQLException {
        super.close();
        // delete control file
        File fl = new File(this.mControlFile);
        fl.delete();
    }

    private String escape(String mString) {

        // check for escaping needed
        if ((mString.indexOf('\\') != -1) || (mString.indexOf('|') != -1) || (mString.indexOf('\n') != -1)
                || (mString.indexOf('\r') != -1) || mString.indexOf((char) 0) != -1 || mString.indexOf('\b') != -1
                || mString.indexOf('\t') != -1 || mString.indexOf('\f') != -1) {
        }
        else {
            return mString;
        }

        // escape string if needed
        int len = mString.length();

        this.sb.setLength(0);

        for (int i = 0; i < len; i++) {
            char c = mString.charAt(i);

            switch (c) {
            case 0:
                System.out.println("Removing null in string: " + mString);
                break;
            case '\f':
                this.sb.append("\\f");
                break;
            case '\b':
                this.sb.append("\\b");
                break;
            case '\t':
                this.sb.append("\\t");
                break;
            case '\r':
                this.sb.append("\\r");
                break;
            case '\n':
                this.sb.append("\\n");
                break;
            case '\\':
            case '|':
                this.sb.append("\\");

            default:
                this.sb.append(c);
            }
        }

        return this.sb.toString();
    }

    @Override
    protected String getLoadStatement() {
        return this.mLoadStatement;
    }

    @Override
    protected SQLException handleLoaderStatus(int finalStatus, Thread thread) throws InterruptedException {
        switch (finalStatus) {
        case EX_SUCC:
            break;
        case EX_WARN:
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                    "nzload reported a warning, check the log " + this.mControlFile + ".log");
            break;
        default:
            Thread.sleep(1000);
            if (this.filesOpen() == false) {
                // this is here in the event that the pipe fails and hangs the
                // other thread
                ResourcePool.LogException(new KETLWriteException("STDERROR:" + this.getStandardErrorMessage()
                        + "\nSTDOUT:" + this.getStandardOutMessage()), thread);
                if (thread != Thread.currentThread())
                    thread.interrupt();
            }
            return new SQLException("nzload Failed\nExtended Log: " + this.mControlFile + ".log\nError code: "
                    + finalStatus + "\nSTDERROR:" + this.getStandardErrorMessage() + "\nSTDOUT:"
                    + this.getStandardOutMessage());
        }

        return null;
    }

    @Override
    public void setBoolean(int pos, boolean arg0) {
        this.setObject(pos, arg0 ? this.TRUE : this.FALSE);
    }

    @Override
    public void setByteArrayValue(int pos, byte[] arg0) {
        this.setObject(pos, arg0);
    }

    @Override
    public void setDouble(int pos, Double arg0) throws SQLException {
        try {
            this.setObject(pos, Double.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setFloat(int pos, Float arg0) throws SQLException {
        try {
            this.setObject(pos, Float.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setInt(int pos, Integer arg0) throws SQLException {
        try {
            this.setObject(pos, Integer.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setLong(int pos, Long arg0) throws SQLException {
        try {
            this.setObject(pos, Long.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setNull(int pos, int dataType) {
        this.setObject(pos, null);
    }

    @Override
    public void setObject(int pos, byte[] arg0) {
        this.mDatums[pos - 1] = arg0;
    }

    @Override
    public void setShort(int pos, Short arg0) throws SQLException {
        try {
            this.setObject(pos, Short.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setSQLDate(int pos, java.sql.Date arg0) throws SQLException {
        try {
            this.setObject(pos, this.sqlDateFormatter.format(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setSQLTime(int pos, java.sql.Time arg0) throws SQLException {
        try {
            this.setObject(pos, this.sqlTimeFormatter.format(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setSQLTimestamp(int pos, java.sql.Timestamp arg0) throws SQLException {

        try {
            if (this.mDatumTrueSQLTimestamp[pos - 1] == false) {
                this.setTimestamp(pos, arg0);
                return;
            }
            int nanos = arg0.getNanos();
            String zeros = "000000000";

            String nanosString = Integer.toString(nanos);

            // Add leading zeros
            nanosString = zeros.substring(0, (9 - nanosString.length())) + nanosString;

            // Truncate trailing zeros
            char[] nanosChar = new char[nanosString.length()];
            nanosString.getChars(0, nanosString.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0') {
                truncIndex--;
            }

            nanosString = new String(nanosChar, 0, truncIndex + 1);

            String data = (this.sqlTimestampFormatter.format(arg0) + nanosString);
            this.setObject(pos, data.getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setString(int pos, String arg0) throws SQLException {
        try {
            this.setObject(pos, this.escape(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void setTimestamp(int pos, Date arg0) throws SQLException {
        try {
            this.setObject(pos, this.sqlTimestampFormatter.format(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    protected void writeRecord() throws IOException, Error {

        for (int i = 0; i < this.mAllCols; i++) {

            if (i > 0) {
                this.mWriter.write(this.DELIMITER_AS_BYTES);
            }

            int pos = this.mItemOrderMap[i];

            if (pos >= 0) {
                if (this.mDatums[pos] != null)
                    this.mWriter.write(this.mDatums[pos]);
                this.mDatums[pos] = null;
            }
        }
        this.mWriter.write(this.RECORD_DELIMITER_AS_BYTES);
    }
}
