package com.kni.etl.ketl.dbutils.oracle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.BulkLoaderStatementWrapper;
import com.kni.etl.dbutils.ColumnDefinition;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.exceptions.KETLWriteException;

final public class SQLLoaderStatementWrapper extends BulkLoaderStatementWrapper {

    Connection con;
    JDBCItemHelper helper;
    String mLoadStatement;
    int mColumns;
    DatabaseColumnDefinition[] mColumnDetails;
    private String mControlFile;
    private int mDBCase;
    private SimpleDateFormat sqlTimeFormatter;
    private SimpleDateFormat sqlTimestampFormatter;
    private SimpleDateFormat sqlDateFormatter;
    private boolean[] mDatumTrueSQLTimestamp;
    private static final int DEL_LENGTH = "|".getBytes().length;

    public SQLLoaderStatementWrapper(Connection connection, String pTableName, String loadStatement,
            DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper, boolean pipe) throws IOException,
            SQLException {
        super(pipe);
        this.con = connection;
        this.helper = helper;

        this.mColumns = madcdColumns.length;
        this.mColumnDetails = madcdColumns;
        this.mDatums = new byte[madcdColumns.length][];
        this.mDatumTrueSQLTimestamp = new boolean[madcdColumns.length];
        this.mDatumPadLength = new int[madcdColumns.length];
        this.mDatumNeedsDelimiter = new boolean[madcdColumns.length];
        this.dateFormatter = new SimpleDateFormat(this.dateTimeFormat);
        this.sqlTimeFormatter = new SimpleDateFormat(this.sqlTimeFormat);
        this.sqlTimestampFormatter = new SimpleDateFormat(this.sqlTimestampFormat);
        this.sqlDateFormatter = new SimpleDateFormat(this.dateTimeFormat);
        this.doubleFormatter = new DecimalFormat();
        this.doubleFormatter.setGroupingUsed(false);

        DatabaseMetaData md = this.con.getMetaData();

        if (md.storesUpperCaseIdentifiers()) {
            this.mDBCase = ColumnDefinition.UPPER_CASE;
        }
        else if (md.storesLowerCaseIdentifiers()) {
            this.mDBCase = ColumnDefinition.LOWER_CASE;
        }
        else if (md.storesMixedCaseIdentifiers()) {
            this.mDBCase = ColumnDefinition.MIXED_CASE;
        }

        // this.mEncoder = Charset.forName(charset);

        StringBuffer cols = new StringBuffer();
        for (int i = 0; i < madcdColumns.length; i++) {
            if (i > 0) {
                cols.append(",\n\t");
            }

            cols.append(madcdColumns[i].getColumnName(null, this.mDBCase) + " ");

            Class pClass = madcdColumns[i].getSourceClass();

            // by default not delimiter
            this.mDatumNeedsDelimiter[i] = false;

            if (pClass == Short.class || pClass == short.class) {
                cols.append("INTEGER(2) NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                this.recordLenSize += 3;
            }
            else if (pClass == Integer.class || pClass == int.class) {
                cols.append("INTEGER(4)NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                this.recordLenSize += 5;
            }
            else if (pClass == Double.class || pClass == double.class) {
                cols.append("DOUBLE NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                this.recordLenSize += 9;
            }
            else if (pClass == Float.class || pClass == float.class) {
                cols.append("FLOAT NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                this.recordLenSize += 5;
            }
            else if (pClass == Long.class || pClass == long.class) {
                cols.append("INTEGER(8) NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                this.recordLenSize += 9;
            }
            else if (pClass == String.class) {
                this.mDatumPadLength[i] = Integer.toString(this.mColumnDetails[i].iSize).length();

                cols.append("VARCHARC(" + this.mDatumPadLength[i] + "," + this.mColumnDetails[i].iSize + ")");
                this.recordLenSize += this.mColumnDetails[i].iSize + this.mDatumPadLength[i];
            }
            else if (pClass == java.util.Date.class) {
                this.mDatumNeedsDelimiter[i] = i < this.mColumns - 1 ? true : false;
                cols.append(madcdColumns[i].sTypeName + " \"" + this.ORACLE_DATE_FORMAT + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                this.recordLenSize += this.ORACLE_DATE_FORMAT.getBytes().length + SQLLoaderStatementWrapper.DEL_LENGTH;
            }
            else if (pClass == java.sql.Date.class) {
                this.mDatumNeedsDelimiter[i] = i < this.mColumns - 1 ? true : false;
                cols.append(madcdColumns[i].sTypeName + " \"" + this.ORACLE_DATE_FORMAT + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                this.recordLenSize += this.ORACLE_DATE_FORMAT.getBytes().length + SQLLoaderStatementWrapper.DEL_LENGTH;
            }
            else if (pClass == java.sql.Time.class) {
                this.mDatumNeedsDelimiter[i] = i < this.mColumns - 1 ? true : false;
                cols.append(madcdColumns[i].sTypeName + " \"" + this.ORACLE_TIME_FORMAT + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                this.recordLenSize += this.ORACLE_TIME_FORMAT.getBytes().length + SQLLoaderStatementWrapper.DEL_LENGTH;
            }
            else if (pClass == java.sql.Timestamp.class) {
                this.mDatumNeedsDelimiter[i] = i < this.mColumns - 1 ? true : false;
                this.mDatumTrueSQLTimestamp[i] = madcdColumns[i].sTypeName.startsWith("TIMESTAMP");

                cols.append(madcdColumns[i].sTypeName + " \""
                        + (this.mDatumTrueSQLTimestamp[i] ? this.ORACLE_TIMESTAMP_FORMAT : this.ORACLE_DATE_FORMAT)
                        + "\"" + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));

                this.recordLenSize += this.ORACLE_TIMESTAMP_FORMAT.getBytes().length
                        + SQLLoaderStatementWrapper.DEL_LENGTH;
            }
            else if (pClass == Boolean.class || pClass == boolean.class) {
                throw new IOException("Oracle does not support the followin datatype " + pClass.getCanonicalName());
            }
            else if (pClass == Character.class || pClass == char.class) {
                cols.append("CHAR(1) " + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                this.recordLenSize += 4 + SQLLoaderStatementWrapper.DEL_LENGTH;
            }
            else if (pClass == Byte[].class || pClass == byte[].class) {
                this.mDatumPadLength[i] = Integer.toString(this.mColumnDetails[i].iSize).length();
                cols.append("VARRAWC(" + this.mDatumPadLength[i] + "," + this.mColumnDetails[i].iSize + ")");
                this.recordLenSize += this.mColumnDetails[i].iSize + this.mDatumPadLength[i];
            }
            else {
                throw new IOException("Datatype not supported " + pClass.getCanonicalName());
            }

        }

        this.recordLenSize = Integer.toString(this.recordLenSize).length();

        String template = "LOAD DATA  LENGTH CHARACTER BYTEORDER BIG ENDIAN INFILE '" + this.getDataFile() + "' \"VAR "
                + this.recordLenSize
                + "\" APPEND INTO TABLE ${DESTINATIONTABLENAME}\n\tTRAILING NULLCOLS (${DESTINATIONCOLUMNS})\n\t";

        template = EngineConstants.replaceParameterV2(template, "DESTINATIONCOLUMNS", cols.toString());
        template = EngineConstants.replaceParameterV2(template, "DESTINATIONTABLENAME", pTableName);

        File file = File.createTempFile("KETL", ".def");
        this.mControlFile = file.getAbsolutePath();
        Writer out = new FileWriter(file);
        out.write(template);
        out.close();
        template = EngineConstants.replaceParameterV2(template, "DESTINATIONTABLENAME", pTableName);

        this.mLoadStatement = EngineConstants.replaceParameterV2(loadStatement, "CONTROLFILE", this.mControlFile);
        this.mLoadStatement = EngineConstants.replaceParameterV2(this.mLoadStatement, "BADDATA", this.mControlFile
                + ".bad");
        this.mLoadStatement = EngineConstants
                .replaceParameterV2(this.mLoadStatement, "LOG", this.mControlFile + ".log");
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, this.mLoadStatement);
    }

    @Override
    final public void close() throws SQLException {
        super.close();
        // delete control file
        File fl = new File(this.mControlFile);
        fl.delete();
    }

    public static StatementWrapper prepareStatement(Connection mcDBConnection, String pTableName, String loadStatement,
            DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper, boolean pipe) throws SQLException {
        try {
            return new SQLLoaderStatementWrapper(mcDBConnection, pTableName, loadStatement, madcdColumns, helper, pipe);
        } catch (IOException e) {
            throw new SQLException(e.toString());
        }
    }

    private final static String mDelimiter = "|";
    private final static byte[] mNull = "".getBytes();
    private DateFormat dateFormatter;
    private String dateTimeFormat = "yyyyMMddHHmmss";
    private String sqlTimeFormat = "HHmmss";
    private String sqlTimestampFormat = "yyyyMMddHHmmss";

    public String ORACLE_DATE_FORMAT = "YYYYMMDDHH24MISS";
    public String ORACLE_TIME_FORMAT = "HH24MISS";
    public String ORACLE_TIMESTAMP_FORMAT = "YYYYMMDDHH24MISSFF9";
    private NumberFormat doubleFormatter;
    private byte[][] mDatums;
    private int[] mDatumPadLength;
    private boolean[] mDatumNeedsDelimiter;
    // private FileOutputStream mTarget = null;

    private static final int EX_SUCC = 0;
    private static final int EX_WARN = 2;
    private int recordLen = 0, recordLenSize = 7;

    private final static byte[] TRUE = "1".getBytes();
    private final static byte[] FALSE = "0".getBytes();

    @Override
    public void setBoolean(int pos, boolean arg0) {
        this.setObject(pos, arg0 ? SQLLoaderStatementWrapper.TRUE : SQLLoaderStatementWrapper.FALSE);
    }

    @Override
    public void setByteArrayValue(int pos, byte[] arg0) {
        String sLen = Integer.toString(arg0.length);
        sLen = this.pad(sLen, this.mDatumPadLength[pos - 1], '0');
        byte[] a = sLen.getBytes();

        byte[] z = new byte[arg0.length + a.length];
        System.arraycopy(a, 0, z, 0, a.length);
        System.arraycopy(arg0, 0, z, a.length, arg0.length);

        this.setObject(pos, z);
    }

    @Override
    public void setFloat(int pos, Float arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.packF4(arg0));
    }

    @Override
    public void setInt(int pos, Integer arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.pack4(arg0));
    }

    @Override
    public void setLong(int pos, Long arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.pack8(arg0));
    }

    @Override
    public void setShort(int pos, Short arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.pack2(arg0));
    }

    @Override
    public void setDouble(int pos, Double arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.packF8(arg0));
    }

    @Override
    public void setNull(int pos, int dataType) {
        switch (dataType) {
        case java.sql.Types.VARCHAR:
        case java.sql.Types.BLOB:
            this.mDatums[pos - 1] = this.pad("0", this.mDatumPadLength[pos - 1], '0').getBytes();
            break;
        default:
            this.mDatums[pos - 1] = SQLLoaderStatementWrapper.mNull;
        }

        this.recordLen += this.mDatums[pos - 1].length
                + (this.mDatumNeedsDelimiter[pos - 1] ? SQLLoaderStatementWrapper.DEL_LENGTH : 0);

    }

    @Override
    public void setObject(int pos, byte[] arg0) {

        this.mDatums[pos - 1] = arg0;
        this.recordLen += arg0.length + (this.mDatumNeedsDelimiter[pos - 1] ? SQLLoaderStatementWrapper.DEL_LENGTH : 0);

    }

    @Override
    public void setString(int pos, String arg0) {

        this.setObject(pos, (this.pad(Integer.toString(arg0.length()), this.mDatumPadLength[pos - 1], '0') + arg0)
                .getBytes());

    }

    @Override
    public void setTimestamp(int pos, Date arg0) {
        this.setObject(pos, this.dateFormatter.format(arg0).getBytes());
    }

    @Override
    public void setSQLDate(int pos, java.sql.Date arg0) {

        this.setObject(pos, this.sqlDateFormatter.format(arg0).getBytes());
    }

    @Override
    public void setSQLTimestamp(int pos, java.sql.Timestamp arg0) {

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
        this.setObject(pos, data.getBytes());
    }

    public static byte[] pack2(Short val) {
        if (val == null)
            return new byte[] { 0, 0, 0 };
        return new byte[] { (byte) (val >> 8), (byte) val.shortValue(), 1 };
    }

    public static byte[] pack4(Integer val) {
        if (val == null)
            return new byte[] { 0, 0, 0, 0, 0, 0 };
        return new byte[] { (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val.intValue(), 1 };
    }

    public static byte[] pack8(Long val) {
        if (val == null)
            return new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        return new byte[] { (byte) (val >> 56), (byte) (val >> 48), (byte) (val >> 40), (byte) (val >> 32),
                (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val.longValue(), 1 };
    }

    public static byte[] packF4(Float val) {
        return SQLLoaderStatementWrapper.pack4(val == null ? null : Float.floatToIntBits(val));
    }

    public static byte[] packF8(Double val) {
        return SQLLoaderStatementWrapper.pack8(val == null ? null : Double.doubleToLongBits(val));
    }

    StringBuffer tmpPadBuffer = new StringBuffer();

    private String pad(String word, int len, char padChar) throws Error {
        int wordLen = word.length();

        int diff = len - wordLen;

        if (diff == 0)
            return word;
        if (diff < 0)
            throw new Error("Input string length greater than requested length");

        this.tmpPadBuffer.setLength(0);

        for (int i = 0; i < diff; i++)
            this.tmpPadBuffer.append(padChar);

        this.tmpPadBuffer.append(word);

        return this.tmpPadBuffer.toString();
    }

    @Override
    public void setSQLTime(int pos, java.sql.Time arg0) {
        this.setObject(pos, this.sqlTimeFormatter.format(arg0).getBytes());
    }

    @Override
    protected SQLException handleLoaderStatus(int finalStatus, Thread thread) throws InterruptedException {
        switch (finalStatus) {
        case EX_SUCC:
            break;
        case EX_WARN:
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                    "SQL*Loader reported a warning, check the log " + this.mControlFile + ".log");
            break;
        default:
            Thread.sleep(1000);
            if (this.filesOpen() == false) {
                // this is here in the event that the pipe fails and hangs the other thread
                ResourcePool.LogException(new KETLWriteException("STDERROR:" + this.getStandardErrorMessage()
                        + "\nSTDOUT:" + this.getStandardOutMessage()), thread);
                if (thread != Thread.currentThread())
                    thread.interrupt();
            }
            return new SQLException("SQL*Loader Failed\nExtended Log: " + this.mControlFile + ".log\nError code: "
                    + finalStatus + "\nSTDERROR:" + this.getStandardErrorMessage() + "\nSTDOUT:"
                    + this.getStandardOutMessage());
        }

        return null;
    }

    @Override
    protected void writeRecord() throws IOException, Error {
        this.mWriter.write(this.pad(Integer.toString(this.recordLen), this.recordLenSize, '0').getBytes());

        this.recordLen = 0;
        for (int i = 0; i < this.mColumns; i++) {
            this.mWriter.write(this.mDatums[i]);
            if (this.mDatumNeedsDelimiter[i] && i < (this.mColumns - 1)) {
                this.mWriter.write(SQLLoaderStatementWrapper.mDelimiter.getBytes());
            }
            this.mDatums[i] = null;
        }
    }

    @Override
    protected String getLoadStatement() {
        return this.mLoadStatement;
    }
}
