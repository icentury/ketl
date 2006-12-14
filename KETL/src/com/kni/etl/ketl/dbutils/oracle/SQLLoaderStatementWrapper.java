package com.kni.etl.ketl.dbutils.oracle;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.w3c.dom.Element;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ColumnDefinition;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLWriteException;

final public class SQLLoaderStatementWrapper extends StatementWrapper {

    class InputStreamHandler extends Thread {

        private StringBuffer m_captureBuffer;
        private InputStream m_stream;

        InputStreamHandler(StringBuffer captureBuffer, InputStream stream) {
            m_stream = stream;
            m_captureBuffer = captureBuffer;
            start();
        }

        public void run() {
            try {
                int nextChar;

                while ((nextChar = m_stream.read()) != -1) {
                    m_captureBuffer.append((char) nextChar);
                }
            } catch (IOException ioe) {
            }
        }
    }

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
        super();
        this.con = connection;
        this.helper = helper;

        this.pipeSupported = pipe;
        this.mColumns = madcdColumns.length;
        this.mColumnDetails = madcdColumns;
        this.mDatums = new byte[madcdColumns.length][];
        this.mDatumTrueSQLTimestamp = new boolean[madcdColumns.length];
        this.mDatumPadLength = new int[madcdColumns.length];
        this.mDatumNeedsDelimiter = new boolean[madcdColumns.length];
        this.dateFormatter = new SimpleDateFormat(dateTimeFormat);
        this.sqlTimeFormatter = new SimpleDateFormat(sqlTimeFormat);
        this.sqlTimestampFormatter = new SimpleDateFormat(sqlTimestampFormat);
        this.sqlDateFormatter = new SimpleDateFormat(dateTimeFormat);
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
            mDatumNeedsDelimiter[i] = false;

            if (pClass == Short.class || pClass == short.class) {
                cols.append("INTEGER(2) NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                recordLenSize += 3;
            }
            else if (pClass == Integer.class || pClass == int.class) {
                cols.append("INTEGER(4)NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                recordLenSize += 5;
            }
            else if (pClass == Double.class || pClass == double.class) {
                cols.append("DOUBLE NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                recordLenSize += 9;
            }
            else if (pClass == Float.class || pClass == float.class) {
                cols.append("FLOAT NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                recordLenSize += 5;
            }
            else if (pClass == Long.class || pClass == long.class) {
                cols.append("INTEGER(8) NULLIF NC" + i + "=X'0', NC" + i + " FILLER BYTEINT");
                recordLenSize += 9;
            }
            else if (pClass == String.class) {
                this.mDatumPadLength[i] = Integer.toString(this.mColumnDetails[i].iSize).length();

                cols.append("VARCHARC(" + this.mDatumPadLength[i] + "," + this.mColumnDetails[i].iSize + ")");
                recordLenSize += this.mColumnDetails[i].iSize + this.mDatumPadLength[i];
            }
            else if (pClass == java.util.Date.class) {
                mDatumNeedsDelimiter[i] = i < mColumns - 1 ? true : false;
                cols.append(madcdColumns[i].sTypeName + " \"" + this.ORACLE_DATE_FORMAT + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                recordLenSize += this.ORACLE_DATE_FORMAT.getBytes().length + DEL_LENGTH;
            }
            else if (pClass == java.sql.Date.class) {
                mDatumNeedsDelimiter[i] = i < mColumns - 1 ? true : false;
                cols.append(madcdColumns[i].sTypeName + " \"" + this.ORACLE_DATE_FORMAT + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                recordLenSize += this.ORACLE_DATE_FORMAT.getBytes().length + DEL_LENGTH;
            }
            else if (pClass == java.sql.Time.class) {
                mDatumNeedsDelimiter[i] = i < mColumns - 1 ? true : false;
                cols.append(madcdColumns[i].sTypeName + " \"" + this.ORACLE_TIME_FORMAT + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                recordLenSize += this.ORACLE_TIME_FORMAT.getBytes().length + DEL_LENGTH;
            }
            else if (pClass == java.sql.Timestamp.class) {
                mDatumNeedsDelimiter[i] = i < mColumns - 1 ? true : false;
                mDatumTrueSQLTimestamp[i] = madcdColumns[i].sTypeName.startsWith("TIMESTAMP");

                cols.append(madcdColumns[i].sTypeName + " \""
                        + (mDatumTrueSQLTimestamp[i] ? this.ORACLE_TIMESTAMP_FORMAT : this.ORACLE_DATE_FORMAT) + "\""
                        + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));

                recordLenSize += this.ORACLE_TIMESTAMP_FORMAT.getBytes().length + DEL_LENGTH;
            }
            else if (pClass == Boolean.class || pClass == boolean.class) {
                throw new IOException("Oracle does not support the followin datatype " + pClass.getCanonicalName());
            }
            else if (pClass == Character.class || pClass == char.class) {
                cols.append("CHAR(1) " + (i == madcdColumns.length - 1 ? "" : "  terminated by \"|\""));
                recordLenSize += 4 + DEL_LENGTH;
            }
            else if (pClass == Byte[].class || pClass == byte[].class) {
                this.mDatumPadLength[i] = Integer.toString(this.mColumnDetails[i].iSize).length();
                cols.append("VARRAWC(" + this.mDatumPadLength[i] + "," + this.mColumnDetails[i].iSize + ")");
                recordLenSize += this.mColumnDetails[i].iSize + this.mDatumPadLength[i];
            }
            else {
                throw new IOException("Datatype not supported " + pClass.getCanonicalName());
            }

        }

        recordLenSize = Integer.toString(recordLenSize).length();

        if (pipeSupported)
            msDataFile = createPipe();
        else
            msDataFile = File.createTempFile("ketl", ".dat").getAbsolutePath();

        String template = "LOAD DATA  LENGTH CHARACTER BYTEORDER BIG ENDIAN INFILE '" + msDataFile + "' \"VAR "
                + recordLenSize
                + "\" APPEND INTO TABLE ${DESTINATIONTABLENAME}\n\tTRAILING NULLCOLS (${DESTINATIONCOLUMNS})\n\t";

        template = EngineConstants.replaceParameterV2(template, "DESTINATIONCOLUMNS", cols.toString());
        template = EngineConstants.replaceParameterV2(template, "DESTINATIONTABLENAME", pTableName);

        File file = File.createTempFile("KETL", ".def");
        mControlFile = file.getAbsolutePath();
        Writer out = new FileWriter(file);
        out.write(template);
        out.close();
        template = EngineConstants.replaceParameterV2(template, "DESTINATIONTABLENAME", pTableName);

        this.mLoadStatement = EngineConstants.replaceParameterV2(loadStatement, "CONTROLFILE", this.mControlFile);
        this.mLoadStatement = EngineConstants.replaceParameterV2(mLoadStatement, "BADDATA", this.mControlFile + ".bad");
        this.mLoadStatement = EngineConstants.replaceParameterV2(mLoadStatement, "LOG", this.mControlFile + ".log");
        ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.DEBUG_MESSAGE,this.mLoadStatement);
    }

    boolean pipeSupported = false;
    private Process mPipeToProcess;
    private StringBuffer mInBuffer;
    private StringBuffer mErrBuffer;
    private BufferedOutputStream mBuffer;

    private String createPipe() throws IOException {
        String pipeFilename;
        String tempdir = System.getProperty("java.io.tmpdir");

        if ((tempdir != null) && (tempdir.endsWith("/") == false) && (tempdir.endsWith("\\") == false)) {
            tempdir = tempdir + File.separator;
        }
        else if (tempdir == null) {
            tempdir = "";
        }

        pipeFilename = tempdir + "sqlPip" + Long.toString(System.nanoTime());
        String command = "mkfifo " + pipeFilename;

        Process proc = Runtime.getRuntime().exec(command);
        StringBuffer inBuffer = new StringBuffer();
        InputStream inStream = proc.getInputStream();
        new InputStreamHandler(inBuffer, inStream);

        StringBuffer errBuffer = new StringBuffer();
        InputStream errStream = proc.getErrorStream();
        new InputStreamHandler(errBuffer, errStream);

        try {
            int iReturnValue = proc.waitFor();

            switch (iReturnValue) {
            case EX_SUCC:
                break;
            case EX_WARN:
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                        "Pipe creation report a warning");
                break;
            default:
                throw new IOException("STDERROR:" + errBuffer.toString() + "\nSTDOUT:" + inBuffer.toString());
            }
        } catch (InterruptedException e) {
            throw new IOException("Failed to wait for pipe creation: " + e.getMessage());
        }

        return pipeFilename;
    }

    private boolean mFilesOpen = false;

    private void initFile() throws IOException {

        if (pipeSupported) {
            // instantiate process
            this.mActiveSQLLoaderThread = new SQLLoaderProcess(this.mLoadStatement, Thread.currentThread());
            this.mActiveSQLLoaderThread.start();
        }

        this.mTarget = new FileOutputStream(msDataFile);
        this.mBuffer = new BufferedOutputStream(mTarget);
        this.mWriter = new DataOutputStream(this.mBuffer);
        mFilesOpen = true;

    }

    SQLLoaderProcess mActiveSQLLoaderThread = null;

    class SQLLoaderProcess extends Thread {

        private String command;
        private int finalStatus = -999;
        private Thread parent;
        private SQLException exception = null;

        public SQLLoaderProcess(String command, Thread parent) {
            super();
            this.command = command;
            this.parent = parent;
        }

        @Override
        public void run() {
            InputStream inStream;
            InputStream errStream;
            try {
                mPipeToProcess = Runtime.getRuntime().exec(command);
                mInBuffer = new StringBuffer();
                inStream = mPipeToProcess.getInputStream();
                new InputStreamHandler(mInBuffer, inStream);

                mErrBuffer = new StringBuffer();
                errStream = mPipeToProcess.getErrorStream();
                new InputStreamHandler(mErrBuffer, errStream);

                finalStatus = mPipeToProcess.waitFor();

                mPipeToProcess = null;

                // interrupt parent if an error occurs
                switch (finalStatus) {
                case EX_SUCC:
                    break;
                case EX_WARN:
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                            "SQL*Loader reported a warning, check the log " + mControlFile + ".log");
                    break;
                default:
                    Thread.sleep(1000);
                    if (mFilesOpen == false) {
                        // this is here in the event that the pipe fails and hangs the other thread
                        ResourcePool.LogException(new KETLWriteException("STDERROR:" + mErrBuffer.toString()
                                + "\nSTDOUT:" + mInBuffer.toString()), parent);
                        parent.interrupt();
                    }
                    exception = new SQLException("SQL*Loader Failed\nExtended Log: " + mControlFile + ".log\nError code: " + this.finalStatus + "\nSTDERROR:" + mErrBuffer.toString() + "\nSTDOUT:"
                            + mInBuffer.toString());
                }

            } catch (Exception e) {
                ResourcePool.LogException(new KETLWriteException("STDERROR:" + mErrBuffer.toString() + "\nSTDOUT:"
                        + mInBuffer.toString()), parent);
                ResourcePool.LogException(e, parent);
                parent.interrupt();
            }

        }

    }

    private void deleteDataFile() {

        File fl = new File(msDataFile);

        if (fl.exists()) {
            fl.delete();
            fl = new File(msDataFile);
        }

    }

    private void closeFile() throws IOException {
        if (this.mWriter != null) {
            this.mWriter.close();
            this.mWriter = null;
        }

        if (!this.pipeSupported) {
            if (this.mBuffer != null) {
                this.mBuffer.close();
                this.mBuffer = null;
            }
        }

        if (this.mTarget != null) {
            this.mTarget.close();
            this.mTarget = null;
        }
    }

    public static StatementWrapper prepareStatement(Connection mcDBConnection, String pTableName, String loadStatement,
            DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper, boolean pipe) throws SQLException {
        try {
            return new SQLLoaderStatementWrapper(mcDBConnection, pTableName, loadStatement, madcdColumns, helper, pipe);
        } catch (IOException e) {
            throw new SQLException(e.toString());
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new RuntimeException("Single updates not supported yet by SQLLoader");
    }

    @Override
    public void setParameterFromClass(int parameterIndex, Class pClass, Object pDataItem, int maxCharLength,
            Element pXMLConfig) throws SQLException {
        if (pClass == Short.class || pClass == short.class) {
            this.setShort(parameterIndex, (Short) pDataItem);
        }
        else if (pClass == Integer.class || pClass == int.class) {
            this.setInt(parameterIndex, (Integer) pDataItem);
        }
        else if (pClass == Double.class || pClass == double.class) {
            this.setDouble(parameterIndex, (Double) pDataItem);
        }
        else if (pClass == Float.class || pClass == float.class) {
            this.setFloat(parameterIndex, (Float) pDataItem);

        }
        else if (pClass == Long.class || pClass == long.class) {
            this.setLong(parameterIndex, (Long) pDataItem);
        }
        else if (pClass == String.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.VARCHAR);
            else
                this.setString(parameterIndex, (String) pDataItem);
        }
        else if (pClass == java.util.Date.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
            else
                this.setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date) pDataItem).getTime()));
        }
        else if (pClass == java.sql.Date.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.DATE);
            else
                this.setSQLDate(parameterIndex, (java.sql.Date) pDataItem);
        }
        else if (pClass == java.sql.Time.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.TIME);
            else
                this.setSQLTime(parameterIndex, (java.sql.Time) pDataItem);
        }
        else if (pClass == java.sql.Timestamp.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
            else
                this.setSQLTimestamp(parameterIndex, (java.sql.Timestamp) pDataItem);
        }
        else if (pClass == Boolean.class || pClass == boolean.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.BOOLEAN);
            else
                this.setBoolean(parameterIndex, (Boolean) pDataItem);
        }
        else if (pClass == Character.class || pClass == char.class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.BOOLEAN);
            else
                this.setString(parameterIndex, pDataItem.toString());
        }
        else if (pClass == Byte[].class || pClass == byte[].class) {
            if (pDataItem == null)
                this.setNull(parameterIndex, java.sql.Types.BLOB);
            else
                this.setByteArrayValue(parameterIndex, (byte[]) pDataItem);
        }
        else
            throw new RuntimeException("Datatype" + pClass.getCanonicalName() + " is not supported by SQL*Loader");

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
    private FileOutputStream mTarget = null;
    private DataOutputStream mWriter = null;
    private int rowsInThisBatch = 0;

    public void close() throws SQLException {
        if (pipeSupported)
            deleteDataFile();

        if (mPipeToProcess != null)
            mPipeToProcess.destroy();

        // delete control file
        File fl = new File(mControlFile);
        fl.delete();        
    }

    private String msDataFile;

    public int[] executeBatch() throws SQLException {
        int[] res = new int[this.rowsInThisBatch];
        // copy data from the given input stream to the table
        try {
            int iReturnValue;

            // get dataFile
            this.closeFile();
            // execute data file
            if (!this.pipeSupported) {
                this.mPipeToProcess = Runtime.getRuntime().exec(this.mLoadStatement);
                this.mInBuffer = new StringBuffer();
                InputStream inStream = this.mPipeToProcess.getInputStream();
                new InputStreamHandler(this.mInBuffer, inStream);

                this.mErrBuffer = new StringBuffer();
                InputStream errStream = this.mPipeToProcess.getErrorStream();
                new InputStreamHandler(this.mErrBuffer, errStream);

                iReturnValue = this.mPipeToProcess.waitFor();
                this.mPipeToProcess = null;
                deleteDataFile();
            }
            else {
                while (this.mActiveSQLLoaderThread.isAlive()) {
                    Thread.sleep(100);
                }

                iReturnValue = this.mActiveSQLLoaderThread.finalStatus;

                if (this.mActiveSQLLoaderThread.exception != null)
                    throw this.mActiveSQLLoaderThread.exception;
            }

            switch (iReturnValue) {
            case EX_SUCC:
                break;
            case EX_WARN:
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                        "SQL*loader reporting a a warning");
                break;
            default:
                throw new SQLException("STDERROR:" + this.mErrBuffer.toString() + "\nSTDOUT:"
                        + this.mInBuffer.toString());
            }

            java.util.Arrays.fill(res, 1);

            rowsInThisBatch = 0;
        } catch (Exception e) {
            throw new SQLException(e.toString());
        }
        return res;
    }

    private static final int EX_SUCC = 0;
    private static final int EX_WARN = 2;
    private int recordLen = 0, recordLenSize = 7;

    public void addBatch() throws SQLException {
        try {

            if (this.mWriter == null)
                this.initFile();

            this.mWriter.write(pad(Integer.toString(recordLen), recordLenSize, '0').getBytes());

            recordLen = 0;
            for (int i = 0; i < mColumns; i++) {
                this.mWriter.write((byte[]) this.mDatums[i]);
                if (this.mDatumNeedsDelimiter[i] && i < (mColumns - 1)) {
                    this.mWriter.write(mDelimiter.getBytes());
                }
                this.mDatums[i] = null;
            }

        } catch (Exception e) {
            SQLException e1 = new SQLException(e.toString());
            e1.setStackTrace(e.getStackTrace());
            throw e1;
        }
        this.rowsInThisBatch++;
    }

    private final static byte[] TRUE = "1".getBytes();
    private final static byte[] FALSE = "0".getBytes();

    public void setBoolean(int pos, boolean arg0) {
        this.setObject(pos, arg0 ? TRUE : FALSE);
    }

    public void setByteArrayValue(int pos, byte[] arg0) {
        String sLen = Integer.toString(arg0.length);
        sLen = this.pad(sLen, this.mDatumPadLength[pos - 1], '0');
        byte[] a = sLen.getBytes();

        byte[] z = new byte[arg0.length + a.length];
        System.arraycopy(a, 0, z, 0, a.length);
        System.arraycopy(arg0, 0, z, a.length, arg0.length);

        this.setObject(pos, z);
    }

    public void setFloat(int pos, Float arg0) {
        this.setObject(pos, packF4(arg0));
    }

    public void setInt(int pos, Integer arg0) {
        this.setObject(pos, pack4(arg0));
    }

    public void setLong(int pos, Long arg0) {
        this.setObject(pos, pack8(arg0));
    }

    public void setShort(int pos, Short arg0) {
        this.setObject(pos, pack2(arg0));
    }

    public void setDouble(int pos, Double arg0) {
        this.setObject(pos, packF8(arg0));
    }

    public void setNull(int pos, int dataType) {
        switch (dataType) {
        case java.sql.Types.VARCHAR:
        case java.sql.Types.BLOB:
            mDatums[pos - 1] = this.pad("0", this.mDatumPadLength[pos - 1], '0').getBytes();
            break;
        default:
            mDatums[pos - 1] = mNull;
        }

        recordLen += mDatums[pos - 1].length + (this.mDatumNeedsDelimiter[pos - 1] ? DEL_LENGTH : 0);

    }

    private void setObject(int pos, byte[] arg0) {

        mDatums[pos - 1] = arg0;
        recordLen += arg0.length + (this.mDatumNeedsDelimiter[pos - 1] ? DEL_LENGTH : 0);

    }

    public void setString(int pos, String arg0) {

        this.setObject(pos, (this.pad(Integer.toString(arg0.length()), this.mDatumPadLength[pos - 1], '0') + arg0)
                .getBytes());

    }

    public void setTimestamp(int pos, Date arg0) {
        this.setObject(pos, this.dateFormatter.format(arg0).getBytes());
    }

    public void setSQLDate(int pos, java.sql.Date arg0) {

        this.setObject(pos, this.sqlDateFormatter.format(arg0).getBytes());
    }

    public void setSQLTimestamp(int pos, java.sql.Timestamp arg0) {

        if (mDatumTrueSQLTimestamp[pos - 1] == false) {
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
        return pack4(val == null ? null : Float.floatToIntBits(val));
    }

    public static byte[] packF8(Double val) {
        return pack8(val == null ? null : Double.doubleToLongBits(val));
    }

    StringBuffer tmpPadBuffer = new StringBuffer();

    private String pad(String word, int len, char padChar) throws Error {
        int wordLen = word.length();

        int diff = len - wordLen;

        if (diff == 0)
            return word;
        if (diff < 0)
            throw new Error("Input string length greater than requested length");

        tmpPadBuffer.setLength(0);

        for (int i = 0; i < diff; i++)
            tmpPadBuffer.append(padChar);

        tmpPadBuffer.append(word);

        return tmpPadBuffer.toString();
    }

    public void setSQLTime(int pos, java.sql.Time arg0) {
        this.setObject(pos, this.sqlTimeFormatter.format(arg0).getBytes());
    }
}
