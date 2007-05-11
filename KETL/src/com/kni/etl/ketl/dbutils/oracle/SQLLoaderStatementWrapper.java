/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
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

// TODO: Auto-generated Javadoc
/**
 * The Class SQLLoaderStatementWrapper.
 */
final public class SQLLoaderStatementWrapper extends BulkLoaderStatementWrapper {

    /** The con. */
    Connection con;
    
    /** The helper. */
    JDBCItemHelper helper;
    
    /** The load statement. */
    String mLoadStatement;
    
    /** The columns. */
    int mColumns;
    
    /** The column details. */
    DatabaseColumnDefinition[] mColumnDetails;
    
    /** The control file. */
    private String mControlFile;
    
    /** The DB case. */
    private int mDBCase;
    
    /** The sql time formatter. */
    private SimpleDateFormat sqlTimeFormatter;
    
    /** The sql timestamp formatter. */
    private SimpleDateFormat sqlTimestampFormatter;
    
    /** The sql date formatter. */
    private SimpleDateFormat sqlDateFormatter;
    
    /** The datum true SQL timestamp. */
    private boolean[] mDatumTrueSQLTimestamp;
    
    /** The Constant DEL_LENGTH. */
    private static final int DEL_LENGTH = "|".getBytes().length;

    /**
     * Instantiates a new SQL loader statement wrapper.
     * 
     * @param connection the connection
     * @param pTableName the table name
     * @param loadStatement the load statement
     * @param madcdColumns the madcd columns
     * @param helper the helper
     * @param pipe the pipe
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws SQLException the SQL exception
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#close()
     */
    @Override
    final public void close() throws SQLException {
        super.close();
        // delete control file
        File fl = new File(this.mControlFile);
        fl.delete();
    }

    /**
     * Prepare statement.
     * 
     * @param mcDBConnection the mc DB connection
     * @param pTableName the table name
     * @param loadStatement the load statement
     * @param madcdColumns the madcd columns
     * @param helper the helper
     * @param pipe the pipe
     * 
     * @return the statement wrapper
     * 
     * @throws SQLException the SQL exception
     */
    public static StatementWrapper prepareStatement(Connection mcDBConnection, String pTableName, String loadStatement,
            DatabaseColumnDefinition[] madcdColumns, JDBCItemHelper helper, boolean pipe) throws SQLException {
        try {
            return new SQLLoaderStatementWrapper(mcDBConnection, pTableName, loadStatement, madcdColumns, helper, pipe);
        } catch (IOException e) {
            throw new SQLException(e.toString());
        }
    }

    /** The Constant mDelimiter. */
    private final static String mDelimiter = "|";
    
    /** The Constant mNull. */
    private final static byte[] mNull = "".getBytes();
    
    /** The date formatter. */
    private DateFormat dateFormatter;
    
    /** The date time format. */
    private String dateTimeFormat = "yyyyMMddHHmmss";
    
    /** The sql time format. */
    private String sqlTimeFormat = "HHmmss";
    
    /** The sql timestamp format. */
    private String sqlTimestampFormat = "yyyyMMddHHmmss";

    /** The ORACL e_ DAT e_ FORMAT. */
    public String ORACLE_DATE_FORMAT = "YYYYMMDDHH24MISS";
    
    /** The ORACL e_ TIM e_ FORMAT. */
    public String ORACLE_TIME_FORMAT = "HH24MISS";
    
    /** The ORACL e_ TIMESTAM p_ FORMAT. */
    public String ORACLE_TIMESTAMP_FORMAT = "YYYYMMDDHH24MISSFF9";
    
    /** The double formatter. */
    private NumberFormat doubleFormatter;
    
    /** The datums. */
    private byte[][] mDatums;
    
    /** The datum pad length. */
    private int[] mDatumPadLength;
    
    /** The datum needs delimiter. */
    private boolean[] mDatumNeedsDelimiter;
    // private FileOutputStream mTarget = null;

    /** The Constant EX_SUCC. */
    private static final int EX_SUCC = 0;
    
    /** The Constant EX_WARN. */
    private static final int EX_WARN = 2;
    
    /** The record len size. */
    private int recordLen = 0, recordLenSize = 7;

    /** The Constant TRUE. */
    private final static byte[] TRUE = "1".getBytes();
    
    /** The Constant FALSE. */
    private final static byte[] FALSE = "0".getBytes();

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setBoolean(int, boolean)
     */
    @Override
    public void setBoolean(int pos, boolean arg0) {
        this.setObject(pos, arg0 ? SQLLoaderStatementWrapper.TRUE : SQLLoaderStatementWrapper.FALSE);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setByteArrayValue(int, byte[])
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setFloat(int, java.lang.Float)
     */
    @Override
    public void setFloat(int pos, Float arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.packF4(arg0));
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setInt(int, java.lang.Integer)
     */
    @Override
    public void setInt(int pos, Integer arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.pack4(arg0));
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setLong(int, java.lang.Long)
     */
    @Override
    public void setLong(int pos, Long arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.pack8(arg0));
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setShort(int, java.lang.Short)
     */
    @Override
    public void setShort(int pos, Short arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.pack2(arg0));
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setDouble(int, java.lang.Double)
     */
    @Override
    public void setDouble(int pos, Double arg0) {
        this.setObject(pos, SQLLoaderStatementWrapper.packF8(arg0));
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setNull(int, int)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setObject(int, byte[])
     */
    @Override
    public void setObject(int pos, byte[] arg0) {

        this.mDatums[pos - 1] = arg0;
        this.recordLen += arg0.length + (this.mDatumNeedsDelimiter[pos - 1] ? SQLLoaderStatementWrapper.DEL_LENGTH : 0);

    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setString(int, java.lang.String)
     */
    @Override
    public void setString(int pos, String arg0) {

        this.setObject(pos, (this.pad(Integer.toString(arg0.length()), this.mDatumPadLength[pos - 1], '0') + arg0)
                .getBytes());

    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setTimestamp(int, java.util.Date)
     */
    @Override
    public void setTimestamp(int pos, Date arg0) {
        this.setObject(pos, this.dateFormatter.format(arg0).getBytes());
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setSQLDate(int, java.sql.Date)
     */
    @Override
    public void setSQLDate(int pos, java.sql.Date arg0) {

        this.setObject(pos, this.sqlDateFormatter.format(arg0).getBytes());
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setSQLTimestamp(int, java.sql.Timestamp)
     */
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

    /**
     * Pack2.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] pack2(Short val) {
        if (val == null)
            return new byte[] { 0, 0, 0 };
        return new byte[] { (byte) (val >> 8), (byte) val.shortValue(), 1 };
    }

    /**
     * Pack4.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] pack4(Integer val) {
        if (val == null)
            return new byte[] { 0, 0, 0, 0, 0, 0 };
        return new byte[] { (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val.intValue(), 1 };
    }

    /**
     * Pack8.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] pack8(Long val) {
        if (val == null)
            return new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        return new byte[] { (byte) (val >> 56), (byte) (val >> 48), (byte) (val >> 40), (byte) (val >> 32),
                (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val.longValue(), 1 };
    }

    /**
     * Pack f4.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] packF4(Float val) {
        return SQLLoaderStatementWrapper.pack4(val == null ? null : Float.floatToIntBits(val));
    }

    /**
     * Pack f8.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] packF8(Double val) {
        return SQLLoaderStatementWrapper.pack8(val == null ? null : Double.doubleToLongBits(val));
    }

    /** The tmp pad buffer. */
    StringBuffer tmpPadBuffer = new StringBuffer();

    /**
     * Pad.
     * 
     * @param word the word
     * @param len the len
     * @param padChar the pad char
     * 
     * @return the string
     * 
     * @throws Error the error
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setSQLTime(int, java.sql.Time)
     */
    @Override
    public void setSQLTime(int pos, java.sql.Time arg0) {
        this.setObject(pos, this.sqlTimeFormatter.format(arg0).getBytes());
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#handleLoaderStatus(int, java.lang.Thread)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#writeRecord()
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#getLoadStatement()
     */
    @Override
    protected String getLoadStatement() {
        return this.mLoadStatement;
    }
}
