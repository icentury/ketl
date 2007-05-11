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

// TODO: Auto-generated Javadoc
/**
 * The Class NetezzaStatementWrapper.
 */
final public class NetezzaStatementWrapper extends BulkLoaderStatementWrapper {

    /** The Constant BOOL_STYLE. */
    private static final String BOOL_STYLE = "1_0";

    /** The Constant DATESTYLE. */
    private static final String DATESTYLE = "yyyy-MM-dd";

    /** The Constant DELIMITER. */
    private static final String DELIMITER = "|";

    /** The Constant ESCAPE_CHAR. */
    private static final String ESCAPE_CHAR = "\\";

    /** The Constant EX_SUCC. */
    private static final int EX_SUCC = 0;

    /** The Constant EX_WARN. */
    private static final int EX_WARN = 2;

    /** The Constant NULL_VALUE. */
    private static final String NULL_VALUE = "''";

    /** The Constant TIMESTYLE. */
    private static final String TIMESTYLE = "HH:mm:ss";

    /** The ENCODING. */
    private static String ENCODING = null;

    /**
     * Prepare statement.
     * 
     * @param mcDBConnection the mc DB connection
     * @param encoding the encoding
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

    /** The ENCODER. */
    private final String ENCODER = java.nio.charset.Charset.forName(NetezzaStatementWrapper.ENCODING).name();

    /** The DELIMITE r_ A s_ BYTES. */
    private final byte[] DELIMITER_AS_BYTES = NetezzaStatementWrapper.DELIMITER.getBytes(this.ENCODER);

    /** The double formatter. */
    private NumberFormat doubleFormatter;

    /** The FALSE. */
    private final byte[] FALSE = "0".getBytes(this.ENCODER);

    /** The all cols. */
    private int mAllCols;

    /** The column details. */
    private DatabaseColumnDefinition[] mColumnDetails;

    /** The control file. */
    private String mControlFile;

    /** The datums. */
    private byte[][] mDatums;

    /** The datum true SQL timestamp. */
    private boolean[] mDatumTrueSQLTimestamp;

    /** The item order map. */
    private int[] mItemOrderMap;

    /** The load statement. */
    private String mLoadStatement;

    /** The RECOR d_ DELIMITE r_ A s_ BYTES. */
    private final byte[] RECORD_DELIMITER_AS_BYTES = "\n".getBytes(this.ENCODER);

    /** The sb. */
    private StringBuffer sb = new StringBuffer();

    /** The sql date formatter. */
    private SimpleDateFormat sqlDateFormatter;

    /** The sql time formatter. */
    private SimpleDateFormat sqlTimeFormatter;

    /** The sql timestamp formatter. */
    private SimpleDateFormat sqlTimestampFormatter;

    /** The TRUE. */
    private final byte[] TRUE = "1".getBytes(this.ENCODER);

    /**
     * Instantiates a new netezza statement wrapper.
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

    /**
     * Reclaim.
     * 
     * @param username the username
     * @param password the password
     * @param tablename the tablename
     * @param database the database
     * @param host the host
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    final public void reclaim(String username, String password, String tablename, String database, String host)
            throws IOException {
        String command = "nzreclaim -quit -blocks -db " + database + " -u " + username + " -pw " + password + " -host "
                + host + " -t" + tablename;

        this.executeCommand(command);

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
     * Escape.
     * 
     * @param mString The string
     * 
     * @return the string
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#getLoadStatement()
     */
    @Override
    protected String getLoadStatement() {
        return this.mLoadStatement;
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setBoolean(int, boolean)
     */
    @Override
    public void setBoolean(int pos, boolean arg0) {
        this.setObject(pos, arg0 ? this.TRUE : this.FALSE);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setByteArrayValue(int, byte[])
     */
    @Override
    public void setByteArrayValue(int pos, byte[] arg0) {
        this.setObject(pos, arg0);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setDouble(int, java.lang.Double)
     */
    @Override
    public void setDouble(int pos, Double arg0) throws SQLException {
        try {
            this.setObject(pos, Double.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setFloat(int, java.lang.Float)
     */
    @Override
    public void setFloat(int pos, Float arg0) throws SQLException {
        try {
            this.setObject(pos, Float.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setInt(int, java.lang.Integer)
     */
    @Override
    public void setInt(int pos, Integer arg0) throws SQLException {
        try {
            this.setObject(pos, Integer.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setLong(int, java.lang.Long)
     */
    @Override
    public void setLong(int pos, Long arg0) throws SQLException {
        try {
            this.setObject(pos, Long.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setNull(int, int)
     */
    @Override
    public void setNull(int pos, int dataType) {
        this.setObject(pos, null);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setObject(int, byte[])
     */
    @Override
    public void setObject(int pos, byte[] arg0) {
        this.mDatums[pos - 1] = arg0;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setShort(int, java.lang.Short)
     */
    @Override
    public void setShort(int pos, Short arg0) throws SQLException {
        try {
            this.setObject(pos, Short.toString(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setSQLDate(int, java.sql.Date)
     */
    @Override
    public void setSQLDate(int pos, java.sql.Date arg0) throws SQLException {
        try {
            this.setObject(pos, this.sqlDateFormatter.format(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setSQLTime(int, java.sql.Time)
     */
    @Override
    public void setSQLTime(int pos, java.sql.Time arg0) throws SQLException {
        try {
            this.setObject(pos, this.sqlTimeFormatter.format(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setSQLTimestamp(int, java.sql.Timestamp)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setString(int, java.lang.String)
     */
    @Override
    public void setString(int pos, String arg0) throws SQLException {
        try {
            this.setObject(pos, this.escape(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#setTimestamp(int, java.util.Date)
     */
    @Override
    public void setTimestamp(int pos, Date arg0) throws SQLException {
        try {
            this.setObject(pos, this.sqlTimestampFormatter.format(arg0).getBytes(this.ENCODER));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.BulkLoaderStatementWrapper#writeRecord()
     */
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
