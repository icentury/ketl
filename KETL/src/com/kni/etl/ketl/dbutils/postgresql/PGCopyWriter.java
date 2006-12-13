package com.kni.etl.ketl.dbutils.postgresql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.postgresql.copy.CopyManager;

/*
 * Created on Apr 20, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final public class PGCopyWriter {

    private static final String dataEnd = "\\.\n";
    private final static String mDelimiter = "|";
    private final static String mNull = "\\N";
    private static final String rowEnd = "\n";
    private int batchesCompleted = 0;
    private CopyManager copy;
    private DateFormat dateFormatter;
    private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    private NumberFormat doubleFormatter;
    private int mColumns = -1;
    private Connection mConnection;
    private Object[] mDatums;
    private Class[] mDatumTypes;
    private String msLoadCommand = null;
    private StringBuilder mWriter = new StringBuilder();
    private int rowsInThisBatch = 0;
    private Charset mEncoder;

    public PGCopyWriter(Connection con) throws SQLException {
        super();

        this.mConnection = con;
        this.copy = ((org.postgresql.PGConnection) mConnection).getCopyAPI();

        this.dateFormatter = new SimpleDateFormat(dateTimeFormat);
        this.doubleFormatter = new DecimalFormat();
        this.copy.setCopyBufferSize(1 << 20);
        this.doubleFormatter.setGroupingUsed(false);
        this.mEncoder = Charset.forName(this.copy.getEncoding());

    }

    public boolean close() throws SQLException {
        return true;
    }

    public String createLoadCommand(String pTableName, String[] pColumns) {
        StringBuffer sb = new StringBuffer("COPY ");

        sb.append(pTableName);
        sb.append(" (");

        this.mColumns = pColumns.length;
        this.mDatums = new Object[this.mColumns];
        this.mDatumTypes = new Class[this.mColumns];

        for (int i = 0; i < mColumns; i++) {
            sb.append(pColumns[i]);

            if (i < (mColumns - 1)) {
                sb.append(",");
            }
        }

        sb.append(") from STDIN with DELIMITER '|';\n");

        this.msLoadCommand = sb.toString();

        return msLoadCommand;
    }

    StringBuilder sb = new StringBuilder();

    private String escape(String mString) throws IOException {

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

    private byte[] mBuffer;
    private int mLoadLen = 0;

    public byte[] badLoadContents() {
        byte[] dump = new byte[mLoadLen];
        System.arraycopy(this.mBuffer, 0, dump, 0, this.mLoadLen);
        return dump;
    }

    public void executeBatch() throws IOException, SQLException {
        // copy data from the given input stream to the table
        this.mWriter.append(dataEnd);

        // encode String as byte array
        mLoadLen = this.encode(this.mWriter.toString());

        // reset stringbuilder to start
        this.mWriter.setLength(0);

        // map input stream arround array.
        InputStream input = new ByteArrayInputStream(this.mBuffer, 0, mLoadLen);

        // send command
        copy.copyInQuery(msLoadCommand, input);

        // increment batch count
        batchesCompleted++;

        // reset variables for next batch
        rowsInThisBatch = 0;
    }

    private int encode(String arg0) {
        ByteBuffer bb = this.mEncoder.encode(arg0);

        int len = bb.limit();
        if (this.mBuffer == null || len > this.mBuffer.length)
            this.mBuffer = new byte[len];

        bb.get(this.mBuffer, 0, len);
        return len;
    }

    public boolean addBatch() throws SQLException, IOException {
        for (int i = 0; i < this.mColumns; i++) {
            if (i > 0)
                this.mWriter.append(mDelimiter);

            this.mWriter.append(this.mDatums[i]);
            this.mDatums[i] = null;
        }
        this.mWriter.append(rowEnd);

        this.rowsInThisBatch++;

        return true;
    }

    public int getBatchSize() {
        return this.rowsInThisBatch;
    }

    public Connection getConnection() {
        return this.mConnection;
    }

    public String loadCommand() {
        return this.msLoadCommand;
    }

    public boolean loadCommandReady() {
        if (this.msLoadCommand == null) {
            return false;
        }

        return true;
    }

    public void setBoolean(int pos, boolean arg0) throws IOException {
        this.setObject(pos, arg0);
    }

    public void setByteArrayValue(int pos, byte[] arg0) throws IOException {
        this.setObject(pos, arg0);
    }

    public void setDouble(int pos, double arg0, int arg1) throws IOException {
        doubleFormatter.setMaximumFractionDigits(arg1);
        this.setObject(pos, doubleFormatter.format(arg0));
    }

    public void setFloat(int pos, float arg0) throws IOException {
        this.setObject(pos, arg0);
    }

    public void setInt(int pos, int arg0) throws IOException {
        this.setObject(pos, arg0);
    }

    public void setLong(int pos, long arg0) throws IOException {
        this.setObject(pos, arg0);
    }

    public void setNull(int pos, int dataType) throws IOException {
        mDatums[pos - 1] = mNull;
    }

    private void setObject(int pos, Object arg0) {
        mDatums[pos - 1] = arg0;
        if (this.mDatumTypes[pos - 1] == null)
            this.mDatumTypes[pos - 1] = mDatums[pos - 1].getClass();
    }

    public void setString(int pos, String arg0) throws IOException {
        this.setObject(pos, escape(arg0));
    }

    public void setTimestamp(int pos, Date arg0) throws IOException {
        this.setObject(pos, this.dateFormatter.format(arg0));
    }

}
