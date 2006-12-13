package com.kni.etl.sessionizer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
final public class OLD_PGCopyWriter {

    private Connection mConnection;
    private String msLoadCommand = null;
    private int lineStart;
    private int fieldStart = 0;
    private int rrIndex = 0;
    private static byte[] dataEnd = "\\.".getBytes();
    private static byte[] rowEnd = "\n".getBytes();

    /**
     * number of bytes that were read in the last read command.
     */
    private int bytesRead;

    /**
     * total rows read from the data file.
     */
    private long totalRowsRead = 0;

    /**
     * we use numDelims to keep track of which delimiter we are at within the line.
     */
    private int numDelims = 0;

    /**
     * number of batches completed so far.
     */
    private int batchesCompleted = 0;
    private byte[] buf;
    private long startTime = System.currentTimeMillis();

    /**
     * data backup start index.
     */
    private int totalBytes = 0;
    private int rowsInThisBatch = 0;
    private int blankRowsInThisBatch = 0;
    private int bytesLeftOver = 0;
    private int concurrent = 0;
    private int batchSize;
    private int batchByteLimit;
    private static final int MEGABYTE = 1048576;
    String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    DateFormat dateFormatter;
    String doubleFormat;
    NumberFormat doubleFormatter;
    boolean executeLastBatch = true;
    CopyManager copy;
    InputStream mInput;

    public Connection getConnection() {
        return this.mConnection;
    }

    /**
     *
     */
    public OLD_PGCopyWriter(Connection con) throws SQLException {
        super();

        mConnection = con;
        copy = ((org.postgresql.PGConnection) mConnection).getCopyAPI();

        buf = new byte[1 * MEGABYTE];
        this.dateFormatter = new SimpleDateFormat(dateTimeFormat);
        this.doubleFormatter = new DecimalFormat();
        this.doubleFormatter.setGroupingUsed(false);
    }

    /**
     * Time to execute the COPY commands for all the batch. Data is already sent away. Here we execute the command in
     * all the relevant databases and wait for the results. If there are any errors or failures they are taken care of.
     * 
     * @param segDbWriter
     * @param dgLoad
     * @param batchesCompleted
     * @throws LoaderIOException
     * @throws IOException when data can't be written into the reject file.
     */
    private void addToBatch(byte[] batchDataOuts, int pStart, int pLength) throws SQLException {
        // copy data from the given input stream to the table
        InputStream input = new ByteArrayInputStream(batchDataOuts, pStart, pLength);
        copy.copyInQuery(msLoadCommand, input);

        pos = 0;
        batchesCompleted++;

        /* reset variables for next batch */
        rowsInThisBatch = 0;
        blankRowsInThisBatch = 0;
    }

    /**
     * Parse the data, escape it as necessary and write it away.
     * 
     * @param is The input stream from which to load data.
     * @param segDbWriter The array of DG databases.
     * @param bufferSize The size of the input-buffer to use.
     * @param dgLoad The parsed LOAD command.
     * @throws LoaderIOException if writing and communicating with DG nodes failed at some point.
     * @throws IOException for standard any non-related DG node operations, such as reading from a data file. <p/>
     *             application schema. For example: this exception will be thrown if table "foo" is specified in the
     *             LOAD statement, but does not actually exist in the schema.
     */
    public boolean addBatch() throws SQLException, IOException {
        finishRow();
        this.rowsInThisBatch++;

        if (pos > (buf.length - 50000)) {
            // need to increase buffer size by 50% to stop partial batches
            executeBatch();

            buf = new byte[buf.length + MEGABYTE];
        }

        return true;
    }

    public boolean executeBatch() throws SQLException {
        String x = new String(buf, 0, pos);

        if (x.length() > 0) {
            addToBatch(buf, 0, pos);
        }

        return true;
    }

    public boolean loadCommandReady() {
        if (this.msLoadCommand == null) {
            return false;
        }

        return true;
    }

    public String loadCommand() {
        return this.msLoadCommand;
    }

    public boolean close() throws SQLException {
        return true;
    }

    private OutputStream mOutputStream = null;
    private static byte[] mDelimiter = "|".getBytes();
    private static byte[] mNull = "\\N".getBytes();
    private int pos;

    private final void byteAppend(byte[] arg0) {
        System.arraycopy(arg0, 0, buf, pos, arg0.length);
        pos = pos + arg0.length;
    }

    public void setInt(int pos, int arg0) throws IOException {
        fieldSeperator();
        byteAppend(this.copy.encodeString(Integer.toString(arg0)));
    }

    public void setLong(int pos, long arg0) throws IOException {
        fieldSeperator();
        byteAppend(this.copy.encodeString(Long.toString(arg0)));
    }

    public void setFloat(int pos, float arg0) throws IOException {
        fieldSeperator();
        byteAppend(this.copy.encodeString(Float.toString(arg0)));
    }

    public void setDouble(int pos, double arg0, int arg1) throws IOException {
        fieldSeperator();
        doubleFormatter.setMaximumFractionDigits(arg1);

        String tmp = doubleFormatter.format(arg0);
        byteAppend(tmp.getBytes());
    }

    public void setString(int pos, String arg0) throws IOException {
        fieldSeperator();
        byteAppend(escape(arg0));
    }

    public void setBoolean(int pos, boolean arg0) throws IOException {
        fieldSeperator();
        byteAppend(this.copy.encodeString(Boolean.toString(arg0)));
    }

    public void setByteArrayValue(int pos, byte[] arg0) throws IOException {
        fieldSeperator();
        byteAppend(arg0);
    }

    public void setTimestamp(int pos, Date arg0) throws IOException {
        fieldSeperator();

        String tmp = this.dateFormatter.format(arg0);
        byteAppend(this.copy.encodeString(tmp));
    }

    public void setNull(int pos, int dataType) throws IOException {
        fieldSeperator();
        byteAppend(mNull);
    }

    public String createLoadCommand(String pTableName, String[] pColumns) {
        StringBuffer sb = new StringBuffer("COPY ");

        sb.append(pTableName);
        sb.append(" (");

        for (int i = 0; i < pColumns.length; i++) {
            sb.append(pColumns[i]);

            if (i < (pColumns.length - 1)) {
                sb.append(",");
            }
        }

        sb.append(") from STDIN with DELIMITER '|';\n");

        this.msLoadCommand = sb.toString();

        return msLoadCommand;
    }

    private void finishRow() throws IOException {
        this.byteAppend(rowEnd);
        inRow = false;
    }

    private void finish() throws IOException {
        this.byteAppend(dataEnd);
    }

    private byte[] escape(String mString) throws IOException {
        int len = mString.length();
        boolean escapeNeeded = false;

        // check for escaping needed
        if ((mString.indexOf('\\') != -1) || (mString.indexOf('|') != -1) || (mString.indexOf('\n') != -1)
                || (mString.indexOf('\r') != -1)) {
            escapeNeeded = true;
        }

        // escape string if needed
        if (escapeNeeded) {
            StringBuffer sb = new StringBuffer(len);

            for (int i = 0; i < len; i++) {
                char c = mString.charAt(i);

                switch (c) {
                case '\\':
                case '|':
                case '\n':
                case '\r':
                    sb.append("\\");

                default:
                    sb.append(c);
                }
            }

            return this.copy.encodeString(sb.toString());
        }

        return this.copy.encodeString(mString);
    }

    boolean inRow = false;

    private void fieldSeperator() throws IOException {
        if (inRow) {
            byteAppend(mDelimiter);
        }
        else {
            lineStart = pos;
            inRow = true;
        }
    }

    public int getBatchByteSize() {
        return this.buf.length;
    }

    public int getBatchSize() {
        return this.rowsInThisBatch;
    }

}
