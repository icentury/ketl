/*
 *  Copyright (C) 2006 Kinetic Networks, Inc. All Rights Reserved. 
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
 *
 *
 * 
 * Created on Apr 20, 2005
 *
 */
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
        this.copy = ((org.postgresql.PGConnection) this.mConnection).getCopyAPI();

        this.dateFormatter = new SimpleDateFormat(this.dateTimeFormat);
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

        for (int i = 0; i < this.mColumns; i++) {
            sb.append(pColumns[i]);

            if (i < (this.mColumns - 1)) {
                sb.append(",");
            }
        }

        sb.append(") from STDIN with DELIMITER '|';\n");

        this.msLoadCommand = sb.toString();

        return this.msLoadCommand;
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

    private byte[] mBuffer;
    private int mLoadLen = 0;

    public byte[] badLoadContents() {
        byte[] dump = new byte[this.mLoadLen];
        System.arraycopy(this.mBuffer, 0, dump, 0, this.mLoadLen);
        return dump;
    }

    public void executeBatch() throws IOException, SQLException {
        // copy data from the given input stream to the table
        this.mWriter.append(PGCopyWriter.dataEnd);

        // encode String as byte array
        this.mLoadLen = this.encode(this.mWriter.toString());

        // reset stringbuilder to start
        this.mWriter.setLength(0);

        // map input stream arround array.
        InputStream input = new ByteArrayInputStream(this.mBuffer, 0, this.mLoadLen);

        // send command
        this.copy.copyInQuery(this.msLoadCommand, input);

        // increment batch count
        this.batchesCompleted++;

        // reset variables for next batch
        this.rowsInThisBatch = 0;
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
                this.mWriter.append(PGCopyWriter.mDelimiter);

            this.mWriter.append(this.mDatums[i]);
            this.mDatums[i] = null;
        }
        this.mWriter.append(PGCopyWriter.rowEnd);

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
        this.doubleFormatter.setMaximumFractionDigits(arg1);
        this.setObject(pos, this.doubleFormatter.format(arg0));
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
        this.mDatums[pos - 1] = PGCopyWriter.mNull;
    }

    private void setObject(int pos, Object arg0) {
        this.mDatums[pos - 1] = arg0;
        if (this.mDatumTypes[pos - 1] == null)
            this.mDatumTypes[pos - 1] = this.mDatums[pos - 1].getClass();
    }

    public void setString(int pos, String arg0) throws IOException {
        this.setObject(pos, this.escape(arg0));
    }

    public void setTimestamp(int pos, Date arg0) throws IOException {
        this.setObject(pos, this.dateFormatter.format(arg0));
    }

}
