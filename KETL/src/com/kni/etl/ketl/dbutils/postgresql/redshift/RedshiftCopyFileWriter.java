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
package com.kni.etl.ketl.dbutils.postgresql.redshift;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;

import javax.swing.text.DateFormatter;

import org.postgresql.copy.CopyManager;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.writer.S3OutputFile;
import com.kni.etl.stringtools.FastSimpleDateFormat;

// TODO: Auto-generated Javadoc
/**
 * The Class PGCopyWriter.
 */
final public class RedshiftCopyFileWriter {

	
	/** The Constant mDelimiter. */
	private final static String mDelimiter = "\001";

	/** The Constant mNull. */
	private final static String mNull = "\\N";

	/** The Constant rowEnd. */
	private static final String rowEnd = "\n";

	/** The batches completed. */
	private int batchesCompleted = 0;

	/** The copy. */
	//private CopyManager copy;

	/** The date formatter. */
	private DateFormat dateFormatter;

	/** The date time format. */
	private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";

	/** The double formatter. */
	private NumberFormat doubleFormatter;

	/** The columns. */
	private int mColumns = -1;

	/** The connection. */
	private Connection mConnection;

	/** The datums. */
	private Object[] mDatums;

	/** The datum types. */
	private Class[] mDatumTypes;

	/** The ms load command. */
	private String msLoadCommand = null;
	
	/** The rows in this batch. */
	private int rowsInThisBatch = 0;

	/** The encoder. */
	private Charset mEncoder;

	private S3OutputFile outputFile;

	private String fileName;

	private Object awsKey;

	private String secretKey;

	private String bucketName;

	private String parentDir;

	/**
	 * Instantiates a new PG copy writer.
	 * @param cols 
	 * 
	 * @param con
	 *            the con
	 * @param filePath 
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 * @throws IOException 
	 */
	public RedshiftCopyFileWriter(String[] cols, Connection con, File spoolDir, String awsKey,
			String secretKey, String bucketName, String parentDir,
			String charsetName, boolean zip, int bufferSize)
			throws IOException {
		super();

		this.mFormatter = new SimpleDateFormat();
		this.mFormatter.applyPattern("yyyyMMdd");
		this.mConnection = con;
		//this.copy = ((org.postgresql.PGConnection) this.mConnection).getCopyAPI();
		this.mColumns = cols.length;
		this.mDatums = new Object[this.mColumns];
		this.mDatumTypes = new Class[this.mColumns];
		this.dateFormatter = new SimpleDateFormat(this.dateTimeFormat);
		this.doubleFormatter = new DecimalFormat();
		this.awsKey = awsKey;
		this.secretKey = secretKey;
		this.bucketName = bucketName;
		this.parentDir = parentDir;
		//this.copy.setCopyBufferSize(1 << 20);
		this.doubleFormatter.setGroupingUsed(false);
		//this.mEncoder = Charset.forName(this.copy.getEncoding());
		this.outputFile = new S3OutputFile( awsKey,  secretKey,  bucketName,parentDir,charsetName,zip,bufferSize);
		if (!spoolDir.exists())
			spoolDir.mkdir();

		this.outputFile.openTemp(spoolDir);		
	}

	/**
	 * Close.
	 * 
	 * @return true, if successful
	 * @throws IOException 
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	public boolean close() throws IOException  {
		this.outputFile.close();
		return true;
	}



	/** The sb. */
	StringBuilder sb = new StringBuilder();

	/**
	 * Escape.
	 * 
	 * @param mString
	 *            The string
	 * 
	 * @return the string
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private String escape(String mString) throws IOException {

		// check for escaping needed
		if ((mString.indexOf('\\') != -1) || (mString.indexOf('|') != -1) || (mString.indexOf('\n') != -1)
				|| (mString.indexOf('\r') != -1) || mString.indexOf((char) 0) != -1 || mString.indexOf('\b') != -1
				|| mString.indexOf('\t') != -1 || mString.indexOf('\f') != -1) {
		} else {
			return mString;
		}

		// escape string if needed
		int len = mString.length();

		this.sb.setLength(0);

		for (int i = 0; i < len; i++) {
			char c = mString.charAt(i);

			switch (c) {
			case 0:
				ResourcePool.logMessage("Removing null in string: " + mString);
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

	/** The buffer. */
	private byte[] mBuffer;

	/** The load len. */
	private int mLoadLen = 0;

	/**
	 * Bad load contents.
	 * 
	 * @return the byte[]
	 */
	public byte[] badLoadContents() {
		byte[] dump = new byte[this.mLoadLen];
		System.arraycopy(this.mBuffer, 0, dump, 0, this.mLoadLen);
		return dump;
	}

	/**
	 * Execute batch.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void executeBatch() throws IOException, SQLException {
		// increment batch count
		this.batchesCompleted++;

		// reset variables for next batch
		this.rowsInThisBatch = 0;
	}

	/**
	 * Encode.
	 * 
	 * @param arg0
	 *            the arg0
	 * 
	 * @return the int
	 */
	private int encode(String arg0) {
		ByteBuffer bb = this.mEncoder.encode(arg0);

		int len = bb.limit();
		if (this.mBuffer == null || len > this.mBuffer.length)
			this.mBuffer = new byte[len];

		bb.get(this.mBuffer, 0, len);
		return len;
	}

	/**
	 * Adds the batch.
	 * 
	 * @return true, if successful
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public boolean addBatch() throws SQLException, IOException {
		for (int i = 0; i < this.mColumns; i++) {
			if (i > 0)
				this.outputFile.writer.append(RedshiftCopyFileWriter.mDelimiter);

			this.outputFile.writer.append(this.mDatums[i].toString());
			this.mDatums[i] = null;
		}
		this.outputFile.writer.append(RedshiftCopyFileWriter.rowEnd);

		this.rowsInThisBatch++;

		return true;
	}

	/**
	 * Gets the batch size.
	 * 
	 * @return the batch size
	 */
	public int getBatchSize() {
		return this.rowsInThisBatch;
	}

	/**
	 * Gets the connection.
	 * 
	 * @return the connection
	 */
	public Connection getConnection() {
		return this.mConnection;
	}

	/**
	 * Load command.
	 * 
	 * @return the string
	 */
	public String loadCommand() {
		return this.msLoadCommand;
	}

	/**
	 * Load command ready.
	 * 
	 * @return true, if successful
	 */
	public boolean loadCommandReady() {
		if (this.msLoadCommand == null) {
			return false;
		}

		return true;
	}

	/**
	 * Sets the boolean.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setBoolean(int pos, boolean arg0) throws IOException {
		this.setObject(pos, arg0);
	}

	/**
	 * Sets the byte array value.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setByteArrayValue(int pos, byte[] arg0) throws IOException {
		this.setObject(pos, arg0);
	}

	/**
	 * Sets the double.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * @param arg1
	 *            the arg1
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setDouble(int pos, double arg0, int arg1) throws IOException {
		this.doubleFormatter.setMaximumFractionDigits(arg1);
		this.setObject(pos, this.doubleFormatter.format(arg0));
	}

	/**
	 * Sets the float.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setFloat(int pos, float arg0) throws IOException {
		this.setObject(pos, arg0);
	}

	/**
	 * Sets the int.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setInt(int pos, int arg0) throws IOException {
		this.setObject(pos, arg0);
	}

	/**
	 * Sets the long.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setLong(int pos, long arg0) throws IOException {
		this.setObject(pos, arg0);
	}

	/**
	 * Sets the null.
	 * 
	 * @param pos
	 *            the pos
	 * @param dataType
	 *            the data type
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setNull(int pos, int dataType) throws IOException {
		this.mDatums[pos - 1] = RedshiftCopyFileWriter.mNull;
	}

	/**
	 * Sets the object.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 */
	private void setObject(int pos, Object arg0) {
		
		this.mDatums[pos - 1] = arg0;
		if (this.mDatumTypes[pos - 1] == null)
			this.mDatumTypes[pos - 1] = this.mDatums[pos - 1].getClass();
	}

	/**
	 * Sets the string.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setString(int pos, String arg0) throws IOException {
		this.setObject(pos, this.escape(arg0));
	}

	/**
	 * Sets the timestamp.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void setTimestamp(int pos, Date arg0) throws IOException {
		this.setObject(pos, arg0.getTime());
	}

	public void commit() {
		// TODO when really connected to db commit
		
	}
	
	SimpleDateFormat mFormatter;

	public void setDate(int pos, Date arg0) {
		this.setObject(pos, mFormatter.format(arg0));
		
	}

}
