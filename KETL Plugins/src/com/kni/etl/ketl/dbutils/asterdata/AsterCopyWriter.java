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
package com.kni.etl.ketl.dbutils.asterdata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.writer.OutputFile;

// TODO: Auto-generated Javadoc
/**
 * The Class PGCopyWriter.
 */
final public class AsterCopyWriter {

	class StreamReader extends Thread {

		private final CopyManagerInterface copyMngr;

		private Exception exception;

		private PipedInputStream is;

		private final String loadCommand;

		private OutputStreamWriter writer;

		private PipedOutputStream pos;

		private BufferedOutputStream bos;

		private final Thread parent;

		private final int bufferSize;

		private BufferedInputStream bis;

		public StreamReader(CopyManagerInterface copyMngr, String loadCommand, int bufferSize, Thread parentThread) {
			super();
			this.copyMngr = copyMngr;
			this.copyMngr.setCopyBufferSize(bufferSize);
			this.loadCommand = loadCommand;
			this.parent = parentThread;
			this.bufferSize = bufferSize;
			this.setName(parentThread.getName() + " - CopyWriter");
		}

		@Override
		public void run() {
			try {
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "loadCommand " + loadCommand);
				copyMngr.copyInQuery(loadCommand, bis);
				bis.close();
				is.close();
				is = null;
			} catch (Exception e) {
				exception = e;
				try {
					while (is != null && is.read() != -1) {
						// absorb incoming data until writer notices error
					}
				} catch (IOException e1) {
					this.exception = e;
				}
			}
		}

		public void throwPendingException() throws SQLException {
			if (this.exception != null) {
				is = null;
				throw new SQLException(this.exception);
			}
		}

		public void close() throws SQLException {
			try {
				this.writer.flush();
				this.writer.close();
				bos.flush();
				bos.close();
				pos.flush();
				pos.close();

				while (this.isAlive()) {
					Thread.sleep(1000);
				}
				this.throwPendingException();
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}

		public Writer getWriter() throws IOException {
			String encoding = copyMngr.getEncoding();
			CharsetEncoder charSet = (encoding == null ? java.nio.charset.Charset.defaultCharset().newEncoder() : java.nio.charset.Charset.forName(encoding).newEncoder());

			is = new PipedInputStream();
			bis = new BufferedInputStream(is, bufferSize);
			pos = new PipedOutputStream(is);
			bos = new BufferedOutputStream(pos, bufferSize);
			this.writer = new OutputStreamWriter(bos, charSet);

			return this.writer;
		}

	}

	/** The Constant dataEnd. */
	private static final String dataEnd = "\\.\n";

	/** The Constant mDelimiter. */
	private final static String mDelimiter = "|";

	/** The Constant mNull. */
	private final static String mNull = "\\N";

	/** The Constant rowEnd. */
	private static final String rowEnd = "\n";

	private int batchCounter = 1;

	/** The batches completed. */
	private int batchesCompleted = 0;

	private final boolean compress;

	/** The copy. */
	private CopyManagerInterface copy;

	private Integer copyBufferSize = null;

	/** The date formatter. */
	private DateFormat dateFormatter;

	/** The date time format. */
	private final String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";

	/** The double formatter. */
	private NumberFormat doubleFormatter;

	/** The buffer. */
	private byte[] mBuffer;

	private String[] mColumnNames;

	/** The columns. */
	private int mColumns = -1;

	/** The datums. */
	private String[] mDatums;

	/** The datum types. */
	private Class[] mDatumTypes;

	/** The encoder. */
	private Charset mEncoder;

	/** The load len. */
	private final int mLoadLen = 0;

	/** The ms load command. */
	private String msLoadCommand = null;

	private long mStartTime;

	private String mTableName;

	private int[] precisions;

	/** The writer. */
	// private StringBuilder mWriter = new StringBuilder();
	/** The rows in this batch. */
	private int rowsInThisBatch = 0;

	/** The sb. */
	StringBuilder sb = new StringBuilder();

	private int[] scales;

	private int[] sizes;

	private OutputFile spool;

	private boolean spoolOnly = false;

	private boolean streaming = false;

	private final String tempPath;

	public AsterCopyWriter(Connection con, boolean streaming) throws SQLException {
		super();
		this.streaming = streaming;
		this.tempPath = System.getenv("java.io.tmpdir");
		this.compress = true;

		setup(con);
	}

	/**
	 * Instantiates a new PG copy writer.
	 * 
	 * @param con
	 *            the con
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	public AsterCopyWriter(Connection con, String tempPath, boolean compress) throws SQLException {
		super();
		this.streaming = false;
		this.tempPath = tempPath;
		this.compress = compress;
		setup(con);

	}

	private StreamReader streamReader;

	private Writer writer;

	private Connection connection;

	private String replace0 = null;

	private String sourceName;

	private Integer partitionID;

	public void setReplaceInvalid(String arg0) {
		this.replace0 = arg0;
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

		if (this.streaming) {
			if (this.streamReader == null) {
				this.streamReader = new StreamReader(this.copy, this.loadCommand(), getDefaultCopyBufferSize(), Thread.currentThread());
				this.writer = this.streamReader.getWriter();
				this.streamReader.start();
			} else
				this.streamReader.throwPendingException();
		} else if (spool == null) {
			this.spool = createSpool();
		}
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < this.mColumns; i++) {
			if (i > 0) {
				writer.append(AsterCopyWriter.mDelimiter);
				sb.append(AsterCopyWriter.mDelimiter);
			}

			writer.append(this.mDatums[i]);
			sb.append(this.mDatums[i]);

			this.mDatums[i] = null;
		}
		// ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
		// "RECORD---- " + sb.toString());
		writer.append(AsterCopyWriter.rowEnd);

		this.rowsInThisBatch++;

		return true;
	}

	public void setSourceName(String arg0) {
		this.sourceName = arg0;
	}

	private OutputFile createSpool() throws IOException {
		OutputFile tmp = new OutputFile(this.mEncoder.name(), compress, 16384);
		String fileName = this.sourceName == null ? this.mTableName + "." + this.mStartTime : this.mTableName + "." + this.sourceName;
		fileName = this.partitionID == null ? fileName : fileName + "." + this.partitionID;
		tmp.open(tempPath + File.separator + fileName + "." + (this.batchCounter++) + (this.compress ? ".gz" : ".dat"));
		this.writer = tmp.getWriter();
		return tmp;
	}

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
	 * Close.
	 * 
	 * @return true, if successful
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	public boolean close() throws SQLException {
		return true;
	}

	private void copyFileIn(CopyManagerInterface copyMngr, File file) throws IOException, SQLException {
		try {
			if (this.copyBufferSize != null)
				copyMngr.setCopyBufferSize(this.copyBufferSize);

			// map input stream arround array.
			FileInputStream fos = new FileInputStream(file);
			BufferedInputStream bos = new BufferedInputStream(fos, getDefaultCopyBufferSize());
			GZIPInputStream zos = null;
			if (this.compress)
				zos = new GZIPInputStream(bos);

			// send command
			copyMngr.copyInQuery(this.msLoadCommand, this.compress ? zos : bos);

			if (this.compress)
				zos.close();
			bos.close();
			fos.close();
		} catch (IOException e) {
			throw e;
		} catch (SQLException e) {
			throw e;
		}
	}

	private int getDefaultCopyBufferSize() {
		return this.copyBufferSize == null ? 16384 : this.copyBufferSize;
	}

	public String createLoadCommand(String pTableName, String[] pColumns, boolean pAutoPartition) {
		StringBuilder sb = new StringBuilder("COPY ");
		mTableName = pTableName;
		mStartTime = System.currentTimeMillis();
		sb.append(pTableName);
		sb.append(" (");

		String autoPartition = pAutoPartition ? "AUTOPARTITION" : "";

		this.mColumns = pColumns.length;
		this.mColumnNames = pColumns;
		this.mDatums = new String[this.mColumns];
		this.mDatumTypes = new Class[this.mColumns];
		this.scales = new int[this.mColumns];
		this.precisions = new int[this.mColumns];
		this.sizes = new int[this.mColumns];
		java.util.Arrays.fill(this.scales, -1);
		java.util.Arrays.fill(this.precisions, -1);
		java.util.Arrays.fill(this.sizes, -1);

		for (int i = 0; i < this.mColumns; i++) {
			sb.append(pColumns[i]);

			if (i < (this.mColumns - 1)) {
				sb.append(",");
			}
		}

		sb.append(") from STDIN with DELIMITER '|' " + autoPartition);// +
		// ";\n");

		this.msLoadCommand = sb.toString();

		return this.msLoadCommand;
	}

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
		if ((mString.indexOf('\\') != -1) || (mString.indexOf('|') != -1) || (mString.indexOf('\n') != -1) || (mString.indexOf('\r') != -1) || mString.indexOf((char) 0) != -1
				|| mString.indexOf('\b') != -1 || mString.indexOf('\t') != -1 || mString.indexOf('\f') != -1) {
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
				if (this.replace0 != null)
					this.sb.append(this.replace0);
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

	public void executeBatch() throws IOException, SQLException {
		this.executeBatch(this.copy);
	}

	public void executeBatch(Connection con) throws IOException, SQLException {
		this.executeBatch(newCopyManager(con));
	}

	public void executeBatch(Connection con, File file) throws IOException, SQLException {
		this.copyFileIn(newCopyManager(con), file);

	}

	private CopyManagerInterface newCopyManager(Connection con) throws SQLException {
		return new CopyManager(con);
	}

	/**
	 * Execute batch.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws SQLException
	 *             the SQL exception
	 */
	private void executeBatch(CopyManagerInterface copyMngr) throws IOException, SQLException {
		// copy data from the given input stream to the table
		// writer.append(AsterCopyWriter.dataEnd);

		if (this.streaming) {
			this.streamReader.close();
			this.streamReader = null;
		} else {
			// encode String as byte array
			spool.close();

			if (this.spoolOnly == false) {
				copyFileIn(copyMngr, spool.getFile());
				spool.getFile().delete();
			}
			spool = null;
		}
		// increment batch count
		this.batchesCompleted++;

		// reset variables for next batch
		this.rowsInThisBatch = 0;
	}

	public Connection getConnection() {
		return this.connection;
	}

	/**
	 * Gets the batch size.
	 * 
	 * @return the batch size
	 */
	public int getBatchSize() {
		return this.rowsInThisBatch;
	}

	public String getSpoolFile() {
		return this.spool.getFile().getAbsolutePath();
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
		validateLength(pos, arg0.length);
		this.setObject(pos, arg0);
	}

	/**
	 * Creates the load command.
	 * 
	 * @param pTableName
	 *            the table name
	 * @param pColumns
	 *            the columns
	 * 
	 * @return the string
	 */

	public void setColumnSizes(int pos, int size, int scale, int precision) {
		this.scales[pos - 1] = scale;
		this.sizes[pos - 1] = size;
		this.precisions[pos - 1] = precision;
	}

	public void setCopyBufferSize(int bufferSize) {
		this.copyBufferSize = bufferSize;
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
		validateNumber(pos, arg0);
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
		validateNumber(pos, arg0);
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
		validateNumber(pos, arg0);
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
		validateNumber(pos, arg0);
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
		this.mDatums[pos - 1] = AsterCopyWriter.mNull;
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
		this.mDatums[pos - 1] = arg0.toString();
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
		validateLength(pos, arg0.length());
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
		this.setObject(pos, this.dateFormatter.format(arg0));
	}

	private void setup(Connection con) throws SQLException {
		if (con != null) {
			this.copy = newCopyManager(con);
			this.copy.setCopyBufferSize(16384);
			this.mEncoder = Charset.forName(this.copy.getEncoding());
		}
		this.connection = con;
		this.dateFormatter = new SimpleDateFormat(this.dateTimeFormat);
		this.doubleFormatter = new DecimalFormat();
		this.doubleFormatter.setGroupingUsed(false);
	}

	public void setEncoding(String charSet) {
		this.mEncoder = Charset.forName(charSet);
	}

	public void spoolOnly(boolean arg0) {
		this.spoolOnly = arg0;
	}

	private void validateLength(int pos, int len) throws IOException {
		if (this.sizes[pos - 1] != -1 && len > this.sizes[pos - 1])
			throw new IOException("Value to large for column " + this.mColumnNames[pos - 1] + ", column size " + this.sizes[pos - 1] + " value length " + len);
	}

	private void validateNumber(int pos, Object arg0) {
		arg0 = null;
	}

	public String getEncoding() {
		return this.mEncoder.name();
	}

	public void setPartitionName(Integer arg0) {
		this.partitionID = arg0;
	}

}
