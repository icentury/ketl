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
package com.kni.etl.dbutils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Date;

import org.w3c.dom.Element;

import com.kni.etl.ketl.exceptions.KETLWriteException;

// TODO: Auto-generated Javadoc
/**
 * The Class BulkLoaderStatementWrapper.
 */
abstract public class BulkLoaderStatementWrapper extends StatementWrapper {

	/**
	 * The Class InputStreamHandler.
	 */
	class InputStreamHandler extends Thread {

		/** The m_capture buffer. */
		private StringBuilder m_captureBuffer;

		/** The m_stream. */
		private InputStream m_stream;

		/**
		 * Instantiates a new input stream handler.
		 * 
		 * @param captureBuffer
		 *            the capture buffer
		 * @param stream
		 *            the stream
		 */
		InputStreamHandler(StringBuilder captureBuffer, InputStream stream) {
			this.m_stream = stream;
			this.m_captureBuffer = captureBuffer;
			this.start();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			try {
				int nextChar;

				while ((nextChar = this.m_stream.read()) != -1) {
					this.m_captureBuffer.append((char) nextChar);
				}
			} catch (IOException ioe) {
			}
		}
	}

	/**
	 * Gets the standard error message.
	 * 
	 * @return the standard error message
	 */
	final protected String getStandardErrorMessage() {
		return this.mErrBuffer.toString();
	}

	/**
	 * Gets the standard out message.
	 * 
	 * @return the standard out message
	 */
	final protected String getStandardOutMessage() {
		return this.mInBuffer.toString();
	}

	/** The file open. */
	private boolean mFileOpen = false;

	/**
	 * Files open.
	 * 
	 * @return true, if successful
	 */
	final protected boolean filesOpen() {
		return this.mFileOpen;
	}

	/**
	 * The Class LoaderProcess.
	 */
	class LoaderProcess extends Thread {

		/** The command. */
		private String command;

		/** The exception. */
		private SQLException exception = null;

		/** The final status. */
		private int finalStatus = -999;

		/** The parent. */
		private Thread parent;

		/**
		 * Instantiates a new loader process.
		 * 
		 * @param command
		 *            the command
		 * @param parent
		 *            the parent
		 */
		public LoaderProcess(String command, Thread parent) {
			super();
			this.command = command;
			this.parent = parent;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			InputStream inStream;
			InputStream errStream;
			try {
				BulkLoaderStatementWrapper.this.mPipeToProcess = Runtime.getRuntime().exec(this.command);
				BulkLoaderStatementWrapper.this.mInBuffer = new StringBuilder();
				inStream = BulkLoaderStatementWrapper.this.mPipeToProcess.getInputStream();
				new InputStreamHandler(BulkLoaderStatementWrapper.this.mInBuffer, inStream);

				BulkLoaderStatementWrapper.this.mErrBuffer = new StringBuilder();
				errStream = BulkLoaderStatementWrapper.this.mPipeToProcess.getErrorStream();
				new InputStreamHandler(BulkLoaderStatementWrapper.this.mErrBuffer, errStream);

				this.finalStatus = BulkLoaderStatementWrapper.this.mPipeToProcess.waitFor();

				BulkLoaderStatementWrapper.this.mPipeToProcess = null;

				// interrupt parent if an error occurs
				this.exception = BulkLoaderStatementWrapper.this.handleLoaderStatus(this.finalStatus, this);

			} catch (Exception e) {
				ResourcePool.LogException(new KETLWriteException("STDERROR:"
						+ BulkLoaderStatementWrapper.this.getStandardErrorMessage() + "\nSTDOUT:"
						+ BulkLoaderStatementWrapper.this.getStandardOutMessage()), this.parent);
				ResourcePool.LogException(e, this.parent);
				this.parent.interrupt();
			}

		}

	}

	/** The active loader thread. */
	private LoaderProcess mActiveLoaderThread = null;

	/** The buffer. */
	private BufferedOutputStream mBuffer;

	/** The err buffer. */
	private StringBuilder mErrBuffer;

	/** The in buffer. */
	private StringBuilder mInBuffer;

	/** The pipe to process. */
	private Process mPipeToProcess;

	/** The ms data file. */
	private String msDataFile;

	/** The target. */
	private FileOutputStream mTarget = null;

	/** The writer. */
	protected DataOutputStream mWriter = null;

	/** The pipe supported. */
	private boolean pipeSupported = false;

	/** The rows in this batch. */
	private int rowsInThisBatch = 0;

	/**
	 * Instantiates a new bulk loader statement wrapper.
	 * 
	 * @param pipe
	 *            the pipe
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws SQLException
	 *             the SQL exception
	 */
	public BulkLoaderStatementWrapper(boolean pipe) throws IOException, SQLException {
		super();
		this.pipeSupported = pipe;

		if (this.pipeSupported)
			this.msDataFile = this.createPipe();
		else
			this.msDataFile = File.createTempFile("ketl", ".dat").getAbsolutePath();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.StatementWrapper#addBatch()
	 */
	@Override
	final public void addBatch() throws SQLException {
		try {

			if (this.mWriter == null)
				this.initFile();

			this.writeRecord();

		} catch (Exception e) {
			SQLException e1 = new SQLException(e.toString());
			e1.setStackTrace(e.getStackTrace());
			throw e1;
		}
		this.rowsInThisBatch++;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.StatementWrapper#close()
	 */
	@Override
	public void close() throws SQLException {
		if (this.pipeSupported)
			this.deleteDataFile();

		if (this.mPipeToProcess != null)
			this.mPipeToProcess.destroy();

	}

	/**
	 * Close file.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	final protected void closeFile() throws IOException {
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
		this.mFileOpen = false;
	}

	/**
	 * Gets the data file.
	 * 
	 * @return the data file
	 */
	final protected String getDataFile() {
		return this.msDataFile;
	}

	/**
	 * Creates the pipe.
	 * 
	 * @return the string
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	final private String createPipe() throws IOException {
		String pipeFilename;
		String tempdir = System.getProperty("java.io.tmpdir");

		if ((tempdir != null) && (tempdir.endsWith("/") == false) && (tempdir.endsWith("\\") == false)) {
			tempdir = tempdir + File.separator;
		} else if (tempdir == null) {
			tempdir = "";
		}

		pipeFilename = tempdir + "nzPip" + Long.toString(System.nanoTime());
		String command = "mkfifo " + pipeFilename;

		this.executeCommand(command);
		return pipeFilename;
	}

	/**
	 * Execute command.
	 * 
	 * @param command
	 *            the command
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	final protected void executeCommand(String command) throws IOException {
		Process proc = Runtime.getRuntime().exec(command);
		StringBuilder inBuffer = new StringBuilder();
		InputStream inStream = proc.getInputStream();
		new InputStreamHandler(inBuffer, inStream);

		StringBuilder errBuffer = new StringBuilder();
		InputStream errStream = proc.getErrorStream();
		new InputStreamHandler(errBuffer, errStream);

		try {
			if (proc.waitFor() != 0)
				throw new IOException("STDERROR:" + errBuffer.toString() + "\nSTDOUT:" + inBuffer.toString());
		} catch (InterruptedException e) {
			throw new IOException("Command failed: " + e.getMessage());
		}
	}

	/**
	 * Delete data file.
	 */
	final private void deleteDataFile() {

		File fl = new File(this.msDataFile);

		if (fl.exists()) {
			fl.delete();
			fl = new File(this.msDataFile);
		}

	}

	/** The command executed. */
	private boolean mCommandExecuted = false;

	/**
	 * Loader executed.
	 * 
	 * @return true, if successful
	 */
	final public boolean loaderExecuted() {
		return this.mCommandExecuted;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.StatementWrapper#executeBatch()
	 */
	@Override
	final public int[] executeBatch() throws SQLException {
		int[] res = new int[this.rowsInThisBatch];
		// copy data from the given input stream to the table
		try {
			int iReturnValue;

			// get dataFile
			this.closeFile();

			this.mCommandExecuted = true;

			// execute data file
			if (!this.pipeSupported) {
				this.mPipeToProcess = Runtime.getRuntime().exec(this.getLoadStatement());
				this.mInBuffer = new StringBuilder();
				InputStream inStream = this.mPipeToProcess.getInputStream();
				new InputStreamHandler(this.mInBuffer, inStream);

				this.mErrBuffer = new StringBuilder();
				InputStream errStream = this.mPipeToProcess.getErrorStream();
				new InputStreamHandler(this.mErrBuffer, errStream);

				iReturnValue = this.mPipeToProcess.waitFor();
				this.mPipeToProcess = null;
				this.deleteDataFile();
			} else {
				while (this.mActiveLoaderThread.isAlive()) {
					Thread.sleep(100);
				}

				iReturnValue = this.mActiveLoaderThread.finalStatus;

				if (this.mActiveLoaderThread.exception != null)
					throw this.mActiveLoaderThread.exception;
			}

			SQLException e = this.handleLoaderStatus(iReturnValue, Thread.currentThread());

			if (e != null)
				throw e;

			java.util.Arrays.fill(res, 1);

			this.rowsInThisBatch = 0;
		} catch (Exception e) {
			if (e instanceof SQLException)
				throw (SQLException) e;

			SQLException se = new SQLException(e.toString());
			se.setStackTrace(e.getStackTrace());
			throw se;
		}
		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.StatementWrapper#executeUpdate()
	 */
	@Override
	final public int executeUpdate() throws SQLException {
		throw new RuntimeException("Single updates not supported yet by SQLLoader");
	}

	/**
	 * Gets the load statement.
	 * 
	 * @return the load statement
	 */
	abstract protected String getLoadStatement();

	/**
	 * Handle loader status.
	 * 
	 * @param finalStatus
	 *            the final status
	 * @param thread
	 *            the thread
	 * 
	 * @return the SQL exception
	 * 
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	abstract protected SQLException handleLoaderStatus(int finalStatus, Thread thread) throws InterruptedException;

	/**
	 * Inits the file.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	final protected void initFile() throws IOException {

		if (this.pipeSupported) {
			// instantiate process
			this.mActiveLoaderThread = new LoaderProcess(this.getLoadStatement(), Thread.currentThread());
			this.mActiveLoaderThread.start();
		}

		this.mTarget = new FileOutputStream(this.msDataFile);
		this.mBuffer = new BufferedOutputStream(this.mTarget);
		this.mWriter = new DataOutputStream(this.mBuffer);
		this.mFileOpen = true;
	}

	/**
	 * Sets the boolean.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setBoolean(int pos, boolean arg0) throws SQLException;

	/**
	 * Sets the byte array value.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setByteArrayValue(int pos, byte[] arg0) throws SQLException;

	/**
	 * Sets the double.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setDouble(int pos, Double arg0) throws SQLException;

	/**
	 * Sets the float.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setFloat(int pos, Float arg0) throws SQLException;

	/**
	 * Sets the int.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setInt(int pos, Integer arg0) throws SQLException;

	/**
	 * Sets the long.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setLong(int pos, Long arg0) throws SQLException;

	/**
	 * Sets the null.
	 * 
	 * @param pos
	 *            the pos
	 * @param dataType
	 *            the data type
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setNull(int pos, int dataType) throws SQLException;

	/**
	 * Sets the object.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setObject(int pos, byte[] arg0) throws SQLException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.StatementWrapper#setParameterFromClass(int, java.lang.Class, java.lang.Object, int,
	 *      org.w3c.dom.Element)
	 */
	@Override
	final public void setParameterFromClass(int parameterIndex, Class pClass, Object pDataItem, int maxCharLength,
			Element pXMLConfig) throws SQLException {

		if (pClass == Short.class || pClass == short.class) {
			this.setShort(parameterIndex, (Short) pDataItem);
		} else if (pClass == Integer.class || pClass == int.class) {
			this.setInt(parameterIndex, (Integer) pDataItem);
		} else if (pClass == Double.class || pClass == double.class) {
			this.setDouble(parameterIndex, (Double) pDataItem);
		} else if (pClass == Float.class || pClass == float.class) {
			this.setFloat(parameterIndex, (Float) pDataItem);

		} else if (pClass == Long.class || pClass == long.class) {
			this.setLong(parameterIndex, (Long) pDataItem);
		} else if (pClass == String.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.VARCHAR);
			else
				this.setString(parameterIndex, (String) pDataItem);
		} else if (pClass == java.util.Date.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
			else
				this.setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date) pDataItem).getTime()));
		} else if (pClass == java.sql.Date.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.DATE);
			else
				this.setSQLDate(parameterIndex, (java.sql.Date) pDataItem);
		} else if (pClass == java.sql.Time.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.TIME);
			else
				this.setSQLTime(parameterIndex, (java.sql.Time) pDataItem);
		} else if (pClass == java.sql.Timestamp.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
			else
				this.setSQLTimestamp(parameterIndex, (java.sql.Timestamp) pDataItem);
		} else if (pClass == Boolean.class || pClass == boolean.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.BOOLEAN);
			else
				this.setBoolean(parameterIndex, (Boolean) pDataItem);
		} else if (pClass == BigDecimal.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.NUMERIC);
			else
				this.setBigDecimal(parameterIndex, (BigDecimal) pDataItem);
		} else if (pClass == Character.class || pClass == char.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.BOOLEAN);
			else
				this.setString(parameterIndex, pDataItem.toString());
		} else if (pClass == Byte[].class || pClass == byte[].class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.BLOB);
			else
				this.setByteArrayValue(parameterIndex, (byte[]) pDataItem);
		} else
			throw new RuntimeException("Datatype" + pClass.getCanonicalName() + " is not supported by SQL*Loader");

	}

	abstract public void setBigDecimal(int parameterIndex, BigDecimal dataItem) throws SQLException;

	/**
	 * Sets the short.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setShort(int pos, Short arg0) throws SQLException;

	/**
	 * Sets the SQL date.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setSQLDate(int pos, java.sql.Date arg0) throws SQLException;

	/**
	 * Sets the SQL time.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setSQLTime(int pos, java.sql.Time arg0) throws SQLException;

	/**
	 * Sets the SQL timestamp.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setSQLTimestamp(int pos, java.sql.Timestamp arg0) throws SQLException;

	/**
	 * Sets the string.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setString(int pos, String arg0) throws SQLException;

	/**
	 * Sets the timestamp.
	 * 
	 * @param pos
	 *            the pos
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract public void setTimestamp(int pos, Date arg0) throws SQLException;

	/**
	 * Write record.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws Error
	 *             the error
	 */
	abstract protected void writeRecord() throws IOException, Error;
}
