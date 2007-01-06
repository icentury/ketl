package com.kni.etl.dbutils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Date;

import org.w3c.dom.Element;

import com.kni.etl.ketl.exceptions.KETLWriteException;

abstract public class BulkLoaderStatementWrapper extends StatementWrapper {

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

	final protected String getStandardErrorMessage() {
		return this.mErrBuffer.toString();
	}

	final protected String getStandardOutMessage() {
		return this.mInBuffer.toString();
	}

	private boolean mFileOpen = false;

	final protected boolean filesOpen() {
		return mFileOpen;
	}

	class LoaderProcess extends Thread {

		private String command;

		private SQLException exception = null;

		private int finalStatus = -999;

		private Thread parent;

		public LoaderProcess(String command, Thread parent) {
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
				this.exception = handleLoaderStatus(this.finalStatus, this);

			} catch (Exception e) {
				ResourcePool.LogException(new KETLWriteException("STDERROR:"
						+ getStandardErrorMessage() + "\nSTDOUT:"
						+ getStandardOutMessage()), parent);
				ResourcePool.LogException(e, parent);
				parent.interrupt();
			}

		}

	}

	private LoaderProcess mActiveLoaderThread = null;

	private BufferedOutputStream mBuffer;

	private StringBuffer mErrBuffer;

	private StringBuffer mInBuffer;

	private Process mPipeToProcess;

	private String msDataFile;

	private FileOutputStream mTarget = null;

	protected DataOutputStream mWriter = null;

	private boolean pipeSupported = false;

	private int rowsInThisBatch = 0;

	public BulkLoaderStatementWrapper(boolean pipe) throws IOException,
			SQLException {
		super();
		this.pipeSupported = pipe;

		if (pipeSupported)
			msDataFile = createPipe();
		else
			msDataFile = File.createTempFile("ketl", ".dat").getAbsolutePath();

	}

	final public void addBatch() throws SQLException {
		try {

			if (this.mWriter == null)
				this.initFile();

			writeRecord();

		} catch (Exception e) {
			SQLException e1 = new SQLException(e.toString());
			e1.setStackTrace(e.getStackTrace());
			throw e1;
		}
		this.rowsInThisBatch++;
	}

	public void close() throws SQLException {
		if (pipeSupported)
			deleteDataFile();

		if (mPipeToProcess != null)
			mPipeToProcess.destroy();

	}

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
		mFileOpen = false;
	}

	final protected String getDataFile() {
		return this.msDataFile;
	}

	final private String createPipe() throws IOException {
		String pipeFilename;
		String tempdir = System.getProperty("java.io.tmpdir");

		if ((tempdir != null) && (tempdir.endsWith("/") == false)
				&& (tempdir.endsWith("\\") == false)) {
			tempdir = tempdir + File.separator;
		} else if (tempdir == null) {
			tempdir = "";
		}

		pipeFilename = tempdir + "nzPip" + Long.toString(System.nanoTime());
		String command = "mkfifo " + pipeFilename;

		this.executeCommand(command);
		return pipeFilename;
	}

	final protected void executeCommand(String command) throws IOException {
		Process proc = Runtime.getRuntime().exec(command);
		StringBuffer inBuffer = new StringBuffer();
		InputStream inStream = proc.getInputStream();
		new InputStreamHandler(inBuffer, inStream);

		StringBuffer errBuffer = new StringBuffer();
		InputStream errStream = proc.getErrorStream();
		new InputStreamHandler(errBuffer, errStream);

		try {
			if (proc.waitFor() != 0)
				throw new IOException("STDERROR:" + errBuffer.toString()
						+ "\nSTDOUT:" + inBuffer.toString());
		} catch (InterruptedException e) {
			throw new IOException("Command failed: " + e.getMessage());
		}
	}

	final private void deleteDataFile() {

		File fl = new File(msDataFile);

		if (fl.exists()) {
			fl.delete();
			fl = new File(msDataFile);
		}

	}

	private boolean mCommandExecuted = false;

	final public boolean loaderExecuted() {
		return this.mCommandExecuted;
	}

	final public int[] executeBatch() throws SQLException {
		int[] res = new int[this.rowsInThisBatch];
		// copy data from the given input stream to the table
		try {
			int iReturnValue;

			// get dataFile
			this.closeFile();

			mCommandExecuted = true;

			// execute data file
			if (!this.pipeSupported) {
				this.mPipeToProcess = Runtime.getRuntime().exec(
						this.getLoadStatement());
				this.mInBuffer = new StringBuffer();
				InputStream inStream = this.mPipeToProcess.getInputStream();
				new InputStreamHandler(this.mInBuffer, inStream);

				this.mErrBuffer = new StringBuffer();
				InputStream errStream = this.mPipeToProcess.getErrorStream();
				new InputStreamHandler(this.mErrBuffer, errStream);

				iReturnValue = this.mPipeToProcess.waitFor();
				this.mPipeToProcess = null;
				deleteDataFile();
			} else {
				while (this.mActiveLoaderThread.isAlive()) {
					Thread.sleep(100);
				}

				iReturnValue = this.mActiveLoaderThread.finalStatus;

				if (this.mActiveLoaderThread.exception != null)
					throw this.mActiveLoaderThread.exception;
			}

			SQLException e = this.handleLoaderStatus(iReturnValue, Thread
					.currentThread());

			if (e != null)
				throw e;

			java.util.Arrays.fill(res, 1);

			rowsInThisBatch = 0;
		} catch (Exception e) {
			if (e instanceof SQLException)
				throw (SQLException) e;

			throw new SQLException(e.toString());
		}
		return res;
	}

	@Override
	final public int executeUpdate() throws SQLException {
		throw new RuntimeException(
				"Single updates not supported yet by SQLLoader");
	}

	abstract protected String getLoadStatement();

	abstract protected SQLException handleLoaderStatus(int finalStatus,
			Thread thread) throws InterruptedException;

	final protected void initFile() throws IOException {

		if (pipeSupported) {
			// instantiate process
			this.mActiveLoaderThread = new LoaderProcess(this
					.getLoadStatement(), Thread.currentThread());
			this.mActiveLoaderThread.start();
		}

		this.mTarget = new FileOutputStream(msDataFile);
		this.mBuffer = new BufferedOutputStream(mTarget);
		this.mWriter = new DataOutputStream(this.mBuffer);
		mFileOpen = true;
	}

	abstract public void setBoolean(int pos, boolean arg0) throws SQLException;

	abstract public void setByteArrayValue(int pos, byte[] arg0)
			throws SQLException;

	abstract public void setDouble(int pos, Double arg0) throws SQLException;

	abstract public void setFloat(int pos, Float arg0) throws SQLException;

	abstract public void setInt(int pos, Integer arg0) throws SQLException;

	abstract public void setLong(int pos, Long arg0) throws SQLException;

	abstract public void setNull(int pos, int dataType) throws SQLException;

	abstract public void setObject(int pos, byte[] arg0) throws SQLException;

	@Override
	final public void setParameterFromClass(int parameterIndex, Class pClass,
			Object pDataItem, int maxCharLength, Element pXMLConfig)
			throws SQLException {

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
				this.setTimestamp(parameterIndex, new java.sql.Timestamp(
						((java.util.Date) pDataItem).getTime()));
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
				this.setSQLTimestamp(parameterIndex,
						(java.sql.Timestamp) pDataItem);
		} else if (pClass == Boolean.class || pClass == boolean.class) {
			if (pDataItem == null)
				this.setNull(parameterIndex, java.sql.Types.BOOLEAN);
			else
				this.setBoolean(parameterIndex, (Boolean) pDataItem);
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
			throw new RuntimeException("Datatype" + pClass.getCanonicalName()
					+ " is not supported by SQL*Loader");

	}

	abstract public void setShort(int pos, Short arg0) throws SQLException;

	abstract public void setSQLDate(int pos, java.sql.Date arg0)
			throws SQLException;

	abstract public void setSQLTime(int pos, java.sql.Time arg0)
			throws SQLException;

	abstract public void setSQLTimestamp(int pos, java.sql.Timestamp arg0)
			throws SQLException;

	abstract public void setString(int pos, String arg0) throws SQLException;

	abstract public void setTimestamp(int pos, Date arg0) throws SQLException;

	abstract protected void writeRecord() throws IOException, Error;
}
