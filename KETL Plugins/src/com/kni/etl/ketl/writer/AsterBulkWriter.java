/*
 *  Copyright (C) May 11, 2009 Kinetic Networks, Inc. All Rights Reserved.
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
package com.kni.etl.ketl.writer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.dbutils.ColumnExtDetail;
import com.kni.etl.ketl.dbutils.asterdata.AsterCopyWriter;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.reader.FileToRead;
import com.kni.etl.ketl.reader.JDBCReader;
import com.kni.etl.ketl.reader.NIOFileReader;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;
import com.kni.util.FileTools;

public class AsterBulkWriter extends ETLWriter implements DefaultWriterCore, WriterBatchManager, DBConnection, PrePostSQL {

	public class AsterBulkETLInPort extends ETLInPort {

		private static final String PARTITIONKEY_ATTRIB = "ASTERKEY";

		public ColumnExtDetail colDetail;

		private DatabaseColumnDefinition columnDefinition;

		public boolean skip = false;

		private Integer partitionKey = -1;

		private boolean skewFix = false;

		/**
		 * Instantiates a new PG bulk ETL in port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public AsterBulkETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
			int res = super.initialize(xmlConfig);

			if (res != 0)
				return res;

			if (XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), PARTITION_NAME, false)) {
				partitionPort = this;
				this.skip = true;
			}

			if (XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), SOURCE_NAME, false)) {
				sourceNamePort = this;
				this.skip = true;
			}

			if (mSkewFixColumn != null && this.getPortName().equalsIgnoreCase(mSkewFixColumn)) {
				this.skewFix = true;
			}

			this.partitionKey = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), PARTITIONKEY_ATTRIB, -1);

			this.skip = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), NIOFileWriter.SKIP, this.skip);

			// It's ok if not specified
			if (skip == false) {

				// Create a new column definition with the default properties...
				columnDefinition = new DatabaseColumnDefinition(xmlConfig, "", 0);
				columnDefinition.setProperty(DatabaseColumnDefinition.INSERT_COLUMN); // INSERT
				// by
				// default

				// Get the column's target name...
				columnDefinition.setColumnName(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), ETLStep.NAME_ATTRIB, null));

				// It's ok if not specified
				AsterBulkWriter.this.mvColumns.add(columnDefinition);
			}

			return 0;
		}

	}

	private enum Task {
		EXECPRESQL
	}

	private class TaskRunner extends Thread {
		SQLException exception;

		Task task;

		public TaskRunner(Task task) {
			super();
			this.task = task;
		}

		@Override
		public void run() {
			this.exception = null;
			switch (task) {
			case EXECPRESQL:
				this.setName("Presql runner for " + AsterBulkWriter.this.getName());
				try {
					executePreStatements();
				} catch (SQLException e) {
					this.exception = e;
				}
				return;
			}

		}

	};

	private static final String BUFFERSIZE_ATTRIB = "BUFFERSIZE";

	/** The Constant COMMITSIZE_ATTRIB. */
	public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";

	private static final String COMPRESS_ATTRIB = "COMPRESS";

	private static final String FILEDUMP_ATTRIB = "FILEONLY";

	private static final String LOADERURL_ATTRIB = "LOADERURL";

	private static final String GROUPWAIT_ATTRIB = "GROUPWAIT";

	private static final String PARTITION_NAME = "PARTITION";

	private static final String REPLACEINVALID_ATTRIB = "REPLACEINVALID";

	private static final String SCRIPT = "SCRIPT";

	private static final String SOURCE_NAME = "SOURCENAME";

	private static final String STREAM_ATTRIB = "STREAMING";

	/** The Constant TABLE_ATTRIB. */
	public static final String TABLE_ATTRIB = "TABLE";

	private static final String TARGETINFILENAME_ATTRIB = "TARGETINFILENAME";

	private static final String USEFILE_ATTRIB = "USEFILE";

	private static final String FILEPATHFORMAT_ATTRIB = "FILEPATHFORMAT";

	private static final String SEARCHPATHFORMAT_ATTRIB = "SEARCHPATHFORMAT";

	private static final String AUTOPARTITION_ATTRIB = "AUTOPARTITION";

	private static final String FLUSHMB_ATTRIB = "FLUSHMB";

	private String[] cols;

	private HashMap<String, ColumnExtDetail> columnMap;

	private boolean compress = false;

	private int copyBufferSize;

	private List<FileToRead> copyFiles;

	private boolean createScript = false;

	private String execScript = null;

	private boolean fileDump = false;

	private String loaderURL;

	// column

	// list and later converting
	// it into the array

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.writer.SubComponentBatchRunnerThread#write(java.lang
	 * .Object)
	 */

	/** The madcd columns. */
	private DatabaseColumnDefinition[] madcdColumns = null;

	/** The mc DB connection. */
	private Connection mcDBConnection = null;

	private Properties mDatabaseProperties;

	/** The mstr table name. */
	private String mstrTableName = null;

	/** The mv columns. */
	private final List<DatabaseColumnDefinition> mvColumns = new ArrayList<DatabaseColumnDefinition>(); // for
	// building
	// the

	/** The writer list. */
	private final List<AsterCopyWriter> mWriterList = new ArrayList<AsterCopyWriter>();

	private final Map<String, AsterCopyWriter> mWriterMap = new HashMap<String, AsterCopyWriter>();

	private boolean offLineConfig = false;

	private AsterBulkETLInPort partitionPort;

	private TaskRunner preSQLRunner;

	/** The records in batch. */
	private int recordsInBatch = 0;

	private String replace0;

	private AsterBulkETLInPort sourceNamePort;

	/** The str driver class. */
	private String strDriverClass = null;

	private long skewFixValue;

	private boolean streaming = false;

	/** The str password. */
	private String strPassword = null;

	/** The str pre SQL. */
	private String strPreSQL = null;

	/** The str URL. */
	private String strURL = null;

	// String strDataStoreName = null;
	/** The str user name. */
	private String strUserName = null;

	private String targetEncoding;

	private String targetFilePath;

	private boolean targetInFilename = false;

	private boolean useFile = false;

	private String searchPathFormat;

	private boolean mAutoPartition = false;

	private String mSkewFixColumn;

	private int mSkewFixThreshold;

	private int miGroupWaitTime;

	private int flushMB;

	private boolean useDataBlocks = true;

	/**
	 * Instantiates a new PG bulk writer.
	 * 
	 * @param pXMLConfig
	 *            the XML config
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public AsterBulkWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

		String skewColumn = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), "SKEWFIX", null);

		if (skewColumn != null) {
			String[] parts = skewColumn.split(",");

			try {
				this.mSkewFixColumn = parts[0];
				this.mSkewFixThreshold = Integer.parseInt(parts[1]);
				this.skewFixValue = this.mSkewFixThreshold;
			} catch (Exception e) {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "SKEWFIX format issues, should be COLUMN,MAXVALUE e.g. ACCOUNT_ID,0");
				throw new KETLThreadException(e, this);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {

		closeWriters();

		if (this.mcDBConnection != null)
			ResourcePool.releaseConnection(this.mcDBConnection);
	}

	private void closeWriters() {
		this.setWaiting(null);

		for (Entry<String, AsterCopyWriter> wr : this.mWriterMap.entrySet()) {
			try {
				Connection con;
				if ((con = wr.getValue().getConnection()) != null)
					ResourcePool.releaseConnection(con);
				wr.getValue().close();
			} catch (Exception e) {
				ResourcePool.LogException(e, this);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.writer.SubComponentParallelBatchWriter#complete()
	 */
	@Override
	public int complete() throws KETLThreadException {
		int res = super.complete();
		if (res != 0)
			return res;
		try {
			waitForPreSQL(this.preSQLRunner);
			if (this.useFile) {
				for (FileToRead f : this.copyFiles) {
					this.mcDBConnection = refreshConnection(this.strURL, this.mcDBConnection);
					File file = new File(f.getFilePath());
					String target = this.mstrTableName;
					if (this.targetInFilename) {
						target = file.getName().split("\\.")[0];
					}
					AsterCopyWriter wr = new AsterCopyWriter(this.mcDBConnection, EngineConstants.PARTITION_PATH + File.separator + this.getPartitionID(), this.compress,
							this.useDataBlocks);
					wr.createLoadCommand(target, this.cols, this.mAutoPartition);
					if (this.streaming == false)
						this.setWaiting("database to process copy file " + f.getFilePath());
					Connection con = null;
					try {
						con = ResourcePool.getConnection(this.strDriverClass, this.loaderURL, this.strUserName, this.strPassword, this.strPreSQL, true, this
								.getDatabaseProperties());
						wr.executeBatch(con, file);
						if (file.renameTo(new File(f.getFilePath() + ".done"))) {
							ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Renamed copy file to " + f.getFilePath() + ".done");
						} else {
							ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Failed to Rename copy file to " + f.getFilePath() + ".done");
						}
					} finally {
						if (con != null) {
							ResourcePool.releaseConnection(con);
						}
						con = null;
					}
				}
			} else {
				if (this.recordsInBatch > 0) {
					this.mcDBConnection = refreshConnection(this.strURL, this.mcDBConnection);
					this.executePreBatchStatements();
					sendBatchToDatabase();
					this.setWaiting(null);
					this.recordsInBatch = 0;
					this.executePostBatchStatements();
				}
			}

			this.executePostStatements();
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		} finally {
			closeWriters();
			if (!this.offLineConfig)
				ResourcePool.releaseConnection(this.getConnection());
			this.mcDBConnection = null;

		}

		return res;
	}

	private void createLoadScript(String target, AsterCopyWriter writer) throws IOException {
		String script = this.execScript.replace("{FILENAME}", writer.getSpoolFile());
		script = script.replace("{TABLENAME}", target);
		script = script.replace("{COLUMNS}", java.util.Arrays.toString(this.cols).replace("[", "").replace("]", "").replace(" ", ""));
		FileWriter fw = new FileWriter(writer.getSpoolFile() + ".sh");
		fw.append(script);
		fw.flush();
		fw.close();
		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Created load script " + writer.getSpoolFile() + ".sh");
	}

	private AsterCopyWriter createNewWriterMap(String target, String source) throws SQLException {
		AsterCopyWriter wr;

		if (this.streaming) {
			try {
				wr = new AsterCopyWriter(ResourcePool.getConnection(this.strDriverClass, this.loaderURL, this.strUserName, this.strPassword, this.strPreSQL, true, this
						.getDatabaseProperties()), true, this.useDataBlocks);
				wr.setFlushMB(this.flushMB);
			} catch (ClassNotFoundException e) {
				throw new SQLException(e);
			}
		} else {
			wr = new AsterCopyWriter(this.mcDBConnection, this.targetFilePath == null ? EngineConstants.PARTITION_PATH + File.separator + this.getPartitionID()
					: this.targetFilePath, this.compress, this.useDataBlocks);
		}

		if (this.offLineConfig) {
			wr.setEncoding(this.targetEncoding);
		} else {
			this.targetEncoding = wr.getEncoding();
		}
		wr.setPartitionName(this.partitions > 1 ? this.partitionID : null);
		wr.setSourceName(source);
		wr.createLoadCommand(target, this.cols, this.mAutoPartition);
		wr.spoolOnly(this.fileDump);
		wr.setReplaceInvalid(this.replace0);
		if (this.copyBufferSize > 8194)
			wr.setCopyBufferSize(this.copyBufferSize);
		for (ETLInPort port : this.mInPorts) {
			AsterBulkETLInPort currentPort = (AsterBulkETLInPort) port;
			if (currentPort.skip == false)
				wr.setColumnSizes(currentPort.colDetail.position, currentPort.colDetail.size, currentPort.colDetail.scale, currentPort.colDetail.precision);
		}

		this.mWriterMap.put(getTargetSourceKey(target, source), wr);
		this.mWriterList.add(wr);
		return wr;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePostBatchStatements()
	 */
	public void executePostBatchStatements() throws SQLException {
		if (this.offLineConfig)
			return;
		StatementManager.executeStatements(this, this, "POSTBATCHSQL");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePostStatements()
	 */
	public void executePostStatements() throws SQLException {
		if (this.offLineConfig)
			return;
		StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
	 */
	public void executePreBatchStatements() throws SQLException {
		if (this.offLineConfig)
			return;
		StatementManager.executeStatements(this, this, "PREBATCHSQL");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
	 */
	public void executePreStatements() throws SQLException {
		if (this.offLineConfig)
			return;
		StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.WriterBatchManager#finishBatch(int)
	 */
	public int finishBatch(int len) throws KETLWriteException {
		int result = 0;
		if (this.recordsInBatch >= this.batchSize || (this.recordsInBatch > 0 && len == BatchManager.LASTBATCH)) {
			try {
				waitForPreSQL(this.preSQLRunner);
				this.mcDBConnection = refreshConnection(this.strURL, this.mcDBConnection);
				this.executePreBatchStatements();
				sendBatchToDatabase();
				this.setWaiting(null);
				this.executePostBatchStatements();
				result += this.recordsInBatch;
			} catch (Exception e) {
				throw new KETLWriteException(e);
			}
			this.recordsInBatch = 0;
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.DBConnection#getConnection()
	 */
	public Connection getConnection() {
		return this.mcDBConnection;
	}

	private Properties getDatabaseProperties() {
		return this.mDatabaseProperties;
	}

	private boolean getFiles() throws Exception {

		List<FileToRead> files = new ArrayList();

		if (this.searchPathFormat != null) {
			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Searching for files in " + this.searchPathFormat);
			String[] fileNames = FileTools.getFilenames(this.searchPathFormat);

			if (fileNames != null) {
				for (String element : fileNames) {
					files.add(new FileToRead(element, 0));
				}

			}
		} else {
			for (int i = 0; i < this.maParameters.size(); i++) {
				String str = this.getParameterValue(i, NIOFileReader.SEARCHPATH);
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Searching for files in " + str);
				String[] fileNames = FileTools.getFilenames(str);

				if (fileNames != null) {
					for (String element : fileNames) {
						files.add(new FileToRead(element, i));
					}

				}
			}
		}

		if (files.size() == 0)
			return false;

		copyFiles = new ArrayList<FileToRead>();

		for (int i = 0; i < files.size(); i++) {
			if (i % this.partitions == this.partitionID)
				copyFiles.add(files.get(i));
		}

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLInPort getNewInPort(ETLStep srcStep) {
		return new AsterBulkETLInPort(this, srcStep);
	}

	class PortComparator implements Comparator<String> {

		public int compare(String o1, String o2) {
			AsterBulkETLInPort o1Port = ((AsterBulkETLInPort) getInPort(o1));
			AsterBulkETLInPort o2Port = ((AsterBulkETLInPort) getInPort(o2));

			if (o1Port.partitionKey != -1 && o2Port.partitionKey != -1)
				return o1Port.partitionKey.compareTo(o2Port.partitionKey);
			if (o1Port.partitionKey == -1)
				return o1Port.partitionKey.compareTo(Integer.MAX_VALUE);
			if (o2Port.partitionKey == -1)
				return o2Port.partitionKey.compareTo(Integer.MAX_VALUE);
			return o1.compareTo(o2);

		}

	}

	/**
	 * DOCUMENT ME!.
	 * 
	 * @param nConfig
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	@Override
	public int initialize(Node nConfig) throws KETLThreadException {
		int res = super.initialize(nConfig);

		if (res != 0) {
			return res;
		}

		// Get the attributes
		NamedNodeMap nmAttrs = nConfig.getAttributes();

		// Pull the parameters from the list...
		this.strUserName = this.getParameterValue(0, DBConnection.USER_ATTRIB);
		this.strPassword = this.getParameterValue(0, DBConnection.PASSWORD_ATTRIB);
		this.strURL = this.getParameterValue(0, DBConnection.URL_ATTRIB);
		this.loaderURL = this.getParameterValue(0, LOADERURL_ATTRIB);
		if (this.loaderURL == null)
			this.loaderURL = this.strURL;
		else {
			String[] urls = this.loaderURL.split(";");
			this.loaderURL = urls[this.partitions % urls.length];
		}

		this.strDriverClass = this.getParameterValue(0, DBConnection.DRIVER_ATTRIB);
		this.strPreSQL = this.getParameterValue(0, DBConnection.PRESQL_ATTRIB);
		try {
			this.setDatabaseProperties(this.getParameterListValues(0));
		} catch (Exception e1) {
			throw new KETLThreadException(e1, this);
		}

		// Convert the vector we've been building into a more common array...
		this.madcdColumns = this.mvColumns.toArray(new DatabaseColumnDefinition[0]);

		if (res != 0)
			return res;
		// Pull the name of the table to be written to...
		this.mstrTableName = XMLHelper.getAttributeAsString(nmAttrs, AsterBulkWriter.TABLE_ATTRIB, null);
		this.miGroupWaitTime = XMLHelper.getAttributeAsInt(nmAttrs, AsterBulkWriter.GROUPWAIT_ATTRIB, -1);
		this.useDataBlocks = XMLHelper.getAttributeAsBoolean(nmAttrs, "_USEDATABLOCKS", false);

		// Pull the commit size...
		this.batchSize = XMLHelper.getAttributeAsInt(nmAttrs, AsterBulkWriter.COMMITSIZE_ATTRIB, this.batchSize);
		this.flushMB = XMLHelper.getAttributeAsInt(nmAttrs, AsterBulkWriter.FLUSHMB_ATTRIB, 5);
		this.mAutoPartition = XMLHelper.getAttributeAsBoolean(nmAttrs, AUTOPARTITION_ATTRIB, false);
		this.replace0 = XMLHelper.getAttributeAsString(nmAttrs, REPLACEINVALID_ATTRIB, null);
		this.compress = XMLHelper.getAttributeAsBoolean(nmAttrs, AsterBulkWriter.COMPRESS_ATTRIB, this.compress);
		this.copyBufferSize = XMLHelper.getAttributeAsInt(nmAttrs, BUFFERSIZE_ATTRIB, -1);
		this.useFile = XMLHelper.getAttributeAsBoolean(nmAttrs, AsterBulkWriter.USEFILE_ATTRIB, this.useFile);
		this.fileDump = XMLHelper.getAttributeAsBoolean(nmAttrs, AsterBulkWriter.FILEDUMP_ATTRIB, this.fileDump);
		if (this.fileDump == false) {
			this.streaming = XMLHelper.getAttributeAsBoolean(nmAttrs, AsterBulkWriter.STREAM_ATTRIB, this.streaming);
			// / if (this.streaming)
			// ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
			// "Streaming has been disabled due to driver issues, fix is being worked on");
		}
		// this.streaming = false;

		this.createScript = XMLHelper.getAttributeAsBoolean(nmAttrs, AsterBulkWriter.SCRIPT, this.createScript);
		if (this.createScript)
			this.execScript = this.getParameterValue(0, SCRIPT);
		this.targetFilePath = this.getParameterValue(0, NIOFileWriter.FILEPATH);
		this.targetFilePath = XMLHelper.getAttributeAsString(nmAttrs, AsterBulkWriter.FILEPATHFORMAT_ATTRIB, this.targetFilePath);

		if (this.targetFilePath != null) {
			File file = new File(this.targetFilePath);
			if (file.exists() == false && file.mkdir()) {
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Created target directory " + file.getAbsolutePath());
			}
		}

		this.searchPathFormat = XMLHelper.getAttributeAsString(nmAttrs, AsterBulkWriter.SEARCHPATHFORMAT_ATTRIB, null);

		this.targetInFilename = XMLHelper.getAttributeAsBoolean(nmAttrs, AsterBulkWriter.TARGETINFILENAME_ATTRIB, this.targetInFilename);
		// connect to aster and if it fails see if offline config is available
		try {
			this.mcDBConnection = ResourcePool.getConnection(this.strDriverClass, this.strURL, this.strUserName, this.strPassword, this.strPreSQL, true, this
					.getDatabaseProperties());
		} catch (Exception e) {
			loadOfflineConfigWrapper(e);
		}

		if (this.useFile) {
			useFileAsSource();
		}
		if (this.batchSize <= 0) {
			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "All records will be inserted in a single batch");
			this.batchSize = Integer.MAX_VALUE;
		}

		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Commit and Batch size: " + this.batchSize);

		cols = new String[this.madcdColumns.length];
		for (int i = 0; i < this.madcdColumns.length; i++) {
			cols[i] = this.madcdColumns[i].getColumnName(null, -1);
		}
		for (String string : cols) {
			// ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
			// "DEBUG:BEFORE:PORT: " + string);
		}
		// sort the columns
		java.util.Arrays.sort(cols, new PortComparator());
		for (String string : cols) {
			// ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
			// "DEBUG:AFTERSORT:PORT: " + string);
		}

		String template = null;

		if (this.offLineConfig) {
			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Trying to use offline config");
			useOfflineConfig();
		} else {
			try {
				columnMap = new HashMap<String, ColumnExtDetail>();
				String mDBType = EngineConstants.cleanseDatabaseName(this.mcDBConnection.getMetaData().getDatabaseProductName());

				template = this.getStepTemplate(mDBType, "SELECTCOLUMNDATATYPE", true);
				template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrTableName);
				template = EngineConstants.replaceParameterV2(template, "COLUMNS", java.util.Arrays.toString(cols).replace("[", "").replace("]", ""));

				Statement mStmt = mcDBConnection.createStatement();
				ResultSet rs = mStmt.executeQuery(template);

				// Log executing sql to feed result record object with single
				// object reference
				ResultSetMetaData rm = rs.getMetaData();

				String hdl = XMLHelper.getAttributeAsString(nmAttrs, "HANDLER", null);

				JDBCItemHelper jdbcHelper;
				if (hdl == null)
					jdbcHelper = new JDBCItemHelper();
				else {
					try {
						Class cl = Class.forName(hdl);
						jdbcHelper = (JDBCItemHelper) cl.newInstance();
					} catch (Exception e) {
						throw new KETLThreadException("HANDLER class not found", e, this);
					}
				}

				for (int i = 1; i <= cols.length; i++) {
					AsterBulkETLInPort currentPort = ((AsterBulkETLInPort) this.getInPort(cols[i - 1]));

					ColumnExtDetail colDetail = new ColumnExtDetail();
					colDetail.size = JDBCReader.getColumnDisplaySize(rm, i);
					colDetail.precision = JDBCReader.getPrecision(rm, i);
					colDetail.scale = JDBCReader.getScale(rm, i);
					colDetail.targetClass = Class.forName(jdbcHelper.getJavaType(rm.getColumnType(i), colDetail.size, colDetail.precision, colDetail.scale));
					currentPort.colDetail = colDetail;
					currentPort.colDetail.position = i;
					columnMap.put(currentPort.getPortName(), colDetail);
				}

				rs.close();

				mStmt.close();

			} catch (Exception e1) {
				try {
					loadOfflineConfigWrapper(e1);
					useOfflineConfig();
				} catch (Exception e2) {
					throw new KETLThreadException("Problem executing fetch data type SQL \"" + template + "\"- " + e1.getMessage(), e1, this);
				}
			}
		}

		try {
			this.createNewWriterMap(mstrTableName, null);
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

		preSQLRunner = new TaskRunner(Task.EXECPRESQL);

		if (this.offLineConfig == false) {
			try {
				if (this.streaming)
					preSQLRunner.run();
				else
					preSQLRunner.start();
				saveOffLineConfig(this.mstrTableName);
			} catch (IOException e) {
				throw new KETLThreadException(e, this);
			}
		}

		return 0;
	}

	private void loadOfflineConfigWrapper(Exception e) throws KETLThreadException {
		if (this.fileDump && (this.offLineConfig = loadOfflineConfig(this.mstrTableName))) {
			ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Connection could not be made, loading config from file system");
		} else
			throw new KETLThreadException(e, this);
	}

	private void useOfflineConfig() throws KETLThreadException {
		for (int i = 1; i <= cols.length; i++) {
			AsterBulkETLInPort currentPort = ((AsterBulkETLInPort) this.getInPort(cols[i - 1]));
			ColumnExtDetail colDetail = columnMap.get(currentPort.getPortName());
			if (colDetail == null)
				throw new KETLThreadException("Offline config not available for port " + currentPort.getPortName(), this);
			currentPort.colDetail = colDetail;
			currentPort.colDetail.position = i;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.WriterBatchManager#initializeBatch(java.lang.Object
	 * [][], int)
	 */
	public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {

		if (this.streaming == false && this.recordsInBatch % 5000 == 0)
			this.setWaiting("copy file to spool, " + this.recordsInBatch + " rows loaded");

		return data;
	}

	private boolean loadOfflineConfig(String tableName) {
		try {
			FileInputStream fos = new FileInputStream(EngineConstants.PARTITION_PATH + File.separator + this.getPartitionID() + File.separator + tableName);
			BufferedInputStream outStream = new BufferedInputStream(fos);
			ObjectInputStream objStream = new ObjectInputStream(outStream);
			this.columnMap = (HashMap<String, ColumnExtDetail>) objStream.readObject();
			this.targetEncoding = (String) objStream.readObject();
			objStream.close();
			outStream.close();
			fos.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[],
	 * java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

		if (!this.useFile) {
			try {
				String targetTable = this.mstrTableName, sourceName = null;

				if (this.partitionPort != null) {
					Object tmp = pInputRecords[this.partitionPort.getSourcePortIndex()];
					if (tmp != null)
						targetTable = tmp.toString();
				}

				if (this.sourceNamePort != null) {
					Object tmp = pInputRecords[this.sourceNamePort.getSourcePortIndex()];
					if (tmp != null)
						sourceName = tmp.toString();
				}

				if (this.partitionPort != null) {
					String key = getTargetSourceKey(targetTable, sourceName);

					AsterCopyWriter wr = this.mWriterMap.get(key);

					if (wr == null) {
						wr = createNewWriterMap(targetTable, sourceName);
					}
					writeRecord(wr, pInputRecords, pExpectedDataTypes, pRecordWidth);
				} else {
					// all done write to streams
					for (AsterCopyWriter wr : this.mWriterList) {
						writeRecord(wr, pInputRecords, pExpectedDataTypes, pRecordWidth);

					}
				}

			} catch (Exception e) {
				throw new KETLWriteException(e);
			}
			this.recordsInBatch++;
		}
		return 1;
	}

	private String getTargetSourceKey(String targetTable, String sourceName) {
		String key = sourceName == null ? targetTable : targetTable + (char) 0 + sourceName;
		return key;
	}

	private Connection refreshConnection(String url, Connection connection) throws SQLException, ClassNotFoundException, InterruptedException {

		if (connection == null)
			return null;
		// poll db every 2 minutes;
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("select 1");
			rs.close();
			stmt.close();
			return connection;
		} catch (Exception e) {
			ResourcePool.releaseConnection(connection);
			int wait = 5000, attempts = 0;
			while (true) {
				try {
					return ResourcePool.getConnection(this.strDriverClass, url, this.strUserName, this.strPassword, this.strPreSQL, true, this.getDatabaseProperties());

				} catch (SQLException e1) {
					if (attempts++ > 30)
						throw e1;
					Thread.sleep(wait);
					wait += 15000;
				}
			}
		}
	}

	private void saveOffLineConfig(String tableName) throws IOException {

		String filePath = EngineConstants.PARTITION_PATH + File.separator + this.getPartitionID() + File.separator + tableName;
		File file = new File(filePath);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileOutputStream fos = new FileOutputStream(file);
		BufferedOutputStream outStream = new BufferedOutputStream(fos);
		ObjectOutputStream objStream = new ObjectOutputStream(outStream);
		objStream.writeObject(this.columnMap);
		objStream.writeObject(this.targetEncoding);
		objStream.flush();
		outStream.flush();
		objStream.close();
		outStream.close();
		fos.close();
	}

	private void sendBatchToDatabase() throws SQLException, ClassNotFoundException, KETLWriteException {

		if (this.miGroupWaitTime > 0) {
			this.setWaiting("for group, batch will fire " + this.miGroupWaitTime + "s after next minute");

			Calendar calendar = new GregorianCalendar();
			int currentMinute = calendar.get(Calendar.MINUTE);
			boolean wait = true;
			while (wait) {
				calendar = new GregorianCalendar();
				if (currentMinute != calendar.get(Calendar.MINUTE) && calendar.get(Calendar.SECOND) > this.miGroupWaitTime) {
					wait = false;
				} else {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						throw new KETLWriteException(e);
					}
				}
			}
			this.setWaiting(null);
		}

		for (Entry<String, AsterCopyWriter> wr : this.mWriterMap.entrySet()) {
			if (wr.getValue().getBatchSize() > 0) {
				if (streaming)
					this.setWaiting("database to replicate " + wr.getKey() + " batch, batchsize: " + wr.getValue().getBatchSize());
				else
					this.setWaiting("database to process " + wr.getKey() + " batch, batchsize: " + wr.getValue().getBatchSize());

				String target = wr.getKey();
				AsterCopyWriter writer = wr.getValue();

				try {
					if (this.fileDump) {
						ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Spool " + wr.getValue().getSpoolFile() + " created copy command " + wr.getValue().loadCommand());
						if (this.createScript) {
							createLoadScript(target, writer);
						}
					}
					Connection con = null;
					try {
						long startTime = System.currentTimeMillis();
						double recs = wr.getValue().getBatchSize();
						if (streaming || this.offLineConfig)
							wr.getValue().executeBatch();
						else {
							con = ResourcePool.getConnection(this.strDriverClass, this.loaderURL, this.strUserName, this.strPassword, this.strPreSQL, true, this
									.getDatabaseProperties());
							wr.getValue().executeBatch(con);
						}
						long endTime = System.currentTimeMillis() + 1;
						long res = Math.min((long) recs, (long) (recs / ((endTime - startTime) / 1000)));

						ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, (this.streaming ? "Replication" : "DB write") + " rate " + res + " rec/s");
					} finally {
						if (con != null)
							ResourcePool.releaseConnection(con);
						con = null;
					}
				} catch (IOException e) {
					try {
						if (this.execScript != null) {
							createLoadScript(target, writer);
							throw new KETLWriteException("Copy failed, a manual load script has been created, see log for details", e);
						} else
							throw new KETLWriteException(e);
					} catch (IOException e1) {
						throw new KETLWriteException(e1);
					}

				}
			}
		}

		this.setWaiting(null);
	}

	private void setDatabaseProperties(Map<String, Object> parameterListValues) throws Exception {
		this.mDatabaseProperties = JDBCItemHelper.getProperties(parameterListValues);
	}

	private void useFileAsSource() throws KETLThreadException {
		ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "File read method will be used, searching for files");
		try {
			if (this.getFiles() == false) {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "No files found");

				for (int i = 0; i < this.maParameters.size(); i++) {
					String searchPath = this.getParameterValue(i, NIOFileReader.SEARCHPATH);
					ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Search path(s): " + searchPath);
				}

				throw new KETLThreadException("No files found, check search paths", this);
			}

			if (this.targetInFilename) {
				Statement stmt = this.mcDBConnection.createStatement();
				try {
					for (FileToRead f : this.copyFiles) {
						File file = new File(f.getFilePath());
						String target = file.getName().split("\\.")[0];
						try {
							ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Checking for target table or partition " + target);
							ResultSet rs = stmt.executeQuery("select 1 from " + target + " where false");
							rs.close();
						} catch (Exception e) {
							throw new KETLThreadException("Table or partition " + target + " not found", this);
						}
					}
				} finally {
					stmt.close();
				}
			}
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}
	}

	private void waitForPreSQL(TaskRunner runner) throws SQLException, InterruptedException {
		List<ETLWorker> fellowWorkers = this.getThreadManager().getFellowWorkers(this.getName());

		for (ETLWorker tmp : fellowWorkers) {
			AsterBulkWriter worker = (AsterBulkWriter) tmp;
			if (worker.preSQLRunner != null) {
				while (worker.preSQLRunner.isAlive()) {
					this.setWaiting("presql on partition " + worker.getPartitionID() + ", spooling in the background");
					Thread.sleep(1000);
				}
				this.setWaiting(null);
				if (preSQLRunner.exception != null)
					throw preSQLRunner.exception;
			}
		}

	}

	/**
	 * Write.
	 * 
	 * @param pPreparedBatch
	 *            the prepared batch
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws SQLException
	 */

	private void writeRecord(AsterCopyWriter stmt, Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException, IOException, SQLException {

		// ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
		// "Load command:" +stmt.loadCommand());

		for (ETLInPort port : this.mInPorts) {

			AsterBulkETLInPort currentPort = (AsterBulkETLInPort) port;

			/*
			 * try { ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
			 * "PORT:POSITION" +currentPort.getPortName() +
			 * currentPort.colDetail.position ); } catch (KETLThreadException
			 * e1) { // TODO Auto-generated catch block e1.printStackTrace(); }
			 */

			if (currentPort.skip == false) {
				Class cl = pExpectedDataTypes[currentPort.getSourcePortIndex()];
				Class tClass = currentPort.colDetail.targetClass;
				int colPos = currentPort.colDetail.position;
				Object data = currentPort.isConstant() ? currentPort.getConstantValue() : pInputRecords[currentPort.getSourcePortIndex()];

				if (data != null && Number.class.isAssignableFrom(cl)) {
					if (tClass == Integer.class || tClass == Long.class) {
						data = ((Number) data).longValue();
						cl = Long.class;
					} else {
						data = ((Number) data).doubleValue();
						cl = Double.class;
					}

					if (currentPort.skewFix) {
						if (((Number) data).intValue() <= this.mSkewFixThreshold) {
							data = this.skewFixValue--;

							if (this.skewFixValue < (this.mSkewFixThreshold - 9999)) {
								this.skewFixValue = this.mSkewFixThreshold - 1;
							}
						}
					}
				}

				if (data == null)
					stmt.setNull(colPos, -1);
				else if (cl == String.class) {
					String str = (String) data;
					stmt.setString(colPos, str);
				} else if (cl == Integer.class || cl == int.class) {
					Integer data2 = (Integer) data;
					stmt.setInt(colPos, data2);
					/*
					 * try { ResourcePool.LogMessage(this,
					 * ResourcePool.INFO_MESSAGE, "ValueInt="
					 * +port.getPortName() +data2); } catch (KETLThreadException
					 * e) { // TODO Auto-generated catch block
					 * e.printStackTrace(); }
					 */

				} else if (cl == Double.class || cl == double.class) {
					Double data2 = (Double) data;
					stmt.setDouble(colPos, data2, 20);
					/*
					 * try { ResourcePool.LogMessage(this,
					 * ResourcePool.INFO_MESSAGE,
					 * "ValueDouble="+port.getPortName() +data2); } catch
					 * (KETLThreadException e) { // TODO Auto-generated catch
					 * block e.printStackTrace(); }
					 */
				} else if (cl == Long.class || cl == long.class) {
					Long data2 = (Long) data;
					stmt.setLong(colPos, data2);
					/*
					 * try { ResourcePool.LogMessage(this,
					 * ResourcePool.INFO_MESSAGE, "ValueLong="
					 * +port.getPortName()+data2); } catch (KETLThreadException
					 * e) { // TODO Auto-generated catch block
					 * e.printStackTrace(); }
					 */
				} else if (cl == Float.class || cl == Float.class) {
					Float data2 = (Float) data;
					stmt.setFloat(colPos, data2);
					/*
					 * try { ResourcePool.LogMessage(this,
					 * ResourcePool.INFO_MESSAGE, "ValueFloat="
					 * +port.getPortName()+data2); } catch (KETLThreadException
					 * e) { // TODO Auto-generated catch block
					 * e.printStackTrace(); }
					 */
				} else if (cl == java.util.Date.class || cl == java.sql.Timestamp.class || cl == java.sql.Time.class || cl == java.sql.Date.class)
					stmt.setTimestamp(colPos, (java.util.Date) data);
				else if (cl == Boolean.class || cl == boolean.class)
					stmt.setBoolean(colPos, (Boolean) data);
				else if (cl == byte[].class) {
					byte[] bytes = (byte[]) data;
					stmt.setByteArrayValue(colPos, bytes);
				} else
					throw new KETLWriteException("Unsupported class for bulk writer " + cl.getCanonicalName());
			}
		}

		stmt.addBatch();
	}

	@Override
	protected String getVersion() {
		return "$LastChangedRevision$";
	}

}
