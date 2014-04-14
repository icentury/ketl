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
package com.kni.etl.ketl.writer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import com.kni.etl.ketl.dbutils.postgresql.redshift.RedshiftCopyFileWriter;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.reader.JDBCReader;
import com.kni.etl.ketl.reader.NIOFileReader;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title: PGBulkWriter
 * </p>
 * <p>
 * Description: Similar functionality to JDBC writer but the data is bulk loaded
 * using a customized JDBC driver for PostgreSQL.
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class RedshiftBulkWriter extends ETLWriter implements DefaultWriterCore, WriterBatchManager, DBConnection, PrePostSQL {

	@Override
	protected String getVersion() {
		return "$LastChangedRevision: 526 $";
	}

	/**
	 * The Class PGBulkETLInPort.
	 */
	public class RedshiftBulkETLInPort extends ETLInPort {

		private boolean skip;

		public Class<?> targetClass;

		/**
		 * Instantiates a new PG bulk ETL in port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public RedshiftBulkETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
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

			this.skip = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), NIOFileWriter.SKIP, false);

			// It's ok if not specified
			if (skip == false) {
				DatabaseColumnDefinition dcdNewColumn;

				// Create a new column definition with the default properties...
				dcdNewColumn = new DatabaseColumnDefinition(xmlConfig, "", 0);
				dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN); // INSERT
				// by
				// default

				// Get the column's target name...
				dcdNewColumn.setColumnName(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), ETLStep.NAME_ATTRIB, null));

				RedshiftBulkWriter.this.mvColumns.add(dcdNewColumn);
			}

			if (XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), NIOFileWriter.FILE_NAME, false)) {
				RedshiftBulkWriter.this.fileNameInPort = true;

				RedshiftBulkWriter.this.fileNameFormat = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), NIOFileWriter.FILENAME_FORMAT,
						"{FILENAME}.{PARTITION}{SUBPARTITION}");
				RedshiftBulkWriter.this.filePathFormat = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), NIOFileWriter.FILEPATH_FORMAT, targetFilePath);
				RedshiftBulkWriter.this.fileNamePort = this;

			}

			if (XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), NIOFileWriter.SUB_PARTITION, false)) {
				RedshiftBulkWriter.this.subPartitionPort = this;
			}

			return 0;
		}

	}

	/** The Constant COMMITSIZE_ATTRIB. */
	private static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
	private static final String AWSKEY_ATTRIB = "AWSKEY";
	private static final String AWSSECRET_ATTRIB = "AWSSECRET";
	private static final String BUCKETNAME_ATTRIB = "BUCKETNAME";

	/** The Constant TABLE_ATTRIB. */
	private static final String TABLE_ATTRIB = "TABLE";
	private static final String AWSPARENTDIR_ATTRIB = "AWSPARENTDIR";

	private String fileNameFormat;

	private boolean fileNameInPort = false;

	private RedshiftBulkETLInPort fileNamePort;

	/** The stmt. */

	private String filePathFormat;

	/** The madcd columns. */
	private DatabaseColumnDefinition[] madcdColumns = null;

	/** The mc DB connection. */
	private Connection mcDBConnection = null;

	private String mCharset;

	private Properties mDatabaseProperties;

	private int mIOBufferSize;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.writer.SubComponentBatchRunnerThread#write(java.lang
	 * .Object)
	 */

	/** The mstr table name. */
	private String mstrTableName = null;

	/** The mv columns. */
	private final List<DatabaseColumnDefinition> mvColumns = new ArrayList<DatabaseColumnDefinition>(); // for
	// building
	// the

	// column list and
	// later converting
	// it into the array

	/** The writer list. */
	private final List<RedshiftCopyFileWriter> mWriterList = new ArrayList<RedshiftCopyFileWriter>();

	private final Map<String, RedshiftCopyFileWriter> mWriterMap = new HashMap<String, RedshiftCopyFileWriter>();

	/** The writers. */
	private RedshiftCopyFileWriter[] mWriters;

	private boolean mZip = true;

	/** The records in batch. */
	private int recordsInBatch = 0;

	/** The str driver class. */
	private String strDriverClass = null;

	/** The str password. */
	private String strPassword = null;

	/** The str pre SQL. */
	private String strPreSQL = null;

	/** The str URL. */
	private String strURL = null;

	// String strDataStoreName = null;
	/** The str user name. */
	private String strUserName = null;

	public RedshiftBulkETLInPort subPartitionPort;

	private String targetFilePath;

	private Class[] colClasses;

	private String[] cols;
	private String accessKey;
	private String secretKey;
	private String bucketName;
	private String parentDir;

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
	public RedshiftBulkWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {

		for (RedshiftCopyFileWriter wr : this.mWriterList) {
			try {
				wr.close();
			} catch (IOException e) {
				ResourcePool.LogException(e, this);
			}
		}

		if (this.mcDBConnection != null)
			ResourcePool.releaseConnection(this.mcDBConnection);
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

			if (this.recordsInBatch > 0) {
				for (RedshiftCopyFileWriter wr : this.mWriterList) {
					try {
						wr.executeBatch();
					} catch (IOException e) {
						ResourcePool.LogException(e, this);
					}
				}
				this.recordsInBatch = 0;
				this.executePostBatchStatements();
			}

			this.executePostStatements();
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		} finally {
			
			for (RedshiftCopyFileWriter wr : this.mWriterList) {
				try {
					wr.close();
				} catch (Exception e) {
					ResourcePool.LogException(e, this);
				}
			}
			

			if (this.isLastThreadToEnterCompletePhase()) {
				// wait for all other threads to complete
				this.setWaiting("all other threads in group to complete");
				while (this.getThreadManager().countOfStepThreadsAlive(this) > 1) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						throw new KETLError(e);
					}
				}
				this.setWaiting("final statements to run");
				Statement stmt;
				StringBuilder sb = new StringBuilder("COPY ");

				try {
					stmt = this.mcDBConnection.createStatement();

					
					sb.append(this.mstrTableName);
					sb.append(" (");

					for (int i = 0; i < cols.length; i++) {
						if (i > 0) {
							sb.append(",");
						}
						sb.append(cols[i]);

					}

					sb.append(") from 's3://"
							+ this.bucketName
							+ File.separator
							+ this.parentDir
							+ File.separator
							+ "'  CREDENTIALS 'aws_access_key_id="
							+ this.accessKey
							+ ";aws_secret_access_key="
							+ this.secretKey
							+ "' GZIP DELIMITER '\\001' MAXERROR AS 10 TIMEFORMAT AS 'epochmillisecs'  ESCAPE TRUNCATECOLUMNS TRIMBLANKS;\n");

					stmt.execute(sb.toString());
					stmt.close();
				} catch (SQLException e) {
					throw new KETLThreadException("Copy command failed: " + sb.toString(), e);
				}
				
			}
			
			ResourcePool.releaseConnection(this.getConnection());
			this.mcDBConnection = null;

		}

		return res;
	}

	private RedshiftCopyFileWriter createNewWriterMap(String fileName, String subPartition) throws KETLWriteException, IOException {

		String path = "";
		if (this.targetFilePath != null)
			path = this.targetFilePath + File.separator;
		else
			path = "." + File.separator;

		if (this.filePathFormat != null) {
			path = this.filePathFormat;
			path = path.replace("{PARTITION}", (this.partitions > 1 ? Integer.toString(this.partitionID) : ""));
			path = path.replace("{SUBPARTITION}", subPartition == null ? "" : subPartition);
			File f = new File(path);
			if (f.exists() == false) {
				ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Creating " + path + " directory " + f.getAbsolutePath());
				f.mkdir();
			} else if (f.isDirectory() == false) {
				throw new KETLWriteException("File path is invalid " + f.getAbsolutePath());
			}
			path = path + File.separator;
		}
		String fn = this.fileNameFormat;
		// partition id has to be part of filename
		if (this.partitions > 1 && fn.contains("{PARTITION}") == false) {
			path = path + this.partitionID + File.separator;
		}

		fn = fn.replace("{PARTITION}", (this.partitions > 1 ? Integer.toString(this.partitionID) : ""));
		fn = fn.replace("{SUBPARTITION}", subPartition == null ? "" : subPartition);
		fn = fn.replace("{FILENAME}", fileName);

		return createOutputFile(this.cols);
	}

	private RedshiftCopyFileWriter createOutputFile(String[] cols) throws IOException {
		RedshiftCopyFileWriter writer = new RedshiftCopyFileWriter(this.cols,
				this.mcDBConnection, new File(EngineConstants.PARTITION_PATH
						+ File.separator + this.getPartitionID()),
				this.accessKey, this.secretKey, this.bucketName,
				this.parentDir, this.mCharset, this.mZip, this.mIOBufferSize);
		this.mWriterList.add(writer);
		return writer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePostBatchStatements()
	 */
	public void executePostBatchStatements() throws SQLException {
		StatementManager.executeStatements(this, this, "POSTBATCHSQL");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePostStatements()
	 */
	public void executePostStatements() throws SQLException {
		StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
	 */
	public void executePreBatchStatements() throws SQLException {
		StatementManager.executeStatements(this, this, "PREBATCHSQL");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
	 */
	public void executePreStatements() throws SQLException {
		StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.WriterBatchManager#finishBatch(int)
	 */
	public int finishBatch(int len) throws KETLWriteException {
		int result = 0;
		if (this.recordsInBatch == this.batchSize || (this.recordsInBatch > 0 && len == BatchManager.LASTBATCH)) {
			try {
				for (RedshiftCopyFileWriter wr : this.mWriterList) {
					try {
						wr.executeBatch();
						result += this.recordsInBatch;
					} catch (IOException e) {
						ResourcePool.LogException(e, this);
					}
				}
				this.executePostBatchStatements();
				this.executePreBatchStatements();
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLInPort getNewInPort(ETLStep srcStep) {
		return new RedshiftBulkETLInPort(this, srcStep);
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

		this.mIOBufferSize = XMLHelper.getAttributeAsInt(nmAttrs, "IOBUFFER", 16384);

		this.accessKey= this.getParameterValue(0, AWSKEY_ATTRIB);
		this.secretKey= this.getParameterValue(0, AWSSECRET_ATTRIB);
		this.parentDir= this.getParameterValue(0, AWSPARENTDIR_ATTRIB);
		this.bucketName= this.getParameterValue(0, BUCKETNAME_ATTRIB);
		// Pull the name of the table to be written to...
		this.mstrTableName = XMLHelper.getAttributeAsString(nmAttrs, RedshiftBulkWriter.TABLE_ATTRIB, null);
		this.parentDir = this.parentDir
				+ File.separator
				+ this.mstrTableName
				+ File.separator
				+ this.getJobExecutionID()
				+ "_"
				+ System.currentTimeMillis();
		this.mCharset = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.CHARACTERSET_ATTRIB, "UTF-8");

		
		// Pull the commit size...
		this.batchSize = XMLHelper.getAttributeAsInt(nmAttrs, RedshiftBulkWriter.COMMITSIZE_ATTRIB, this.batchSize);

		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Commit and Batch size: " + this.batchSize);

		cols = new String[this.madcdColumns.length];
		for (int i = 0; i < this.madcdColumns.length; i++) {
			cols[i] = this.madcdColumns[i].getColumnName(null, -1);
		}

		try {
			this.mcDBConnection = ResourcePool.getConnection(this.strDriverClass, this.strURL, this.strUserName, this.strPassword, this.strPreSQL, true, this
					.getDatabaseProperties());

			String template = null;
			try {
				this.setGroup(EngineConstants.cleanseDatabaseName(this.mcDBConnection.getMetaData().getDatabaseProductName()));

				template = this.getStepTemplate(this.getGroup(), "SELECTCOLUMNDATATYPE", true);
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

					((RedshiftBulkETLInPort) this.getInPort(cols[i - 1])).targetClass = Class.forName(jdbcHelper.getJavaType(rm.getColumnType(i), JDBCReader.getColumnDisplaySize(rm,
							i), JDBCReader.getPrecision(rm, i), JDBCReader.getScale(rm, i)));
				}

				rs.close();

				mStmt.close();

			} catch (Exception e1) {
				throw new KETLThreadException("Problem executing fetch data type SQL \"" + template + "\"- " + e1.getMessage(), e1, this);
			}

			this.executePreStatements();
			this.executePreBatchStatements();


		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.WriterBatchManager#initializeBatch(java.lang.Object
	 * [][], int)
	 */
	public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {

		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[],
	 * java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

		String fileName = "NA";
		Object subPartition = null;
		try {

			if (this.subPartitionPort != null) {
				subPartition = pInputRecords[this.subPartitionPort.getSourcePortIndex()];
			}

			if (this.fileNamePort != null) {
				Object tmp = pInputRecords[this.fileNamePort.getSourcePortIndex()];
				if (tmp != null)
					fileName = tmp.toString();

				String key = subPartition == null ? fileName : fileName + subPartition;
				RedshiftCopyFileWriter wr = this.mWriterMap.get(key);

				if (wr == null) {
					wr = createNewWriterMap(fileName, subPartition == null ? null : subPartition.toString());
					this.mWriterMap.put(key, wr);
				}
				writeRecord(wr, pInputRecords, pExpectedDataTypes, pRecordWidth);
			} else {
				
				if (this.mWriters == null) {
					this.createOutputFile(cols);

					this.mWriters = new RedshiftCopyFileWriter[this.mWriterList
							.size()];
					this.mWriterList.toArray(this.mWriters);

				}
				// all done write to streams
				for (RedshiftCopyFileWriter wr : this.mWriters) {
					writeRecord(wr, pInputRecords, pExpectedDataTypes, pRecordWidth);
				}
			}

			for (RedshiftCopyFileWriter wr : this.mWriterList) {
				wr.addBatch();
			}
		} catch (Exception e) {
			throw new KETLWriteException(e);
		}
		this.recordsInBatch++;
		return 1;
	}

	private void setDatabaseProperties(Map<String, Object> parameterListValues) throws Exception {
		this.mDatabaseProperties = JDBCItemHelper.getProperties(parameterListValues);
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
	 */
	protected void write(RedshiftCopyFileWriter pPreparedBatch) throws SQLException, IOException {
		RedshiftCopyFileWriter currentStatement = pPreparedBatch;
		currentStatement.executeBatch();
		currentStatement.commit();

	}

	private void writeRecord(RedshiftCopyFileWriter stmt, Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException, IOException {
		for (int i = 0; i < pRecordWidth; i++) {

			if (((RedshiftBulkETLInPort) this.mInPorts[i]).skip == false) {

				Class cl = pExpectedDataTypes[this.mInPorts[i].getSourcePortIndex()];
				Class tClass = ((RedshiftBulkETLInPort) this.mInPorts[i]).targetClass;

				Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue() : pInputRecords[this.mInPorts[i].getSourcePortIndex()];

				if (data != null && Number.class.isAssignableFrom(cl)) {
					if (tClass == Integer.class || tClass == Long.class) {
						data = ((Number) data).longValue();
						cl = Long.class;
					} else {
						data = ((Number) data).doubleValue();
						cl = Double.class;
					}
				}

				if (data == null)
					stmt.setNull(i + 1, -1);
				else if (cl == String.class)
					stmt.setString(i + 1, (String) data);
				else if (cl == Integer.class || cl == int.class)
					stmt.setInt(i + 1, (Integer) data);
				else if (cl == Double.class || cl == double.class)
					stmt.setDouble(i + 1, (Double) data, 20);
				else if (cl == Long.class || cl == long.class)
					stmt.setLong(i + 1, (Long) data);
				else if (cl == Float.class || cl == Float.class)
					stmt.setFloat(i + 1, (Float) data);
				else if (cl == java.util.Date.class || cl == java.sql.Timestamp.class || cl == java.sql.Time.class || cl == java.sql.Date.class)
					stmt.setTimestamp(i + 1, (java.util.Date) data);
				else if (cl == Boolean.class || cl == boolean.class)
					stmt.setBoolean(i + 1, (Boolean) data);
				else if (cl == byte[].class)
					stmt.setByteArrayValue(i + 1, (byte[]) data);
				else
					throw new KETLWriteException("Unsupported class for bulk writer " + cl.getCanonicalName());
			}
		}
	}

}
