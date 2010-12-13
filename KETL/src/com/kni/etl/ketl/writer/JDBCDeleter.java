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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title: JDBCWriter
 * </p>
 * <p>
 * Description: Writes a DataItem array to a JDBC datasource, based on ETLWriter
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * .
 * 
 * @author Brian Sullivan
 * @version 1.0
 */
public class JDBCDeleter extends ETLWriter implements DefaultWriterCore, DBConnection, WriterBatchManager, PrePostSQL {

	@Override
	protected String getVersion() {
		return "$LastChangedRevision$";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
		try {
			if (this.mStmt != null)
				this.mStmt.close();
		} catch (SQLException e) {
			ResourcePool.LogException(e, this);
		}
		if (this.mcDBConnection != null)
			ResourcePool.releaseConnection(this.mcDBConnection);

	}

	/**
	 * Instantiates a new JDBC deleter.
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
	public JDBCDeleter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/** The Constant COMMITSIZE_ATTRIB. */
	public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";

	/** The Constant TABLE_ATTRIB. */
	public static final String TABLE_ATTRIB = "TABLE";

	/** The ms required tags. */
	String[] msRequiredTags = { DBConnection.USER_ATTRIB, DBConnection.PASSWORD_ATTRIB, DBConnection.URL_ATTRIB, DBConnection.DRIVER_ATTRIB };

	/** The mc DB connection. */
	private Connection mcDBConnection;

	/** The mi row count. */
	int miRowCount;

	/** The mi delete estimate. */
	int miDeleteEstimate;

	/** The mi max in size. */
	int miMaxInSize;

	/** The mi batch count. */
	int miBatchCount;

	/** The mi commit count. */
	int miCommitCount;

	/** The mi commit size. */
	int miCommitSize;

	/** The stmt. */
	private PreparedStatement mStmt = null;

	/** The mstr table name. */
	String mstrTableName = null;

	// Get the current connection object...
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.DBConnection#getConnection()
	 */
	public Connection getConnection() {
		return this.mcDBConnection;
	}

	/** The mi in count. */
	int miInCount = 0;

	/** The data types. */
	int[] mDataTypes;

	/** The rows effected. */
	boolean rowsEffected = false;

	/** The max char length. */
	private int maxCharLength;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.WriterBatchManager#finishBatch(int)
	 */
	public int finishBatch(int rows) throws KETLWriteException {
		int iDeletes = 0;

		if (this.miBatchCount > 0) {

			this.miBatchCount = 0;
			try {

				if (this.miInCount > 0) {
					while (this.miInCount < this.miMaxInSize) {
						for (int a = 0; a < this.mInPorts.length; a++) {
							this.mStmt.setNull(this.miInCount + 1 + a, this.mDataTypes[a]);

						}
						this.miInCount++;
					}
					this.mStmt.addBatch();
				}

				int res[] = this.mStmt.executeBatch();
				for (int element : res) {
					iDeletes += element;
				}

				this.executePostBatchStatements();
				this.executePreBatchStatements();
			} catch (SQLException e) {
				throw new KETLWriteException(e);
			}

			if (iDeletes == 0)
				iDeletes = this.miDeleteEstimate;

			this.rowsEffected = true;
			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deletes in batch estimated to be " + iDeletes);

		}

		return iDeletes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#complete()
	 */
	@Override
	public int complete() throws KETLThreadException {

		// final batch
		try {
			if (this.rowsEffected) {
				// Commit any last data in the transaction...
				this.mcDBConnection.commit();
			}
		} catch (SQLException e) {
			throw new KETLThreadException(e, this);
		} finally {
			if (this.mStmt != null)
				try {
					this.mStmt.close();
					this.mStmt = null;
				} catch (Exception e) {
					ResourcePool.LogException(e, this);
				}
			ResourcePool.releaseConnection(this.mcDBConnection);
			this.mcDBConnection = null;
		}

		this.mcDBConnection = null;

		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Job Results: ");
		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, this.miDeleteEstimate + " records expected to be deleted");

		return 0;
	}

	/** The jdbc helper. */
	private JDBCItemHelper jdbcHelper;

	private Properties mDatabaseProperties;

	private Properties getDatabaseProperties() {
		return this.mDatabaseProperties;
	}

	private void setDatabaseProperties(Map<String, Object> parameterListValues) throws Exception {
		this.mDatabaseProperties = JDBCItemHelper.getProperties(parameterListValues);
	}

	// Return 0 if success, otherwise error code...
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
	 */
	@Override
	public int initialize(Node xmlDestNode) throws KETLThreadException {
		// String strDataStoreName = null;
		String strUserName = null;
		String strPassword = null;
		String strURL = null;
		String strDriverClass = null;
		String strPreSQL = null;

		int res = super.initialize(xmlDestNode);

		if (res != 0)
			return res;

		// Pull the name of the table to be written to...
		this.mstrTableName = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), JDBCDeleter.TABLE_ATTRIB, null);

		String hdl = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), "HANDLER", null);

		if (hdl == null)
			this.jdbcHelper = new JDBCItemHelper();
		else {
			try {
				Class cl = Class.forName(hdl);
				this.jdbcHelper = (JDBCItemHelper) cl.newInstance();
			} catch (Exception e) {
				throw new KETLThreadException("HANDLER class not found", e, this);
			}
		}

		if (this.mstrTableName == null)
			throw new KETLThreadException("No table specified", this);

		this.miMaxInSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(), "MAXINSIZE", 256);

		// Pull the commit size...
		this.miCommitSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(), JDBCDeleter.COMMITSIZE_ATTRIB, this.batchSize);

		// Pull the parameters from the list...
		// Pull the parameters from the list...
		strUserName = this.getParameterValue(0, DBConnection.USER_ATTRIB);
		strPassword = this.getParameterValue(0, DBConnection.PASSWORD_ATTRIB);
		strURL = this.getParameterValue(0, DBConnection.URL_ATTRIB);
		strDriverClass = this.getParameterValue(0, DBConnection.DRIVER_ATTRIB);
		strPreSQL = this.getParameterValue(0, DBConnection.PRESQL_ATTRIB);
		try {
			this.setDatabaseProperties(this.getParameterListValues(0));
		} catch (Exception e1) {
			throw new KETLThreadException(e1, this);
		}

		try {
			this.setConnection(ResourcePool.getConnection(strDriverClass, strURL, strUserName, strPassword, strPreSQL, true, this.getDatabaseProperties()));

			this.maxCharLength = this.mcDBConnection.getMetaData().getMaxCharLiteralLength();
			this.executePreStatements();

			if (this.mStmt == null) {
				StringBuilder sb = new StringBuilder("delete from " + this.mstrTableName + " where ");

				for (int i = 0; i < this.miMaxInSize; i++) {
					if (i > 0) {
						sb.append(" OR ");
					}
					sb.append("(");
					for (int a = 0; a < this.mInPorts.length; a++) {
						if (a > 0)
							sb.append(" AND ");

						sb.append(this.mInPorts[a].mstrName);
						sb.append(" = ?");

					}
					sb.append(")");
				}
				this.mStmt = this.mcDBConnection.prepareStatement(sb.toString());

			}
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		} finally {
			if (this.mcDBConnection != null) {
				ResourcePool.releaseConnection(this.mcDBConnection);
			}
		}

		return 0;
	}

	// Set the connection object for this reader. Note that there can only be
	// one connection per reader, so this closes and releases any previous one.
	/**
	 * Sets the connection.
	 * 
	 * @param conn
	 *            the new connection
	 */
	public void setConnection(Connection conn) {
		// Close any existing connection...
		if (this.mcDBConnection != null) {
			try {
				ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Closing connection for unexpected reason, connection not returned to resource pool");
				ResourcePool.releaseConnection(this.mcDBConnection);
			} catch (Exception e) {
			} finally {
				this.mcDBConnection = null;
			}
		}

		// Point to the new connection...
		this.mcDBConnection = conn;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[],
	 * java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {
		try {
			for (int a = 0; a < pRecordWidth; a++) {
				this.jdbcHelper.setParameterFromClass(this.mStmt, this.miInCount + 1 + a, pExpectedDataTypes[this.mInPorts[a].getSourcePortIndex()],
						this.mInPorts[a].isConstant() ? this.mInPorts[a].getConstantValue() : pInputRecords[this.mInPorts[a].getSourcePortIndex()], this.maxCharLength,
						this.mInPorts[a].getXMLConfig());
			}

			this.miDeleteEstimate++;
			this.miInCount++;
			// protect against massive in statements
			if (this.miInCount >= this.miMaxInSize) {
				this.mStmt.addBatch();
				this.miInCount = 0;
			}
			this.miBatchCount++;
			return 0;
		} catch (SQLException e) {
			throw new KETLWriteException(e);
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
		try {
			this.executePreBatchStatements();
		} catch (SQLException e) {
			throw new KETLWriteException(e);
		}
		return data;
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
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
	 */
	public void executePreStatements() throws SQLException {
		StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
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
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
	 */
	public void executePreBatchStatements() throws SQLException {
		StatementManager.executeStatements(this, this, "PREBATCHSQL");
	}

}
