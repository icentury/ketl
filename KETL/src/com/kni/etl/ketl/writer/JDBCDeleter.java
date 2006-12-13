/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
 * 
 * @author Brian Sullivan
 * @version 1.0
 */
public class JDBCDeleter extends ETLWriter implements DefaultWriterCore, DBConnection, WriterBatchManager, PrePostSQL {

    @Override
    protected void close(boolean success) {
        try {
            if (this.mStmt != null)
                this.mStmt.close();
        } catch (SQLException e) {
            ResourcePool.LogException(e, this);
        }
        if (this.mcDBConnection != null)
            ResourcePool.releaseConnection(this.mcDBConnection);

    }

    public JDBCDeleter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    public static final String TABLE_ATTRIB = "TABLE";
    String[] msRequiredTags = { USER_ATTRIB, PASSWORD_ATTRIB, URL_ATTRIB, DRIVER_ATTRIB };
    private Connection mcDBConnection;
    int miRowCount;
    int miDeleteEstimate;
    int miMaxInSize;
    int miBatchCount;
    int miCommitCount;
    int miCommitSize;

    private PreparedStatement mStmt = null;
    String mstrTableName = null;

    // Get the current connection object...
    public Connection getConnection() {
        return mcDBConnection;
    }

    int miInCount = 0;
    int[] mDataTypes;
    boolean rowsEffected = false;
    private int maxCharLength;

    public int finishBatch(int rows) throws KETLWriteException {
        int iDeletes = 0;

        if (miBatchCount > 0) {

            miBatchCount = 0;
            try {

                if (miInCount > 0) {
                    while (miInCount < this.miMaxInSize) {
                        for (int a = 0; a < this.mInPorts.length; a++) {
                            mStmt.setNull(miInCount + 1 + a, mDataTypes[a]);

                        }
                        miInCount++;
                    }
                    mStmt.addBatch();
                }

                int res[] = mStmt.executeBatch();
                for (int i = 0; i < res.length; i++) {
                    iDeletes += res[i];
                }

                this.executePostBatchStatements();
                this.executePreBatchStatements();
            } catch (SQLException e) {
                throw new KETLWriteException(e);
            }

            if (iDeletes == 0)
                iDeletes = this.miDeleteEstimate;

            rowsEffected = true;
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deletes in batch estimated to be " + iDeletes);

        }

        return iDeletes;
    }

    public int complete() throws KETLThreadException {

        // final batch
        try {
            if (rowsEffected) {
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
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, miDeleteEstimate + " records expected to be deleted");

        return 0;
    }

    private JDBCItemHelper jdbcHelper;

    // Return 0 if success, otherwise error code...
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
        mstrTableName = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), TABLE_ATTRIB, null);

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

        if (mstrTableName == null)
            throw new KETLThreadException("No table specified", this);

        this.miMaxInSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(), "MAXINSIZE", 256);

        // Pull the commit size...
        miCommitSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(), COMMITSIZE_ATTRIB, this.batchSize);

        // Pull the parameters from the list...
        // Pull the parameters from the list...
        strUserName = this.getParameterValue(0, USER_ATTRIB);
        strPassword = this.getParameterValue(0, PASSWORD_ATTRIB);
        strURL = this.getParameterValue(0, URL_ATTRIB);
        strDriverClass = this.getParameterValue(0, DRIVER_ATTRIB);
        strPreSQL = this.getParameterValue(0, PRESQL_ATTRIB);

        try {
            this.setConnection(ResourcePool.getConnection(strDriverClass, strURL, strUserName, strPassword, strPreSQL,
                    true));

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
    public void setConnection(Connection conn) {
        // Close any existing connection...
        if (mcDBConnection != null) {
            try {
                ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                        "Closing connection for unexpected reason, connection not returned to resource pool");
                ResourcePool.releaseConnection(mcDBConnection);
            } catch (Exception e) {
            } finally {
                mcDBConnection = null;
            }
        }

        // Point to the new connection...
        mcDBConnection = conn;
    }

    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {
        try {
            for (int a = 0; a < pRecordWidth; a++) {
                this.jdbcHelper.setParameterFromClass(mStmt, miInCount + 1 + a, pExpectedDataTypes[this.mInPorts[a]
                        .getSourcePortIndex()], this.mInPorts[a].isConstant() ? this.mInPorts[a].getConstantValue()
                        : pInputRecords[this.mInPorts[a].getSourcePortIndex()], this.maxCharLength, this.mInPorts[a]
                        .getXMLConfig());
            }

            miDeleteEstimate++;
            miInCount++;
            // protect against massive in statements
            if (miInCount >= this.miMaxInSize) {
                mStmt.addBatch();
                miInCount = 0;
            }
            miBatchCount++;
            return 0;
        } catch (SQLException e) {
            throw new KETLWriteException(e);
        }
    }

    public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {
        try {
            this.executePreBatchStatements();
        } catch (SQLException e) {
            throw new KETLWriteException(e);
        }
        return data;
    }

    public void executePostStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
    }

    public void executePreStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
    }

    public void executePostBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTBATCHSQL");
    }

    public void executePreBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PREBATCHSQL");
    }

}
