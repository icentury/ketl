/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.sql.Connection;
import java.sql.SQLException;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderItemHelper;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderStatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

public class SQLLoaderELTWriter extends JDBCWriter {

    private static final String CONNECTIONSTRING_ATTRIB = "CONNECTIONSTRING";

    private boolean mPipeData = !System.getProperty("os.name").startsWith("Windows");

    public SQLLoaderELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    protected boolean pipeData() {
        return this.mPipeData;
    }

    @Override
    StatementWrapper prepareStatementWrapper(Connection Connection, String loadStatement, JDBCItemHelper jdbcHelper)
            throws SQLException {

        return SQLLoaderStatementWrapper.prepareStatement(Connection, mTargetTable, loadStatement, madcdColumns,
                jdbcHelper, this.pipeData());
    }

    @Override
    public int finishBatch(int len) throws KETLWriteException {
        if (this.pipeData() == false)
            return super.finishBatch(len);

        int result = 0;

        // tihs must be done or memory will be exhausted
        this.clearBatchLogBatch();

        try {
            if ((this.mBatchCounter >= this.miCommitSize
                    || (this.mBatchCounter > 0 && this.isMemoryLow(mLowMemoryThreashold)) || (len == LASTBATCH && this.mBatchCounter > 0))) {
                this.dedupeCounter = 0;
                this.stmt.executeBatch();
                result = this.mBatchCounter;
                this.miInsertCount += this.mBatchCounter;
                this.mBatchCounter = 0;
                this.executePostBatchStatements();
                this.firePreBatch = true;                
            }

        } catch (SQLException e) {
            try {
                // StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
                // mStatementSeperator, StatementManager.END, this, true);
            } catch (Exception e1) {

            }

            KETLWriteException e1 = new KETLWriteException(e);
            this.incrementErrorCount(e1, null, 1);

            throw e1;
        }

        return result;
    }

    @Override
    protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException {
        if (hdl == null)
            return new SQLLoaderItemHelper();
        else {
            try {
                Class cl = Class.forName(hdl);
                return (SQLLoaderItemHelper) cl.newInstance();
            } catch (Exception e) {
                throw new KETLThreadException("HANDLER class not found", e, this);
            }
        }
    }

    private String mOSCommand;
    private String mTargetTable;

    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        mOSCommand = this.getStepTemplate(mDBType, "SQLLDR", true);
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "CONNECTIONSTRING", this.getParameterValue(0,
                CONNECTIONSTRING_ATTRIB));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "BINDSIZE",Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(),"BINDSIZE",1000)));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "ROWS", Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(),"ROWS",this.miCommitSize)));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "PARALLEL",XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(),"PARALLEL",true)?"TRUE":"FALSE");
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "DIRECT", XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(),"DIRECT",true)?"TRUE":"FALSE");
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "PASSWORD", this.getParameterValue(0,
                PASSWORD_ATTRIB));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "USER", this.getParameterValue(0,
                USER_ATTRIB));
        mTargetTable = pTable;
        return mOSCommand;
    }

    @Override
    public int complete() throws KETLThreadException {

        if (this.pipeData()) {
            try {
                if (this.mBatchCounter > 0) {
                    this.dedupeCounter = 0;
                    this.stmt.executeBatch();
                    this.miInsertCount += this.mBatchCounter;
                    this.mBatchCounter = 0;
                    this.executePostBatchStatements();
                    this.firePreBatch = true;
                    
                    
                }
            } catch (Exception e) {

                try {
                    stmt.close();
                    stmt = null;
                } catch (Exception e1) {
                    ResourcePool.LogException(e1, this);
                }

                if (this.mcDBConnection != null) {
                    ResourcePool.releaseConnection(this.mcDBConnection);
                    this.mcDBConnection = null;
                }

                throw new KETLThreadException(e, e.getMessage());
            }
        }

        return super.complete();
    }

}
