/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderItemHelper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.ETLThreadManager;

abstract public class BulkLoaderELTWriter extends JDBCWriter {

    
    public BulkLoaderELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }


    private boolean mPipeData = !System.getProperty("os.name").startsWith("Windows");

  

    final protected boolean pipeData() {
        return this.mPipeData;
    }

  

    @Override
    final public int finishBatch(int len) throws KETLWriteException {
        if (this.pipeData() == false)
            return super.finishBatch(len);

        this.clearBatchLogBatch();
        int result = this.mBatchCounter;
        this.miInsertCount += this.mBatchCounter;
        this.mBatchCounter = 0;

        return result;
    }

    @Override
    final protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException {
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

   
    @Override
    final public int complete() throws KETLThreadException {

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
