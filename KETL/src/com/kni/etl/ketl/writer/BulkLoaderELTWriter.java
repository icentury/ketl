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

import org.w3c.dom.Node;

import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderItemHelper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.ETLThreadManager;

// TODO: Auto-generated Javadoc
/**
 * The Class BulkLoaderELTWriter.
 */
abstract public class BulkLoaderELTWriter extends JDBCWriter {

    /**
     * Instantiates a new bulk loader ELT writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public BulkLoaderELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /** The pipe data. */
    private boolean mPipeData = !System.getProperty("os.name").startsWith("Windows");

    /**
     * Pipe data.
     * 
     * @return true, if successful
     */
    final protected boolean pipeData() {
        return this.mPipeData;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.DatabaseELTWriter#finishBatch(int)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.JDBCELTWriter#instantiateHelper(java.lang.String)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.DatabaseELTWriter#complete()
     */
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
                    this.stmt.close();
                    this.stmt = null;
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
