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
package com.kni.etl.ketl.smp;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLReader.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public abstract class ETLReader extends ETLStep {

    /** The queue. */
    ManagedBlockingQueue queue;
    
    /** The core. */
    private DefaultReaderCore core;
    
    /** The mb sampling enabled. */
    private boolean mbSamplingEnabled = false;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreHeader()
     */
    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLReaderCore { ";
    }

    /**
     * Increment error count.
     * 
     * @param e the e
     * @param errors the errors
     * @param recordCounter the record counter
     * 
     * @throws KETLReadException the KETL read exception
     */
    final public void incrementErrorCount(KETLReadException e, int errors, int recordCounter) throws KETLReadException {
        try {
            super.incrementErrorCount(e, errors, recordCounter);
        } catch (Exception e1) {
            if (e1 instanceof KETLReadException)
                throw (KETLReadException) e1;
            throw new KETLReadException(e1);
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getDefaultExceptionClass()
     */
    @Override
    final String getDefaultExceptionClass() {
        return KETLReadException.class.getCanonicalName();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreImports()
     */
    @Override
    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLReaderCore;\n"
                + "import com.kni.etl.ketl.smp.ETLReader;\n";
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#setCore(com.kni.etl.ketl.smp.DefaultCore)
     */
    @Override
    final void setCore(DefaultCore newCore) {
        this.core = (DefaultReaderCore) newCore;
    }

    /**
     * The Constructor.
     * 
     * @param pThreadManager TODO
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public ETLReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        if (XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "INFERRED", this.alwaysOverrideOuts())) {
            this.overrideOuts();
        }

        NodeList nl = ((Element) pXMLConfig).getElementsByTagName("OUT");

        for (int i = 0; i < nl.getLength(); i++) {
            ETLPort out = this.getNewOutPort(null);

            try {
                out.initialize(nl.item(i));
            } catch (ClassNotFoundException e) {
                throw new KETLThreadException(e, this);
            }
            if (this.hmOutports.put(out.getPortName(), out) != null)
                throw new KETLThreadException("Duplicate OUT port name exists, check step " + this.getName() + " port "
                        + out.mstrName, this);
        }

        this.mOutPorts = new ETLOutPort[this.hmOutports.size()];

        this.hmOutports.values().toArray(this.mOutPorts);
    }

    /**
     * Always override outs.
     * 
     * @return true, if successful
     */
    protected boolean alwaysOverrideOuts() {
        return false;
    }

    /**
     * Override outs.
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    protected void overrideOuts() throws KETLThreadException {
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getRecordExecuteMethodHeader()
     */
    @Override
    protected String getRecordExecuteMethodHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("public int getNextRecord(Object[] pOutputRecords, "
                + " Class[] pExpectedDataTypes, int pRecordWidth) throws KETLReadException { try{ int res = 0;");

        return sb.toString();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getRecordExecuteMethodFooter()
     */
    @Override
    protected String getRecordExecuteMethodFooter() {
        return "}catch(Exception e) {try {this.getOwner().handleException(e);} catch(Exception e1){throw new KETLReadException(e1);}} return  SUCCESS;}";
    }

    /*
     * protected String generatePortMappingCode() throws KETLThreadException { StringBuilder sb = new
     * StringBuilder("\n//Going to be reader specific\n"); // generate constants used for references // generate port
     * maps // outputs return sb.toString(); }
     */
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#initializeOutports(com.kni.etl.ketl.ETLPort[])
     */
    @Override
    protected void initializeOutports(ETLPort[] outPortNodes) throws KETLThreadException {
        for (ETLPort port : outPortNodes) {
            if (port.isConstant()) {
                port.instantiateConstant();
            }
            else {
                if (port.getPortClass() == null) {
                    throw new KETLThreadException("For code based transforms DATATYPE must be specified", this);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLWorker#initializeQueues(com.kni.etl.ketl.ETLWorker[], com.kni.etl.ketl.ETLWorker[])
     */
    @Override
    final public void initializeQueues() {
        this.queue.registerWriter(this);
    }

    /** The batch manager. */
    protected ReaderBatchManager mBatchManager;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#setBatchManager(com.kni.etl.ketl.smp.BatchManager)
     */
    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (ReaderBatchManager) batchManager;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#executeWorker()
     */
    @Override
    final protected void executeWorker() throws InterruptedException, ClassNotFoundException, KETLThreadException,
            KETLReadException {

        Object[][] o;
        boolean data = true;

        while (data) {
            this.interruptExecution();
            if (this.mBatchManagement) {
                this.mBatchManager.initializeBatch();
            }

            if (this.timing)
                this.startTimeNano = System.nanoTime();
            o = this.getNextBatch();
            if (this.timing)
                this.totalTimeNano += System.nanoTime() - this.startTimeNano;

            if (this.mBatchManagement) {
                o = this.mBatchManager.finishBatch(o, o.length);
            }

            if (o.length < this.batchSize) {
                data = false;
                this.updateThreadStats(o.length);
            }
            else {
                this.updateThreadStats(this.batchSize);
            }

            this.queue.put(o);
        }

        this.queue.put(ETLWorker.ENDOBJ);

    }

    /** The expected data types. */
    private Class[] mExpectedDataTypes;
    
    /** The record width. */
    private int mRecordWidth;
    
    /** The record counter. */
    protected int mRecordCounter;
    
    /** The sampling rate. */
    private int mSamplingRate;
    
    /** The count. */
    private int count=0;

    /**
     * Gets the next batch.
     * 
     * @return the next batch
     * 
     * @throws KETLReadException the KETL read exception
     * @throws KETLQAException the KETLQA exception
     */
    final protected Object[][] getNextBatch() throws KETLReadException, KETLQAException {
        Object[][] batch = new Object[this.batchSize][];
        int resultLength = 0;
        for (int i = 0; i < this.batchSize; i++) {
            Object[] o = new Object[this.mRecordWidth];
            try {
                int code = this.core.getNextRecord(o, this.mExpectedDataTypes, this.mRecordWidth);

                if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + (this.count++)+"]" + java.util.Arrays.toString(o));

                switch (code) {
                case DefaultReaderCore.SUCCESS:
                    this.mRecordCounter++;
                    this.recordCheck(o,null);                    
                    if ((this.mbSamplingEnabled == false || (this.mRecordCounter++ % this.mSamplingRate == 0)))
                        batch[resultLength++] = o;
                    else
                        i--;
                    break;
                case DefaultReaderCore.SKIP_RECORD:
                    i--;
                    break;
                case DefaultReaderCore.COMPLETE:
                    // forces loop to finish
                    i = this.batchSize;
                    break;
                default:
                    throw new KETLReadException("Invalid return code, check previous error message", code);
                }
            } catch (KETLReadException e) {
                this.recordCheck(o,e);            
                this.incrementErrorCount(e, 1, this.mRecordCounter);
            }
        }

        if (resultLength != this.batchSize) {
            Object[][] newResult = new Object[resultLength][];
            System.arraycopy(batch, 0, newResult, 0, resultLength);
            batch = newResult;
        }
        return batch;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#setOutputRecordDataTypes(java.lang.Class[], java.lang.String)
     */
    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
        this.mExpectedDataTypes = pClassArray;
        this.mRecordWidth = this.mExpectedDataTypes.length;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#switchTargetQueue(com.kni.etl.ketl.smp.ManagedBlockingQueue, com.kni.etl.ketl.smp.ManagedBlockingQueue)
     */
    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        this.queue = newQueue;        
    }

}
