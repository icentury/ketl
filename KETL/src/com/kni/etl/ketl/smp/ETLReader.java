/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public abstract class ETLReader extends ETLStep {

    ManagedBlockingQueue queue;
    private DefaultReaderCore core;
    private boolean mbSamplingEnabled = false;

    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLReaderCore { ";
    }

    final public void incrementErrorCount(KETLReadException e, int errors, int recordCounter) throws KETLReadException {
        try {
            super.incrementErrorCount(e, errors, recordCounter);
        } catch (Exception e1) {
            if (e1 instanceof KETLReadException)
                throw (KETLReadException) e1;
            throw new KETLReadException(e1);
        }
    }

    final String getDefaultExceptionClass() {
        return KETLReadException.class.getCanonicalName();
    }

    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLReaderCore;\n"
                + "import com.kni.etl.ketl.smp.ETLReader;\n";
    }

    @Override
    final void setCore(DefaultCore newCore) {
        core = (DefaultReaderCore) newCore;
    }

    /**
     * @param pBatchSize
     * @param pQueueSize
     * @param pThreadManager TODO
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

        mOutPorts = new ETLOutPort[this.hmOutports.size()];

        this.hmOutports.values().toArray(mOutPorts);
    }

    protected boolean alwaysOverrideOuts() {
        return false;
    }

    protected void overrideOuts() throws KETLThreadException {
    }

    @Override
    protected String getRecordExecuteMethodHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("public int getNextRecord(Object[] pOutputRecords, "
                + " Class[] pExpectedDataTypes, int pRecordWidth) throws KETLReadException { try{ int res = 0;");

        return sb.toString();
    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        return "}catch(Exception e) {try {this.getOwner().handleException(e);} catch(Exception e1){throw new KETLReadException(e1);}} return  SUCCESS;}";
    }

    /*
     * protected String generatePortMappingCode() throws KETLThreadException { StringBuilder sb = new
     * StringBuilder("\n//Going to be reader specific\n"); // generate constants used for references // generate port
     * maps // outputs return sb.toString(); }
     */
    protected void initializeOutports(ETLPort[] outPortNodes) throws KETLThreadException {
        for (int i = 0; i < outPortNodes.length; i++) {
            ETLPort port = outPortNodes[i];

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

    protected ReaderBatchManager mBatchManager;

    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (ReaderBatchManager) batchManager;
    }

    final protected void executeWorker() throws InterruptedException, ClassNotFoundException, KETLThreadException,
            KETLReadException {

        Object[][] o;
        boolean data = true;

        while (data) {
            this.interruptExecution();
            if (this.mBatchManagement) {
                this.mBatchManager.initializeBatch();
            }

            if (timing)
                startTimeNano = System.nanoTime();
            o = getNextBatch();
            if (timing)
                totalTimeNano += System.nanoTime() - startTimeNano;

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

        this.queue.put(ENDOBJ);

    }

    private Class[] mExpectedDataTypes;
    private int mRecordWidth;
    protected int mRecordCounter;
    private int mSamplingRate;
    private int count=0;

    final protected Object[][] getNextBatch() throws KETLReadException {
        Object[][] batch = new Object[this.batchSize][];
        int resultLength = 0;
        for (int i = 0; i < this.batchSize; i++) {
            Object[] o = new Object[mRecordWidth];
            try {
                int code = core.getNextRecord(o, mExpectedDataTypes, mRecordWidth);

                if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + (count++)+"]" + java.util.Arrays.toString(o));

                switch (code) {
                case ETLReaderCore.SUCCESS:
                    this.mRecordCounter++;
                    if ((this.mbSamplingEnabled == false || (this.mRecordCounter++ % this.mSamplingRate == 0)))
                        batch[resultLength++] = o;
                    else
                        i--;
                    break;
                case ETLReaderCore.SKIP_RECORD:
                    i--;
                    break;
                case ETLReaderCore.COMPLETE:
                    // forces loop to finish
                    i = this.batchSize;
                    break;
                default:
                    throw new KETLReadException("Invalid return code, check previous error message", code);
                }
            } catch (KETLReadException e) {
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

    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
        this.mExpectedDataTypes = pClassArray;
        this.mRecordWidth = this.mExpectedDataTypes.length;
    }

    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        this.queue = newQueue;        
    }

}
