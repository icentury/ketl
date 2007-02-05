/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.io.IOException;
import java.util.ArrayList;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public abstract class ETLWriter extends ETLStep {

    final public void incrementErrorCount(KETLWriteException e, Object[] objects, int val) throws KETLWriteException {

        try {
            if (objects != null)
                this.logBadRecord(val, objects, e);
        } catch (IOException e1) {
            throw new KETLWriteException(e1);
        }
        try {
            super.incrementErrorCount(e, 1, val);
        } catch (Exception e1) {
            if (e1 instanceof KETLWriteException)
                throw (KETLWriteException) e1;
            throw new KETLWriteException(e1);
        }
    }

    private ManagedBlockingQueue srcQueue;

    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLWriterCore { ";
    }

    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLWriterCore;\n"
                + "import com.kni.etl.ketl.smp.ETLWriter;\n";
    }

    public ETLWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    private Class[] mExpectedDataTypes;
    private int mRecordWidth;
    private DefaultWriterCore core;

    protected Class[] getExpectedDataTypes() {
        return this.mExpectedDataTypes;
    }

    @Override
    final protected void initializeOutports(ETLPort[] outPortNodes) throws KETLThreadException {
    }

    final protected int putNextBatch(Object[][] o, int length) throws KETLWriteException {
        int count = 0;
        for (int i = 0; i < length; i++) {
            try {
                if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + count+"]" + java.util.Arrays.toString(o[i]));
                int res = core.putNextRecord(o[i], this.mExpectedDataTypes, this.mRecordWidth);
                if (res > 0) {
                    this.recordCheck(o[i],null);                                        
                    count += res;
                }
                else if (res < 0) {
                    throw new KETLWriteException("Unknown error, see previous messages");
                }
            } catch (KETLWriteException e) {
                this.recordCheck(o[i],e);                                    
                this.incrementErrorCount(e, o[i], this.getRecordsProcessed() + i + 1);
            }
        }

        return count;
    }

    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
    }

    private WriterBatchManager mBatchManager;

    final protected void executeWorker() throws InterruptedException, KETLWriteException {
        int res;
        while (true) {
            this.interruptExecution();
            Object o;
            o = this.getSourceQueue().take();
            if (o == ENDOBJ) {
                break;
            }

            Object[][] data = (Object[][]) o;

            if (this.mBatchManagement) {

                data = this.mBatchManager.initializeBatch(data, data.length);

            }

            if (timing)
                startTimeNano = System.nanoTime();
            res = this.putNextBatch(data, data.length);
            if (timing)
                totalTimeNano += System.nanoTime() - startTimeNano;

            if (this.mBatchManagement) {
                res = this.mBatchManager.finishBatch(data.length);

            }

            this.updateThreadStats(res);

        }

        if (this.mBatchManagement) {
            res = this.mBatchManager.finishBatch(BatchManager.LASTBATCH);
            this.updateThreadStats(res);
        }

    }

    final String getDefaultExceptionClass() {
        return KETLWriteException.class.getCanonicalName();
    }

    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (WriterBatchManager) batchManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLWorker#initializeQueues(com.kni.etl.ketl.ETLWorker[], com.kni.etl.ketl.ETLWorker[])
     */
    @Override
    final public void initializeQueues() {
        // instantiate out array
        ArrayList al = new ArrayList();
        Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "IN", "*", "*");
        if (nl != null) {

            for (int i = 0; i < nl.length; i++) {
                ETLPort p = this.getInPort(XMLHelper.getAttributeAsString(nl[i].getAttributes(), "NAME", null));

                if (p != null)
                    al.add(p);
            }

            mInPorts = new ETLInPort[al.size()];

            al.toArray(mInPorts);
        }

        this.getSourceQueue().registerReader(this);
    }

    void setSourceQueue(ManagedBlockingQueue srcQueue, ETLWorker worker) throws KETLThreadException {
        this.getUsedPortsFromWorker(worker, ETLWorker.getChannel((Element) this.getXMLConfig(), DEFAULT));
        this.srcQueue = srcQueue;
        try {
            this.mExpectedDataTypes = worker.getOutputRecordDatatypes(ETLWorker.getChannel((Element) this
                    .getXMLConfig(), DEFAULT));
            this.mRecordWidth = this.mExpectedDataTypes.length;
        } catch (ClassNotFoundException e) {
            throw new KETLThreadException(e, this);
        }

    }

    ManagedBlockingQueue getSourceQueue() {
        return srcQueue;
    }

    @Override
    protected String getRecordExecuteMethodHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) "
                + " throws KETLWriteException {");

        return sb.toString();
    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return -1;}";
    }

    @Override
    final void setCore(DefaultCore newCore) {
        core = (DefaultWriterCore) newCore;
    }

    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        // do nothing        
    }
}
