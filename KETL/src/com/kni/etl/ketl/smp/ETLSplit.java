/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public abstract class ETLSplit extends ETLStep {

    private DefaultSplitCore core;

    ManagedBlockingQueue queue[];
    private ManagedBlockingQueue srcQueue;
    int queues;
    ArrayList channelList = new ArrayList();
    HashMap channelMap = new HashMap();

    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLSplitCore { ";
    }

    final String getDefaultExceptionClass() {
        return KETLTransformException.class.getCanonicalName();
    }

    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLSplitCore;\n"
                + "import com.kni.etl.ketl.smp.ETLSplit;\n";
    }

    final public void incrementErrorCount(KETLTransformException e, Object[] objects, int val)
            throws KETLTransformException {

        try {
            if (objects != null)
                this.logBadRecord(val, objects, e);
        } catch (IOException e1) {
            throw new KETLTransformException(e1);
        }
        try {
            super.incrementErrorCount(e, 1, val);
        } catch (Exception e1) {
            if (e1 instanceof KETLTransformException)
                throw (KETLTransformException) e1;
            throw new KETLTransformException(e1);
        }
    }

    /**
     * @param pQueueSize
     * @param pQueueSize
     */
    public ETLSplit(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
        channelMap.put(pChannel, channelList.size());
        channelList.add(pClassArray);
    }

    protected String generatePortMappingCode() throws KETLThreadException {
        StringBuilder sb = new StringBuilder();
        // generate constants used for references

        // generate port maps

        // generate mapping method header;
        sb.append(this.getRecordExecuteMethodHeader() + "\n");
        // outputs
        if (this.mOutPorts != null) {

            int x = 0;
            sb.append("switch(pOutPath){\n");
            for (Object o : this.channelMap.keySet()) {
                String channel = (String) o;

                sb.append("case " + (x++) + ": {\n");
                for (int i = 0; i < this.mOutPorts.length; i++) {
                    if (this.mOutPorts[i].getChannel().equals(channel)) {
                        sb.append(this.mOutPorts[i].generateCode(i));
                        sb.append(";\n");
                    }
                }
                sb.append("} break;\n");
            }
            sb.append("}\n");
        }
        // generate mapping method footer
        sb.append(this.getRecordExecuteMethodFooter() + "\n");

        return sb.toString();
    }

    @Override
    final void setCore(DefaultCore newCore) {
        core = (DefaultSplitCore) newCore;
    }

    protected SplitBatchManager mBatchManager;

    final protected void executeWorker() throws InterruptedException, KETLTransformException {

        this.queues = this.queue.length;
        this.mOutputRecordWidth = new int[queues];
        java.util.Arrays.fill(this.mOutputRecordWidth, -1);

        this.mExpectedOutputDataTypes = new Class[queues][];
        this.channelList.toArray(this.mExpectedOutputDataTypes);

        Object[][][] res = new Object[queues][][];
        while (true) {
            this.interruptExecution();
            Object o;
            o = this.getSourceQueue().take();
            if (o == ENDOBJ) {
                
                while(this.remainingRecords()){
                    processData(res,new Object[this.batchSize][this.mExpectedInputDataTypes.length]);
                }
                
                for (int i = 0; i < this.queues; i++) {
                    this.queue[i].put(o);
                }
                break;
            }

            Object[][] data = (Object[][]) o;
            if (this.mBatchManagement) {
                data = this.mBatchManager.initializeBatch(data, data.length);

            }

            processData(res, data);

        }

    }

    protected boolean remainingRecords() {
        return false;
    }

    private void processData(Object[][][] res, Object[][] data) throws KETLTransformException, InterruptedException {
        if (timing)
            startTimeNano = System.nanoTime();
        int rows = splitBatch(data, data.length, res);
        if (timing)
            totalTimeNano += System.nanoTime() - startTimeNano;

        this.updateThreadStats(rows);

        for (int i = 0; i < this.queues; i++) {
            this.queue[i].put(res[i]);
        }        
    }

    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (SplitBatchManager) batchManager;
    }

    private Class[] mExpectedInputDataTypes;
    private Class[][] mExpectedOutputDataTypes;
    private int mInputRecordWidth = -1;
    private int[] mOutputRecordWidth = null;

    /**
     * @param o
     * @return
     * @throws KETLTransformException
     */
    private int splitBatch(Object[][] pInputRecord, int length, Object[][][] pOutput) throws KETLTransformException {
        int rows = 0;

        // build output data arrays
        for(int i=0;i<this.queues;i++)
            pOutput[i] = new Object[length][];
        
        int[] resultLength = new int[this.queues];

        for (int i = 0; i < length; i++) {

            if (i == 0)
                this.mInputRecordWidth = this.mExpectedInputDataTypes.length;

            for (int path = 0; path < this.queues; path++) {

                if (i == 0 && this.mOutputRecordWidth[path] == -1)
                    this.mOutputRecordWidth[path] = this.mExpectedOutputDataTypes[path].length;
                
                Object[] result = new Object[this.mOutputRecordWidth[path]];

                try {
                    int code = core.splitRecord(pInputRecord[i], this.mExpectedInputDataTypes, this.mInputRecordWidth,
                            path, result, this.mExpectedOutputDataTypes[path], this.mOutputRecordWidth[path]);

                    switch (code) {
                    case ETLSplitCore.SUCCESS:
                        pOutput[path][resultLength[path]++] = result;
                        break;
                    case ETLSplitCore.SKIP_RECORD:
                        break;
                    default:
                        throw new KETLTransformException("Invalid return code, check previous error message", code);
                    }
                } catch (KETLTransformException e) {
                    this.incrementErrorCount(e, pInputRecord[i], 1);
                }
            }

        }

        for (int path = 0; path < this.queues; path++) {
            if (resultLength[path] != length) {
                Object[][] newResult = new Object[resultLength[path]][];
                System.arraycopy(pOutput[path], 0, newResult, 0, resultLength[path]);
                pOutput[path] = newResult;
            }

            rows += resultLength[path];
        }

        return rows;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLWorker#initializeQueues(com.kni.etl.ketl.ETLWorker[], com.kni.etl.ketl.ETLWorker[])
     */
    @Override
    final public void initializeQueues() {
        for (int i = 0; i < this.queue.length; i++)
            this.queue[i].registerWriter(this);
        this.getSourceQueue().registerReader(this);
    }

    void setSourceQueue(ManagedBlockingQueue srcQueue, ETLWorker worker) throws KETLThreadException {
        this.getUsedPortsFromWorker(worker, ETLWorker.getChannel((Element) this.getXMLConfig(), DEFAULT));
        this.srcQueue = srcQueue;
        try {
            this.mExpectedInputDataTypes = worker.getOutputRecordDatatypes(ETLWorker.getChannel((Element) this
                    .getXMLConfig(), DEFAULT));
            this.mInputRecordWidth = this.mExpectedInputDataTypes.length;
        } catch (ClassNotFoundException e) {
            throw new KETLThreadException(e, this);
        }
        
        this.configureBufferSort(srcQueue);

    }

    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        for (int i = 0; i < this.queue.length; i++)
            if (this.queue[i] == currentQueue)
                this.queue[i] = newQueue;
    }

    ManagedBlockingQueue getSourceQueue() {
        return srcQueue;
    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        StringBuilder sb = new StringBuilder();

        sb.append("public int splitRecord(Object[] pInputRecords,"
                + " Class[] pInputDataTypes, int pInputRecordWidth, "
                + " int pOutPath, Object[] pOutputRecords, Class[] pOutputDataTypes,"
                + " int pOutputRecordWidth) throws KETLTransformException {\n");

        return sb.toString();
    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return SUCCESS;}";
    }
}
