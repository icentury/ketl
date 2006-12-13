/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public abstract class ETLMerge extends ETLStep {


    final public void incrementErrorCount(KETLTransformException e, Object[] left, Object[] right, int val)
            throws KETLTransformException {

        try {
            if (left != null)
                this.logBadRecord(val, left,e);
            if (right != null)
                this.logBadRecord(val, right,e);
        } catch (IOException e1) {
            throw new KETLTransformException(e);
        }

        try {
            super.incrementErrorCount(e, 1, val);
        } catch (Exception e1) {
            if(e1 instanceof KETLTransformException)
                throw (KETLTransformException)e1;
            throw new KETLTransformException(e1);
        }
    }

    ManagedBlockingQueue queue;
    private ManagedBlockingQueue srcQueueRight;
    private ManagedBlockingQueue srcQueueLeft;
    private boolean leftQueueOpen = true, rightQueueOpen = true;
    private DefaultMergeCore core;

    /**
     * @param pQueueSize
     * @param pQueueSize
     */
    public ETLMerge(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    final void setCore(DefaultCore newCore) {
        core = (DefaultMergeCore) newCore;
    }

    protected MergeBatchManager mBatchManager;

    private void closeQueue(Object[][] data, int len) throws InterruptedException, KETLTransformException {

        if (len != data.length) {
            Object[][] newResult = new Object[len][];
            System.arraycopy(data, 0, newResult, 0, len);
            data = newResult;
        }

        if (this.mBatchManagement) {
            data = this.mBatchManager.finishBatch(data, len);
            len = data.length;
        }

        this.updateThreadStats(len);
        this.queue.put(data);
        this.queue.put(ENDOBJ);
    }

    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
        this.mExpectedOutputDataTypes = pClassArray;
        this.mOutputRecordWidth = this.mExpectedOutputDataTypes.length;
    }

    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLMergeCore { ";
    }

    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLMergeCore;\n"
                + "import com.kni.etl.ketl.smp.ETLMerge;\n";
    }

    final protected void executeWorker() throws KETLTransformException, InterruptedException {

        Object[][] oLeftBatch = null, oRightBatch = null, res = new Object[this.batchSize][], freeBatch = null;
        int leftPos = 0, rightPos = 0, rightLen = 0, leftLen = 0, resultLength = 0, batchLength = this.batchSize;

        while (true) {
            this.interruptExecution();
            Object oLeft = null, oRight = null;
            if (leftPos == leftLen && leftQueueOpen) {
                oLeft = this.getSourceQueueLeft().take();
                if (oLeft == ENDOBJ) {
                    leftQueueOpen = false;
                    oLeftBatch = null;

                    if (rightQueueOpen == false) {
                        closeQueue(res, resultLength);
                        break;
                    }
                }
                else {
                    if (freeBatch == null || freeBatch.length < oLeftBatch.length)
                        freeBatch = oLeftBatch;

                    oLeftBatch = (Object[][]) oLeft;
                    leftLen = oLeftBatch.length;
                    leftPos = 0;
                }
            }
            if (rightPos == rightLen && rightQueueOpen) {
                oRight = this.getSourceQueueRight().take();
                if (oRight == ENDOBJ) {
                    rightQueueOpen = false;
                    oRightBatch = null;
                    if (leftQueueOpen == false) {
                        closeQueue(res, resultLength);
                        break;
                    }
                }
                else {
                    if (freeBatch == null || freeBatch.length < oRightBatch.length)
                        freeBatch = oRightBatch;

                    oRightBatch = (Object[][]) oRight;
                    rightLen = oRightBatch.length;
                    rightPos = 0;
                }
            }

            Object[] result = new Object[this.mOutputRecordWidth];
            try {
                if(timing) startTimeNano = System.nanoTime();
                int code = core.mergeRecord(leftQueueOpen ? oLeftBatch[leftPos] : null,
                        this.mLeftExpectedInputDataTypes, this.mLeftInputRecordWidth,
                        rightQueueOpen ? oRightBatch[rightPos] : null, this.mRightExpectedInputDataTypes,
                        this.mRightInputRecordWidth, result, this.mExpectedOutputDataTypes, this.mOutputRecordWidth);
                if(timing) totalTimeNano += System.nanoTime() - startTimeNano;
                
                switch (code) {
                case ETLMergeCore.SUCCESS_ADVANCE_BOTH:
                    if (leftQueueOpen && rightQueueOpen) {
                        leftPos++;
                        rightPos++;
                        res[resultLength++] = result;
                    }
                    else
                        throw new KETLTransformException(
                                "Cannot advance to next left or right record, invalid request", code);
                    break;
                case ETLMergeCore.SUCCESS_ADVANCE_LEFT:
                    if (leftQueueOpen) {
                        leftPos++;
                        res[resultLength++] = result;
                    }
                    else
                        throw new KETLTransformException("Cannot advance to next left record, invalid request", code);
                    break;
                case ETLMergeCore.SUCCESS_ADVANCE_RIGHT:
                    if (rightQueueOpen) {
                        rightPos++;
                        res[resultLength++] = result;
                    }
                    else
                        throw new KETLTransformException("Cannot advance to next right record, invalid request", code);
                    break;
                case ETLMergeCore.SKIP_ADVANCE_BOTH:
                    if (leftQueueOpen && rightQueueOpen) {
                        leftPos++;
                        rightPos++;
                    }
                    else
                        throw new KETLTransformException(
                                "Cannot advance to next left or right record, invalid request", code);
                    break;
                case ETLMergeCore.SKIP_ADVANCE_LEFT:
                    if (leftQueueOpen)
                        leftPos++;
                    else
                        throw new KETLTransformException("Cannot advance to next left record, invalid request", code);
                    break;
                case ETLMergeCore.SKIP_ADVANCE_RIGHT:
                    if (rightQueueOpen)
                        rightPos++;
                    else
                        throw new KETLTransformException("Cannot advance to next right record, invalid request", code);
                    break;

                default:
                    throw new KETLTransformException("Invalid return code, check previous error message", code);
                }
            } catch (KETLTransformException e) {
                this.incrementErrorCount(e, leftQueueOpen ? oLeftBatch[leftPos] : null,
                        rightQueueOpen ? oRightBatch[rightPos] : null, -1);
            }
            if (resultLength == batchLength) {

                if (this.mBatchManagement) {
                    res = this.mBatchManager.finishBatch(res, res.length);
                }

                this.queue.put(res);
                this.updateThreadStats(res.length);
                resultLength = 0;
                if (freeBatch == null) {
                    res = new Object[this.batchSize][];
                }
                else
                    res = freeBatch;

                freeBatch = null;
                batchLength = res.length;
            }
        }

    }

    private Class[] mLeftExpectedInputDataTypes, mRightExpectedInputDataTypes, mExpectedOutputDataTypes;
    private int mLeftInputRecordWidth = -1, mRightInputRecordWidth = -1, mOutputRecordWidth = -1;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.transform.ETLSourceQueue#getSourceQueue()
     */
    final public BlockingQueue getLeftSourceQueue() {
        return this.getSourceQueueLeft();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.transform.ETLSourceQueue#getSourceQueue()
     */
    final public BlockingQueue getRightSourceQueue() {
        return this.getSourceQueueRight();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLWorker#initializeQueues(com.kni.etl.ketl.ETLWorker[], com.kni.etl.ketl.ETLWorker[])
     */
    @Override
    final public void initializeQueues() {
        this.queue.registerWriter(this);
        this.getSourceQueueLeft().registerReader(this);
        this.getSourceQueueRight().registerReader(this);
    }

    
    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        this.queue = newQueue;        
    }

    
    void setSourceQueueLeft(ManagedBlockingQueue srcQueueLeft, ETLWorker worker) throws KETLThreadException {
        this.getUsedPortsFromWorker(worker, ETLWorker.getChannel((Element) this.getXMLConfig(), LEFT), LEFT,
                "pLeftInputRecords");
        this.srcQueueLeft = srcQueueLeft;
        try {
            this.mLeftExpectedInputDataTypes = worker.getOutputRecordDatatypes(ETLWorker.getChannel((Element) this
                    .getXMLConfig(), LEFT));
            this.mLeftInputRecordWidth = this.mLeftExpectedInputDataTypes.length;
        } catch (ClassNotFoundException e) {
            throw new KETLThreadException(e, this);
        }

    }

    ManagedBlockingQueue getSourceQueueLeft() {
        return srcQueueLeft;
    }

    void setSourceQueueRight(ManagedBlockingQueue srcQueueRight, ETLWorker worker) throws KETLThreadException {
        this.getUsedPortsFromWorker(worker, ETLWorker.getChannel((Element) this.getXMLConfig(), RIGHT), RIGHT,
                "pRightInputRecords");
        this.srcQueueRight = srcQueueRight;
        try {
            this.mRightExpectedInputDataTypes = worker.getOutputRecordDatatypes(ETLWorker.getChannel((Element) this
                    .getXMLConfig(), RIGHT));
            this.mRightInputRecordWidth = this.mRightExpectedInputDataTypes.length;
        } catch (ClassNotFoundException e) {
            throw new KETLThreadException(e, this);
        }
    }

    protected void getUsedPortsFromWorker(ETLWorker pWorker, String port, int type, String objectNameInCode)
            throws KETLThreadException {

        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Registering port usage for step "
                + pWorker.toString() + ":" + (type == LEFT ? "LEFT" : "RIGHT") + " by step " + this.toString());

        Node[] nl = com.kni.etl.util.XMLHelper.getElementsByName(this.getXMLConfig(), "IN", type == LEFT ? "LEFT"
                : "RIGHT", "TRUE");
        registerUsedPorts(pWorker, nl, objectNameInCode);

    }

    ManagedBlockingQueue getSourceQueueRight() {

        return srcQueueRight;
    }

    @Override
    protected String getRecordExecuteMethodHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("public int mergeRecord(Object[] pLeftInputRecords, Class[] pLeftInputDataTypes,"
                + " int pLeftInputRecordWidth, Object[] pRightInputRecords, "
                + " Class[] pRightInputDataTypes, int pRightInputRecordWidth," + " Object[] pOutputRecords "
                + ", Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException {");

        return sb.toString();
    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return -1;}";
    }

    
    final String getDefaultExceptionClass() {
        return KETLTransformException.class.getCanonicalName();
    }
    
    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (MergeBatchManager) batchManager;
    }
}
