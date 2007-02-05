/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.util.ExternalSort;
import com.kni.etl.util.XMLHelper;
import com.kni.etl.util.aggregator.Aggregator;
import com.kni.etl.util.aggregator.Direct;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public abstract class ETLTransform extends ETLStep {

    final class DefaultComparator implements Comparator {

        Integer[] elements;
        int len;
        Boolean[] order;

        public DefaultComparator(Integer[] elements) {
            super();
            this.elements = elements;
            this.order = new Boolean[elements.length];
            java.util.Arrays.fill(order, new Boolean(true));
            len = this.elements.length;
        }

        public DefaultComparator(Integer[] elements, Boolean[] order) {
            super();
            this.elements = elements;
            this.order = order;
            len = this.elements.length;
        }

        public int compare(Object o1, Object o2) {

            Object[] left = (Object[]) o1;
            Object[] right = (Object[]) o2;

            for (int i = 0; i < len; i++) {

                Comparable l = (Comparable) (order[i] ? left : right)[elements[i]];
                Comparable r = (Comparable) (order[i] ? right : left)[elements[i]];
                int res;
                if (l == null && r == null)
                    res = 0;
                else if (l == null && r != null)
                    res = -1;
                else if (l != null && r == null)
                    res = 1;
                else
                    res = l.compareTo(r);

                if (res != 0)
                    return res;
            }

            return 0;
        }

    }

    private ArrayList aggregateOut = new ArrayList();

    private Aggregator[] aggregates;

    private DefaultTransformCore core;

    private boolean mAggregate = false, mSortData = false;
    private TransformBatchManager mBatchManager;
    private Comparator mDefaultComparator = null;

    private Class[] mExpectedInputDataTypes, mExpectedOutputDataTypes;

    private ExternalSort mExternalSort;

    private int mInputRecordWidth = -1;

    private int mOutputRecordWidth = -1;

    int postSortBatchSize;

    private Object[] previous = null;

    ManagedBlockingQueue queue;
    private ManagedBlockingQueue srcQueue;

    /**
     * @param pQueueSize
     * @param pQueueSize
     */
    public ETLTransform(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    private void aggregateBatch(Object[][] res, int length) throws InterruptedException {

        Comparator cmp = mDefaultComparator;

        // if end of data reached aggregate last value and return
        if (res == null && (this.aggregateOut.size() > 0 || previous != null)) {
            Object[] last = new Object[this.mOutputRecordWidth];
            for (int pos = 0; pos < this.mOutputRecordWidth; pos++) {
                last[pos] = aggregates[pos].getValue();
            }
            this.aggregateOut.add(last);
            sendAggregateBatch();
            return;
        }

        for (int i = 0; i < length; i++) {
            Object[] current = res[i];

            // if previous exists and previous different from current then aggregate values to record
            // and submit batch if big enough
            if (previous != null && cmp.compare(current, previous) != 0) {

                // possible buffer overrun
                Object[] result = new Object[this.mOutputRecordWidth];
                for (int pos = 0; pos < this.mOutputRecordWidth; pos++) {
                    result[pos] = aggregates[pos].getValue();
                }

                this.aggregateOut.add(result);
                if (this.aggregateOut.size() >= length) {
                    sendAggregateBatch();
                }
            }

            // aggregate values
            for (int pos = 0; pos < this.mOutputRecordWidth; pos++) {
                aggregates[pos].add(current[pos]);
            }

            previous = current;

        }

    }

    final protected void executeWorker() throws ClassNotFoundException, KETLThreadException, InterruptedException,
            IOException, KETLTransformException {

        if (this instanceof AggregatingTransform) {
            this.mAggregate = true;
            this.aggregates = ((AggregatingTransform) this).getAggregates();
            this.mDefaultComparator = this.getAggregateComparator();
        }

        if (this.mSortData) {
            this.sortData();
            this.transformFromSort();
        }
        else
            this.transformFromQueue();

    }

    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLTransformCore { ";
    }

    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLTransformCore;\nimport java.util.Calendar;\n"
                + "import com.kni.etl.ketl.smp.ETLTransform;\n";
    }

    private Comparator getAggregateComparator() {
        ArrayList res = new ArrayList();
        for (int i = 0; i < this.aggregates.length; i++)
            if (this.aggregates[i] instanceof Direct) {
                res.add(i);
            }

        Integer[] elements = new Integer[res.size()];

        res.toArray(elements);

        return new DefaultComparator(elements);
    }

    private int getMaxSortSize() {
        return 5000;
    }

    private int getMergeSize() {
        return 128;
    }

    private int getReadBufferSize() {
        return 4096;
    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return SUCCESS;}";
    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        StringBuilder sb = new StringBuilder();

        sb.append("public int transformRecord(Object[] pInputRecords, "
                + "Class[] pInputDataTypes, int pInputRecordWidth, Object[] pOutputRecords"
                + " , Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException {");

        return sb.toString();
    }

    private Comparator getSortComparator() {
        ArrayList cols = new ArrayList();
        ArrayList order = new ArrayList();
        for (int i = 0; i < this.mInPorts.length; i++)
            switch (this.mInPorts[i].getSort()) {
            case ETLInPort.ASC:
                cols.add(i);
                order.add(true);
                break;
            case ETLInPort.DESC:
                cols.add(i);
                order.add(false);
                break;
            }

        Integer[] elements = new Integer[cols.size()];
        Boolean[] elementOrder = new Boolean[order.size()];

        cols.toArray(elements);
        order.toArray(elementOrder);

        return new DefaultComparator(elements, elementOrder);
    }

    ManagedBlockingQueue getSourceQueue() {
        return srcQueue;
    }

    private int getTotalReadBufferSize() {
        return 524288;
    }

    private int getWriteBufferSize() {
        return 4096;
    }

    final public void incrementErrorCount(KETLTransformException e, Object[] objects, int val)
            throws KETLTransformException {

        try {
            // thread is being interrupted externally, do not log
            if(e.getCause() instanceof InterruptedException) throw e;
            
            if (objects != null)
                this.logBadRecord(val, objects,e);
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

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        this.mSortData = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), "SORT", false);

        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLWorker#initializeQueues(com.kni.etl.ketl.ETLWorker[], com.kni.etl.ketl.ETLWorker[])
     */
    @Override
    final public void initializeQueues() {
        this.queue.registerWriter(this);
        this.getSourceQueue().registerReader(this);
    }

    protected boolean repeatRecordPossible() {
        return false;
    }

    private void sendAggregateBatch() throws InterruptedException {
        Object[][] out = new Object[this.aggregateOut.size()][];
        this.aggregateOut.toArray(out);
        this.queue.put(out);
        this.aggregateOut.clear();
    }

    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (TransformBatchManager) batchManager;
    }

    @Override
    final void setCore(DefaultCore newCore) {
        core = (DefaultTransformCore) newCore;
    }

    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
        this.mExpectedOutputDataTypes = pClassArray;
        this.mOutputRecordWidth = this.mExpectedOutputDataTypes.length;
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

    }
    
    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        this.queue = newQueue;        
    }


    private void sortData() throws InterruptedException, IOException, ClassNotFoundException {

        this.postSortBatchSize = this.batchSize;
        // instantiate sorter object
        // int maxSortSize, int mergeSize, int readBufferSize, int maxIndividualReadBufferSize, int writeBufferSize
        mExternalSort = new ExternalSort(this.getSortComparator(), this.getMaxSortSize(), this.getMergeSize(), this
                .getTotalReadBufferSize(), this.getReadBufferSize(), this.getWriteBufferSize());

        while (true) {
            this.interruptExecution();
            Object o;
            o = this.getSourceQueue().take();
            if (o == ENDOBJ) {
                break;
            }
            Object[][] res = (Object[][]) o;

            this.postSortBatchSize = (res.length + this.postSortBatchSize) / 2;

            for (int i = res.length - 1; i > -1; i--)
                this.mExternalSort.add(res[i]);
        }

    }

    private int count=0;
    /**
     * @param o
     * @return
     * @throws KETLTransformException
     * @throws KETLQAException 
     */
    final Object[][] transformBatch(Object[][] res, int length) throws KETLTransformException, KETLQAException {
        int resultLength = 0;
        int outputArraySize = length;
        boolean newDataArray = true;
        Object[][] data = res;

        for (int i = 0; i < length; i++) {
            Object[] result = new Object[this.mOutputRecordWidth];

            if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + (count)+"][IN][" + i + "]" + java.util.Arrays.toString(res[i]));

            int code;
            try {
                code = core.transformRecord(res[i], this.mExpectedInputDataTypes, this.mInputRecordWidth, result,
                        this.mExpectedOutputDataTypes, this.mOutputRecordWidth);
                
            } catch (KETLTransformException e) {
                this.recordCheck(result,e);                    
                
                this.incrementErrorCount(e, res[i], 1);
                code = ETLTransformCore.SKIP_RECORD;
            }

            switch (code) {
            case ETLTransformCore.SUCCESS:
                
                if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + (count++)+"][OUT][" + i + "]" + java.util.Arrays.toString(result));

                this.recordCheck(result,null);
                
                if (resultLength == outputArraySize) {
                    outputArraySize = outputArraySize + ((outputArraySize + 2) / 2);
                    Object[][] newResult = new Object[outputArraySize][];
                    System.arraycopy(data, 0, newResult, 0, resultLength);
                    data = newResult;
                }
                                
                data[resultLength++] = result;
                break;
            case ETLTransformCore.SKIP_RECORD:
                break;
            case ETLTransformCore.REPEAT_RECORD:
                // if we repeat then we need to enlarge the array
                if (resultLength == outputArraySize) {
                    outputArraySize = outputArraySize + ((outputArraySize + 2) / 2);
                    Object[][] newResult = new Object[outputArraySize][];
                    System.arraycopy(data, 0, newResult, 0, resultLength);
                    data = newResult;
                }

                if (newDataArray) {
                    Object[][] tmp = new Object[length][];
                    System.arraycopy(data, 0, tmp, 0, resultLength);
                    data = tmp;
                    newDataArray = false;
                }

                data[resultLength++] = result;
                i--;
                break;
            default:
                throw new KETLTransformException("Invalid return code, check previous error message", code);
            }

        }

        if (resultLength != length) {
            Object[][] newResult = new Object[resultLength][];
            System.arraycopy(data, 0, newResult, 0, resultLength);
            data = newResult;
        }

        return data;
    }

    final private void transformFromQueue() throws InterruptedException, KETLTransformException, KETLQAException {

        while (true) {
            this.interruptExecution();
            Object o;
            o = this.getSourceQueue().take();
            if (o == ENDOBJ) {
                if (this.mAggregate) {
                    this.aggregateBatch(null, -1);
                }

                if (this.mBatchManagement) {
                    this.mBatchManager.finishBatch(null,BatchManager.LASTBATCH);
                }
                this.queue.put(o);
                break;
            }
            Object[][] res = (Object[][]) o;

            if (this.mBatchManagement) {
                res = this.mBatchManager.initializeBatch(res, res.length);

            }

            if(timing) startTimeNano = System.nanoTime();            
            res = transformBatch(res, res.length);
            if(timing) totalTimeNano += System.nanoTime() - startTimeNano;            

            if (this.mBatchManagement) {
                res = this.mBatchManager.finishBatch(res, res.length);

            }

            if (this.mAggregate) {
                aggregateBatch(res, res.length);
            }
            else
                this.queue.put(res);

            this.updateThreadStats(res.length);

        }

    }

    private void transformFromSort() throws IOException, ClassNotFoundException, InterruptedException,
            KETLTransformException, KETLQAException {
        boolean readData = true;
        while (true) {
            this.interruptExecution();
            Object o = null;

            Object[][] data = readData ? new Object[this.postSortBatchSize][] : null;

            if (readData) {
                for (int i = 0; i < this.postSortBatchSize; i++) {
                    o = this.mExternalSort.getNext();
                    if (o == null) {
                        readData = false;
                        Object[][] tmp = new Object[i][];
                        System.arraycopy(data, 0, tmp, 0, i);
                        data = tmp;
                        this.postSortBatchSize = i;
                        break;
                    }
                    data[i] = (Object[]) o;
                }
            }

            if (data == null) {
                if (this.mAggregate) {
                    this.aggregateBatch(null, -1);
                }

                this.queue.put(ENDOBJ);
                break;
            }

            Object[][] res;
            
            if(timing) startTimeNano += System.nanoTime() ;            
            res = transformBatch(data, this.postSortBatchSize);
            if(timing) totalTimeNano += System.nanoTime() - startTimeNano;
            
            if (this.mAggregate) {
                aggregateBatch(res, res.length);
            }
            else
                this.queue.put(res);

            this.updateThreadStats(res.length);

        }

    }

    final String getDefaultExceptionClass() {
        return KETLTransformException.class.getCanonicalName();
    }

    
    final protected Class[] getExpectedInputDataTypes() {
        return mExpectedInputDataTypes;
    }
}
