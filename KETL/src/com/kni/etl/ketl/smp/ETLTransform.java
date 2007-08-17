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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

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

// TODO: Auto-generated Javadoc
/**
 * The Class ETLTransform.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public abstract class ETLTransform extends ETLStep {

    /** The aggregate out. */
    private ArrayList aggregateOut = new ArrayList();

    /** The aggregates. */
    private Aggregator[] aggregates;

    /** The core. */
    private DefaultTransformCore core;

    /** The sort data. */
    private boolean mAggregate = false, mSortData = false;
    
    /** The batch manager. */
    private TransformBatchManager mBatchManager;
    
    /** The default comparator. */
    private Comparator mDefaultComparator = null;

    /** The expected output data types. */
    private Class[] mExpectedInputDataTypes, mExpectedOutputDataTypes;

    /** The external sort. */
    private ExternalSort mExternalSort;

    /** The input record width. */
    private int mInputRecordWidth = -1;

    /** The output record width. */
    private int mOutputRecordWidth = -1;

    /** The post sort batch size. */
    int postSortBatchSize;

    /** The previous. */
    private Object[] previous = null;

    /** The queue. */
    ManagedBlockingQueue queue;
    
    /** The src queue. */
    private ManagedBlockingQueue srcQueue;

    /**
     * The Constructor.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public ETLTransform(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /**
     * Aggregate batch.
     * 
     * @param res the res
     * @param length the length
     * 
     * @throws InterruptedException the interrupted exception
     */
    private void aggregateBatch(Object[][] res, int length) throws InterruptedException {

        Comparator cmp = this.mDefaultComparator;

        // if end of data reached aggregate last value and return
        if (res == null && (this.aggregateOut.size() > 0 || this.previous != null)) {
            Object[] last = new Object[this.mOutputRecordWidth];
            for (int pos = 0; pos < this.mOutputRecordWidth; pos++) {
                last[pos] = this.aggregates[pos].getValue();
            }
            this.aggregateOut.add(last);
            this.sendAggregateBatch();
            return;
        }

        for (int i = 0; i < length; i++) {
            Object[] current = res[i];

            // if previous exists and previous different from current then aggregate values to record
            // and submit batch if big enough
            if (this.previous != null && cmp.compare(current, this.previous) != 0) {

                // possible buffer overrun
                Object[] result = new Object[this.mOutputRecordWidth];
                for (int pos = 0; pos < this.mOutputRecordWidth; pos++) {
                    result[pos] = this.aggregates[pos].getValue();
                }

                this.aggregateOut.add(result);
                if (this.aggregateOut.size() >= length) {
                    this.sendAggregateBatch();
                }
            }

            // aggregate values
            for (int pos = 0; pos < this.mOutputRecordWidth; pos++) {
                this.aggregates[pos].add(current[pos]);
            }

            this.previous = current;

        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#executeWorker()
     */
    @Override
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreHeader()
     */
    @Override
    protected CharSequence generateCoreHeader() {
        return " public class " + this.getCoreClassName() + " extends ETLTransformCore { ";
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreImports()
     */
    @Override
    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLTransformCore;\nimport java.util.Calendar;\n"
                + "import com.kni.etl.ketl.smp.ETLTransform;\n";
    }

    /**
     * Gets the aggregate comparator.
     * 
     * @return the aggregate comparator
     */
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

    /**
     * Gets the max sort size.
     * 
     * @return the max sort size
     */
    private int getMaxSortSize() {
        return 5000;
    }

    /**
     * Gets the merge size.
     * 
     * @return the merge size
     */
    private int getMergeSize() {
        return 128;
    }

    /**
     * Gets the read buffer size.
     * 
     * @return the read buffer size
     */
    private int getReadBufferSize() {
        return 4096;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getRecordExecuteMethodFooter()
     */
    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return SUCCESS;}";
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getRecordExecuteMethodHeader()
     */
    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        StringBuilder sb = new StringBuilder();

        sb.append("public int transformRecord(Object[] pInputRecords, "
                + "Class[] pInputDataTypes, int pInputRecordWidth, Object[] pOutputRecords"
                + " , Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException {");

        return sb.toString();
    }

    /**
     * Gets the sort comparator.
     * 
     * @return the sort comparator
     */
    protected Comparator getSortComparator() {
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

    /**
     * Gets the source queue.
     * 
     * @return the source queue
     */
    ManagedBlockingQueue getSourceQueue() {
        return this.srcQueue;
    }

    /**
     * Gets the total read buffer size.
     * 
     * @return the total read buffer size
     */
    private int getTotalReadBufferSize() {
        return 524288;
    }

    /**
     * Gets the write buffer size.
     * 
     * @return the write buffer size
     */
    private int getWriteBufferSize() {
        return 4096;
    }

    /**
     * Increment error count.
     * 
     * @param e the e
     * @param objects the objects
     * @param val the val
     * 
     * @throws KETLTransformException the KETL transform exception
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
     */
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

    /**
     * Repeat record possible.
     * 
     * @return true, if successful
     */
    protected boolean repeatRecordPossible() {
        return false;
    }

    /**
     * Send aggregate batch.
     * 
     * @throws InterruptedException the interrupted exception
     */
    private void sendAggregateBatch() throws InterruptedException {
        Object[][] out = new Object[this.aggregateOut.size()][];
        this.aggregateOut.toArray(out);
        this.queue.put(out);
        this.aggregateOut.clear();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#setBatchManager(com.kni.etl.ketl.smp.BatchManager)
     */
    @Override
    final protected void setBatchManager(BatchManager batchManager) {
        this.mBatchManager = (TransformBatchManager) batchManager;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#setCore(com.kni.etl.ketl.smp.DefaultCore)
     */
    @Override
    final void setCore(DefaultCore newCore) {
        this.core = (DefaultTransformCore) newCore;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#setOutputRecordDataTypes(java.lang.Class[], java.lang.String)
     */
    @Override
    void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
        this.mExpectedOutputDataTypes = pClassArray;
        this.mOutputRecordWidth = this.mExpectedOutputDataTypes.length;
    }

    /**
     * Sets the source queue.
     * 
     * @param srcQueue the src queue
     * @param worker the worker
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    void setSourceQueue(ManagedBlockingQueue srcQueue, ETLWorker worker) throws KETLThreadException {
        this.getUsedPortsFromWorker(worker, ETLWorker.getChannel(this.getXMLConfig(), ETLWorker.DEFAULT));
        this.srcQueue = srcQueue;
        try {
            this.mExpectedInputDataTypes = worker.getOutputRecordDatatypes(ETLWorker.getChannel(this
                    .getXMLConfig(), ETLWorker.DEFAULT));
            this.mInputRecordWidth = this.mExpectedInputDataTypes.length;
        } catch (ClassNotFoundException e) {
            throw new KETLThreadException(e, this);
        }
          
        this.configureBufferSort(srcQueue);
    }
    
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#switchTargetQueue(com.kni.etl.ketl.smp.ManagedBlockingQueue, com.kni.etl.ketl.smp.ManagedBlockingQueue)
     */
    @Override
    public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
        this.queue = newQueue;        
    }


    /**
     * Sort data.
     * 
     * @throws InterruptedException the interrupted exception
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    private void sortData() throws InterruptedException, IOException, ClassNotFoundException {

        this.postSortBatchSize = this.batchSize;
        // instantiate sorter object
        // int maxSortSize, int mergeSize, int readBufferSize, int maxIndividualReadBufferSize, int writeBufferSize
        this.mExternalSort = new ExternalSort(this.getSortComparator(), this.getMaxSortSize(), this.getMergeSize(), this
                .getTotalReadBufferSize(), this.getReadBufferSize(), this.getWriteBufferSize());

        while (true) {
            this.interruptExecution();
            Object o;
            o = this.getSourceQueue().take();
            if (o == ETLWorker.ENDOBJ) {
                break;
            }
            Object[][] res = (Object[][]) o;

            this.postSortBatchSize = (res.length + this.postSortBatchSize) / 2;

            for (int i = res.length - 1; i > -1; i--)
                this.mExternalSort.add(res[i]);
        }

    }

    /** The count. */
    private int count=0;
    
    /**
     * Transform batch.
     * 
     * @param res the res
     * @param length the length
     * 
     * @return the object[][]
     * 
     * @throws KETLTransformException the KETL transform exception
     * @throws KETLQAException the KETLQA exception
     */
    final Object[][] transformBatch(Object[][] res, int length) throws KETLTransformException, KETLQAException {
        int resultLength = 0;
        int outputArraySize = length;
        boolean newDataArray = true;
        Object[][] data = res;

        for (int i = 0; i < length; i++) {
            Object[] result = new Object[this.mOutputRecordWidth];

            if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + (this.count)+"][IN][" + i + "]" + java.util.Arrays.toString(res[i]));

            int code;
            try {
                code = this.core.transformRecord(res[i], this.mExpectedInputDataTypes, this.mInputRecordWidth, result,
                        this.mExpectedOutputDataTypes, this.mOutputRecordWidth);
                
            } catch (KETLTransformException e) {
                this.recordCheck(result,e);                    
                
                this.incrementErrorCount(e, res[i], 1);
                code = DefaultTransformCore.SKIP_RECORD;
            }

            switch (code) {
            case DefaultTransformCore.SUCCESS:
                
                if(this.mMonitor) ResourcePool.LogMessage(this,ResourcePool.DEBUG_MESSAGE,"[" + (this.count++)+"][OUT][" + i + "]" + java.util.Arrays.toString(result));

                this.recordCheck(result,null);
                
                if (resultLength == outputArraySize) {
                    outputArraySize = outputArraySize + ((outputArraySize + 2) / 2);
                    Object[][] newResult = new Object[outputArraySize][];
                    System.arraycopy(data, 0, newResult, 0, resultLength);
                    data = newResult;
                }
                                
                data[resultLength++] = result;
                break;
            case DefaultTransformCore.SKIP_RECORD:
                break;
            case DefaultTransformCore.REPEAT_RECORD:
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

    /**
     * Transform from queue.
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLTransformException the KETL transform exception
     * @throws KETLQAException the KETLQA exception
     */
    final private void transformFromQueue() throws InterruptedException, KETLTransformException, KETLQAException {

        while (true) {
            this.interruptExecution();
            Object o;
            o = this.getSourceQueue().take();
            if (o == ETLWorker.ENDOBJ) {
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

            if(this.timing) this.startTimeNano = System.nanoTime();            
            res = this.transformBatch(res, res.length);
            if(this.timing) this.totalTimeNano += System.nanoTime() - this.startTimeNano;            

            if (this.mBatchManagement) {
                res = this.mBatchManager.finishBatch(res, res.length);

            }

            if (this.mAggregate) {
                this.aggregateBatch(res, res.length);
            }
            else
                this.queue.put(res);

            this.updateThreadStats(res.length);

        }

    }

    /**
     * Transform from sort.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     * @throws InterruptedException the interrupted exception
     * @throws KETLTransformException the KETL transform exception
     * @throws KETLQAException the KETLQA exception
     */
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

                this.queue.put(ETLWorker.ENDOBJ);
                break;
            }

            Object[][] res;
            
            if(this.timing) this.startTimeNano += System.nanoTime() ;            
            res = this.transformBatch(data, this.postSortBatchSize);
            if(this.timing) this.totalTimeNano += System.nanoTime() - this.startTimeNano;
            
            if (this.mAggregate) {
                this.aggregateBatch(res, res.length);
            }
            else
                this.queue.put(res);

            this.updateThreadStats(res.length);

        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getDefaultExceptionClass()
     */
    @Override
    final String getDefaultExceptionClass() {
        return KETLTransformException.class.getCanonicalName();
    }

    
    /**
     * Gets the expected input data types.
     * 
     * @return the expected input data types
     */
    final protected Class[] getExpectedInputDataTypes() {
        return this.mExpectedInputDataTypes;
    }
}
