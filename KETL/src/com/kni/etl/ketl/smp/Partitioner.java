package com.kni.etl.ketl.smp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.w3c.dom.Node;

import com.kni.etl.util.SortedList;
import com.kni.etl.util.XMLHelper;

final public class Partitioner extends ManagedBlockingQueue {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private int mCapacity;
    private int[] mHashOrder;
    private List[] mOutData;
    private int mHashLength = 0;
    private Node[] mPartitionKeys;
    private ManagedBlockingQueue[] mDestinationQueues;
    private int mDestQueues = 0;
    private String name;
    private boolean mSorted;

    public Partitioner(Node[] partitionKeys, Comparator sortComparator, int targetPartitions, int capacity) {
        // we don't use this queue and instead redirect
        super(1);

        this.mDestQueues = targetPartitions;

        this.mHashOrder = new int[partitionKeys.length];
        this.mOutData = new List[targetPartitions];
        this.mSorted = sortComparator != null;

        for (int i = 0; i < targetPartitions; i++)
            this.mOutData[i] = mSorted ? new SortedList(sortComparator) : new ArrayList();

        this.mPartitionKeys = partitionKeys;
        this.mHashLength = partitionKeys.length;
        this.mDestinationQueues = new ManagedBlockingQueue[targetPartitions];
        this.mCapacity = capacity;
    }

    class PartitionTargerManagedBlockingQueue extends ManagedBlockingQueue {

        private static final long serialVersionUID = 1L;
        private String name;
        private Partitioner mParentPartitioner;

        /**
         * @param pCapacity
         * @param pName TODO
         */
        public PartitionTargerManagedBlockingQueue(Partitioner parentPartitioner, int pCapacity) {
            super(pCapacity);
            mParentPartitioner = parentPartitioner;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.kni.etl.ketl.smp.MQueue#setName(java.lang.String)
         */
        public void setName(String arg0) {
            this.name = arg0;
        }

        public String toString() {
            return this.name == null ? "NA" : this.name + this.size();
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.kni.etl.ketl.smp.MQueue#registerReader(com.kni.etl.ketl.smp.ETLWorker)
         */
        public void registerReader(ETLWorker worker) {
            mParentPartitioner.registerReader(worker);
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.kni.etl.ketl.smp.MQueue#registerWriter(com.kni.etl.ketl.smp.ETLWorker)
         */
        public void registerWriter(ETLWorker worker) {
            mParentPartitioner.registerWriter(worker);
        }

    }

    public ManagedBlockingQueue getTargetSourceQueue(int i) {
        mDestinationQueues[i] = new PartitionTargerManagedBlockingQueue(this, this.mCapacity);
        return mDestinationQueues[i];
    }

    @Override
    public void registerReader(ETLWorker worker) {
        for (int i = 0; i < worker.mInPorts.length; i++) {
            for (int x = 0; x < this.mPartitionKeys.length; x++) {
                if (this.mPartitionKeys[x] == worker.mInPorts[i].getXMLConfig()) {
                    int id = XMLHelper.getAttributeAsInt(this.mPartitionKeys[x].getAttributes(), "PARTITIONKEY", -1);
                    this.mHashOrder[id - 1] = worker.mInPorts[i].getSourcePortIndex();
                }
            }
        }
    }

    @Override
    public String toString() {
        return this.name == null ? "NA" : this.name + this.size();
    }

    @Override
    public void registerWriter(ETLWorker worker) {

    }

    @Override
    public void setName(String arg0) {
        this.name = arg0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.LinkedBlockingQueue#take()
     */
    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.smp.MQueue#take()
     */
    @Override
    public Object take() throws InterruptedException {
        throw new RuntimeException("Method should not be called");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.LinkedBlockingQueue#put(java.lang.Object)
     */
    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.smp.MQueue#put(java.lang.Object)
     */
    @Override
    public void put(Object pO) throws InterruptedException {
        if (pO == com.kni.etl.ketl.smp.ETLWorker.ENDOBJ) {

            if (this.mOutData[0] instanceof SortedList) {
                for (int i = 0; i < this.mDestQueues; i++) {

                    ((SortedList) this.mOutData[i]).releaseAll();
                    if (this.mOutData[i].size() > 0) {
                        Object[][] outData = new Object[((SortedList) this.mOutData[i]).fetchSize()][];
                        this.mOutData[i].toArray(outData);
                        this.mDestinationQueues[i].put(outData);
                    }
                }
            }

            for (int i = 0; i < this.mDestinationQueues.length; i++) {
                this.mDestinationQueues[i].put(pO);
            }
            return;
        }

        Object[][] batch = (Object[][]) pO;

        int size = batch.length;
        for (int r = 0; r < size; r++) {
            Object[] data = batch[r];
            int h = 1;
            for (int i = 0; i < mHashLength; i++) {
                int pos = this.mHashOrder[i];
                h = 31 * h + (data[pos] == null ? 0 : data[pos].hashCode());
            }
            this.mOutData[Math.abs(h % this.mDestQueues)].add(data);
        }

        for (int i = 0; i < this.mDestQueues; i++) {
            if (this.mOutData[i].size() > 0) {

                Object[][] outData = new Object[this.mOutData[i].size()][];
                outData = (Object[][]) this.mOutData[i].toArray(outData);

                if (outData != null)
                    this.mDestinationQueues[i].put(outData);

                if (mSorted == false)
                    this.mOutData[i].clear();

            }
        }
    }

}
