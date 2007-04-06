/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 18, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.util.Comparator;
import java.util.List;

import com.kni.etl.util.SortedList;

final class ManagedBlockingQueueImpl extends ManagedBlockingQueue {

    private static final long serialVersionUID = 1L;
    private int writingThreads = 0, readingThreads = 0;
    private String name;

    /**
     * @param pCapacity
     */
    public ManagedBlockingQueueImpl(int pCapacity) {
        super(pCapacity);
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
    public synchronized void registerReader(ETLWorker worker) {
        this.readingThreads++;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.smp.MQueue#registerWriter(com.kni.etl.ketl.smp.ETLWorker)
     */
    public synchronized void registerWriter(ETLWorker worker) {
        this.writingThreads++;
    }

    private List<Object[]> bufferedSort;

    public void setSortComparator(Comparator arg0){
        this.bufferedSort=new SortedList<Object[]>(arg0);
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
            synchronized (this) {
                writingThreads--;
                if (writingThreads == 0) {

                    if (this.bufferedSort != null) {
                        Object[][] batch;
                        ((SortedList)this.bufferedSort).releaseAll();
                        while ((batch = this.bufferedSort.toArray(new Object[((SortedList) this.bufferedSort).fetchSize()][])) != null)
                            super.put(batch);
                    }

                    for (int i = 0; i < this.readingThreads; i++) {
                        super.put(pO);
                    }
                }
            }

            return;
        }

        if (this.bufferedSort != null) {
            Object[][] batch = (Object[][]) pO;

            int size = batch.length;
            
            for(int i=0;i<size;i++)
                this.bufferedSort.add(batch[i]);
            
            batch = (Object[][]) this.bufferedSort.toArray(batch);

            if (batch == null)
                return;
        }

        super.put(pO);
    }

}