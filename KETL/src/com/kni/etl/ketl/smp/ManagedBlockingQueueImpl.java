/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 18, 2006
 * 
 */
package com.kni.etl.ketl.smp;


final class ManagedBlockingQueueImpl extends ManagedBlockingQueue {

    private static final long serialVersionUID = 1L;
    private int writingThreads = 0, readingThreads = 0;
    private String name;

    /**
     * @param pCapacity
     * @param pName TODO
     */
    public ManagedBlockingQueueImpl(int pCapacity) {
        super(pCapacity);

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.MQueue#setName(java.lang.String)
     */
    public void setName(String arg0) {
        this.name = arg0;
    }

    public String toString() {
        return this.name == null ? "NA" : this.name + this.size();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.MQueue#registerReader(com.kni.etl.ketl.smp.ETLWorker)
     */
    public void registerReader(ETLWorker worker) {
        this.readingThreads++;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.MQueue#registerWriter(com.kni.etl.ketl.smp.ETLWorker)
     */
    public void registerWriter(ETLWorker worker) {
        this.writingThreads++;
    }

   
    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.LinkedBlockingQueue#take()
     */
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.MQueue#take()
     */
    @Override
    public Object take() throws InterruptedException {
        Object o = super.take();
        /*
         * if (o == com.kni.etl.ketl.smp.ETLWorker.ENDOBJ) { writingThreads--; if (writingThreads == 0) { return o; }
         * return this.take(); }
         */
        return o;

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.LinkedBlockingQueue#put(java.lang.Object)
     */
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.MQueue#put(java.lang.Object)
     */
    @Override
    public void put(Object pO) throws InterruptedException {
        if (pO == com.kni.etl.ketl.smp.ETLWorker.ENDOBJ) {
            writingThreads--;
            if (writingThreads == 0) {
                for (int i = 0; i < this.readingThreads; i++) {
                    super.put(pO);
                }
            }
            return;
        }
        super.put(pO);
    }

}