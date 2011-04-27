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

import java.util.Comparator;
import java.util.List;

import com.kni.etl.util.SortedList;

// TODO: Auto-generated Javadoc
/**
 * The Class ManagedBlockingQueueImpl.
 */
final class ManagedBlockingQueueImpl extends ManagedBlockingQueue {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The reading threads. */
    private int writingThreads = 0, readingThreads = 0;
    
    /** The name. */
    private String name;

    /**
     * The Constructor.
     * 
     * @param pCapacity the capacity
     */
    public ManagedBlockingQueueImpl(int pCapacity) {
        super(pCapacity);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.smp.MQueue#setName(java.lang.String)
     */
    @Override
    public void setName(String arg0) {
        this.name = arg0;
    }
    
    public String getName() {
    	return this.name;
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.LinkedBlockingQueue#toString()
     */
    @Override
    public String toString() {
        return this.name == null ? "NA" : this.name + this.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.smp.MQueue#registerReader(com.kni.etl.ketl.smp.ETLWorker)
     */
    @Override
    public synchronized void registerReader(ETLWorker worker) {
        this.readingThreads++;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.smp.MQueue#registerWriter(com.kni.etl.ketl.smp.ETLWorker)
     */
    @Override
    public synchronized void registerWriter(ETLStats worker) {
        this.writingThreads++;
    }

    /** The buffered sort. */
    private List<Object[]> bufferedSort;

    /**
     * Sets the sort comparator.
     * 
     * @param arg0 the new sort comparator
     */
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
                this.writingThreads--;
                if (this.writingThreads == 0) {

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
            
            batch = this.bufferedSort.toArray(batch);

            if (batch == null)
                return;
        }

        super.put(pO);
    }

}