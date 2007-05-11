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
/*
 * Created on Jul 20, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.util.Sort;

// TODO: Auto-generated Javadoc
/**
 * The Class ExternalSort.
 */
final public class ExternalSort implements Set {

    // int subSpoolCount = 0;
    /** The pos. */
    int pos;
    
    /** The db. */
    public BufferedObjectStore db;
    
    /** The data. */
    public Object[] data;
    
    /** The current pos. */
    int currentPos;
    
    /** The records. */
    int records = 0;
    
    /** The total records. */
    int totalRecords = 0;
    
    /** The max sort size. */
    int maxSortSize = 1024;
    
    /** The spools. */
    ArrayList spools = new ArrayList();
    
    /** The ma stores. */
    Store[] maStores;
    
    /** The merged spools. */
    ArrayList mergedSpools = new ArrayList();
    
    /** The root. */
    SortedList root;
    
    /** The merge size. */
    int mMergeSize = 128;
    
    /** The read buffer size. */
    int readBufferSize;
    
    /** The max individual read buffer size. */
    int maxIndividualReadBufferSize;
    
    /** The write buffer size. */
    int writeBufferSize;
    
    /** The distinct. */
    boolean mDistinct = false;

    /**
     * The Class BufferedObjectStore.
     */
    final public class BufferedObjectStore {

        /** The stores. */
        ArrayList stores = new ArrayList();
        
        /** The store cnt. */
        int storeCnt = 0;

        /* (non-Javadoc)
         * @see java.lang.Object#finalize()
         */
        @Override
        protected void finalize() throws Throwable {
            if ((this.stores != null) & (this.stores.size() > 0)) {
                this.close();
            }

            super.finalize();
        }

        /**
         * Creates the set.
         * 
         * @param bufferSize the buffer size
         * 
         * @return the store
         * 
         * @throws IOException Signals that an I/O exception has occurred.
         */
        final public Store createSet(int bufferSize) throws IOException {
            Store store = new Store(bufferSize);

            this.stores.add(store);

            return store;
        }

        /**
         * Close.
         * 
         * @throws Exception the exception
         */
        final public void close() throws Exception {
            for (int i = 0; i < this.stores.size(); i++) {
                ((Store) this.stores.get(i)).close();
            }

            this.stores.clear();
        }
    }

    /**
     * Instantiates a new external sort.
     * 
     * @param cmp the cmp
     * @param maxSortSize the max sort size
     * @param mergeSize the merge size
     * @param readBufferSize the read buffer size
     * @param maxIndividualReadBufferSize the max individual read buffer size
     * @param writeBufferSize the write buffer size
     */
    public ExternalSort(Comparator cmp, int maxSortSize, int mergeSize, int readBufferSize,
            int maxIndividualReadBufferSize, int writeBufferSize) {
        super();

        this.db = new BufferedObjectStore();

        this.maxSortSize = maxSortSize;
        this.mMergeSize = mergeSize;
        this.writeBufferSize = writeBufferSize;
        this.readBufferSize = readBufferSize;
        this.maxIndividualReadBufferSize = maxIndividualReadBufferSize;
        this.mComparator = cmp;
        this.data = new Object[maxSortSize];
        this.currentPos = 0;
    }

    /** The last exception. */
    Exception mLastException;

    /* (non-Javadoc)
     * @see java.util.Set#add(java.lang.Object)
     */
    final public boolean add(Object o) {
        this.data[this.currentPos++] = o;

        this.records++;

        if (this.records == this.maxSortSize) {
            this.totalRecords = this.totalRecords + this.records;
            this.records = 0;

            try {
                this.spool();
            } catch (Exception e) {
                throw new SortException(e);
            }
        }

        return true;
    }

    /**
     * Gets the last exception.
     * 
     * @return the last exception
     */
    final public Exception getLastException() {
        return this.mLastException;
    }

    /**
     * Commit.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final public void commit() throws IOException, ClassNotFoundException {
        if (this.records > 0) {
            this.totalRecords = this.totalRecords + this.records;
            this.records = 0;
        }

        if ((this.spools.size() > 0) || (this.mergedSpools.size() > 0)) {
            if (this.currentPos > 0) {
                this.spool();
            }

            if ((this.spools.size() > this.mMergeSize) || (this.mergedSpools.size() > 0)) {
                boolean merging = true;

                while (merging) {
                    this.merge();

                    if (this.spools.size() == 0) {
                        this.spools = this.mergedSpools;

                        if (this.spools.size() < this.mMergeSize) {
                            merging = false;
                        }
                    }
                }
            }

            // release objects
            this.data = null;

            this.prepSpoolsForMergeSortedList();

            this.spools = null;
        }
        else {
            this.currentPos = this.totalRecords;
            Sort.quickSort2(this.data, this.mComparator, 0, this.currentPos - 1);

            if (this.mDistinct) {
                this.dedup();
            }
        }
    }

    /**
     * Dedup.
     */
    final void dedup() {
        Object current = this.data[0];
        ArrayList ar = new ArrayList();

        for (int i = 1; i < this.currentPos; i++) {
            if (this.mComparator.compare(current, this.data[i]) != 0) {
                ar.add(current);
                current = this.data[i];
            }
        }

        ar.add(current);

        if (ar.size() < this.currentPos) {
            ar.toArray(this.data);
            this.currentPos = ar.size();
        }
    }

    /**
     * Prep spools for merge sorted list.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final private void prepSpoolsForMergeSortedList() throws IOException, ClassNotFoundException {
        int nSpools = (this.mMergeSize < this.spools.size()) ? this.mMergeSize : this.spools.size();

        // for each spool supply an equal amount of the buffer
        int iReadBufferSize = this.readBufferSize / nSpools;
        int objectBufferSize = this.maxSortSize / nSpools;

        if (iReadBufferSize > this.maxIndividualReadBufferSize) {
            iReadBufferSize = this.maxIndividualReadBufferSize;
        }

        this.maStores = new Store[nSpools];

        for (int i = 0; i < nSpools; i++) {
            this.maStores[i] = (Store) this.spools.get(i);

            this.maStores[i].start(iReadBufferSize, objectBufferSize);
        }

        this.root = null;

        for (int i = 0; i < nSpools; i++) {
            this.spools.remove(this.maStores[i]);

            if (this.maStores[i].hasNext()) {
                if (this.root == null) {
                    this.root = new SortedList(this.mComparator);
                }

                Object o = this.maStores[i].next();

                this.addToSortedList(o, i);
            }

        }
    }

    /**
     * Adds the to sorted list.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     */
    private void addToSortedList(Object arg0, Object arg1) {
        this.spoolLookup.put(arg0, arg1);
        this.root.add(arg0);
    }

    /** The spool lookup. */
    private HashMap spoolLookup = new HashMap();

    /** The comparator. */
    Comparator mComparator = null;

    /**
     * Spool.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final void spool() throws IOException, ClassNotFoundException {
        // sort data
        Sort.quickSort2(this.data, this.mComparator, 0, this.currentPos - 1);

        // if distinct sort required then dedup
        if (this.mDistinct) {
            this.dedup();
        }

        // create store for result, with complete buffer
        Store store = this.db.createSet(this.writeBufferSize);

        // add data to store
        store.add(this.data, this.currentPos - 1);

        // complete storage
        store.commit();

        // record spool for later use
        this.spools.add(store);

        // if spools >= than merge size then merge
        if (this.spools.size() >= this.mMergeSize) {
            while (this.spools.size() > 0) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Merging");
                this.merge();
            }
        }

        // reset array position
        this.currentPos = 0;
    }

    /**
     * Merge.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final void merge() throws IOException, ClassNotFoundException {
        // if 1 spool left then don't merge it with itself.
        if (this.spools.size() == 1) {
            this.mergedSpools.add(this.spools.get(0));
            this.spools.remove(0);

            return;
        }

        // prep spools for merging
        this.prepSpoolsForMergeSortedList();

        // create output store
        Store store = this.db.createSet(this.writeBufferSize);

        Object o = null;

        // get next from spools and write to new store
        while (!((o = this.getNextFromSpoolsSortedList()) == null)) {
            store.add(o);
        }

        // commit data to the store
        store.commit();

        // record store to merged stores pool
        this.mergedSpools.add(store);

        this.currentPos = 0;
    }

    /** The previous. */
    Object mPrevious = null;

    /**
     * Gets the next from spools sorted list.
     * 
     * @return the next from spools sorted list
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final Object getNextFromSpoolsSortedList() throws IOException, ClassNotFoundException {
        Object first = this.root.removeFirst();

        if (first == null) {
            return null;
        }

        Integer pos = (Integer) this.spoolLookup.remove(first);
        Store spool = this.maStores[pos];

        if (spool.hasNext()) {
            Object o = spool.next();
            this.addToSortedList(o, pos);
        }
        else {
            spool.close();
        }

        return first;
    }

    /**
     * Close.
     * 
     * @throws Exception the exception
     */
    final public void close() throws Exception {
        this.db.close();
    }

    /** The commit pending. */
    boolean commitPending = true;

    /**
     * Gets the next.
     * 
     * @return the next
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final public Object getNext() throws IOException, ClassNotFoundException {
        if (this.commitPending) {
            this.commitPending = false;
            this.commit();
        }

        // if no spools then read directory from memory
        if (this.maStores == null) {
            if (this.pos == this.currentPos) {
                return null;
            }

            return this.data[this.pos++];
        }

        // if distinct results not required then pull next value
        if (this.mDistinct == false) {
            return this.getNextFromSpoolsSortedList();
        }

        Object current = this.getNextFromSpoolsSortedList();

        if ((this.mPrevious == null) || (current == null)) {
            this.mPrevious = current;

            return current;
        }

        while (true) {
            if (this.mComparator.compare(current, this.mPrevious) == 0) {
                current = this.getNextFromSpoolsSortedList();

                if (current == null)
                    return null;
            }
            else {
                this.mPrevious = current;

                return current;
            }
        }
    }

    /* (non-Javadoc)
     * @see java.util.Set#size()
     */
    final public int size() {
        return this.records;
    }

    /* (non-Javadoc)
     * @see java.util.Set#isEmpty()
     */
    final public boolean isEmpty() {
        return (this.records == 0) ? true : false;
    }

    /* (non-Javadoc)
     * @see java.util.Set#contains(java.lang.Object)
     */
    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see java.util.Set#iterator()
     */
    public Iterator iterator() {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#toArray()
     */
    public Object[] toArray() {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#toArray(T[])
     */
    public Object[] toArray(Object[] a) {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#remove(java.lang.Object)
     */
    public boolean remove(Object o) {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#containsAll(java.util.Collection)
     */
    public boolean containsAll(Collection c) {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#addAll(java.util.Collection)
     */
    public boolean addAll(Collection c) {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#retainAll(java.util.Collection)
     */
    public boolean retainAll(Collection c) {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#removeAll(java.util.Collection)
     */
    public boolean removeAll(Collection c) {
        throw new RuntimeException();
    }

    /* (non-Javadoc)
     * @see java.util.Set#clear()
     */
    public void clear() {
        throw new RuntimeException();
    }

}
