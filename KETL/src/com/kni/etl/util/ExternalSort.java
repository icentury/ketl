/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

final public class ExternalSort implements Set {

    // int subSpoolCount = 0;
    int pos;
    public BufferedObjectStore db;
    public Object[] data;
    int currentPos;
    int records = 0;
    int totalRecords = 0;
    int maxSortSize = 1024;
    ArrayList spools = new ArrayList();
    Store[] maStores;
    ArrayList mergedSpools = new ArrayList();
    SortedList root;
    int mMergeSize = 128;
    int readBufferSize;
    int maxIndividualReadBufferSize;
    int writeBufferSize;
    boolean mDistinct = false;

    final public class BufferedObjectStore {

        ArrayList stores = new ArrayList();
        int storeCnt = 0;

        @Override
        protected void finalize() throws Throwable {
            if ((this.stores != null) & (this.stores.size() > 0)) {
                this.close();
            }

            super.finalize();
        }

        final public Store createSet(int bufferSize) throws IOException {
            Store store = new Store(bufferSize);

            this.stores.add(store);

            return store;
        }

        final public void close() throws Exception {
            for (int i = 0; i < this.stores.size(); i++) {
                ((Store) this.stores.get(i)).close();
            }

            this.stores.clear();
        }
    }

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

    Exception mLastException;

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

    final public Exception getLastException() {
        return this.mLastException;
    }

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

    private void addToSortedList(Object arg0, Object arg1) {
        this.spoolLookup.put(arg0, arg1);
        this.root.add(arg0);
    }

    private HashMap spoolLookup = new HashMap();

    Comparator mComparator = null;

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

    Object mPrevious = null;

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

    final public void close() throws Exception {
        this.db.close();
    }

    boolean commitPending = true;

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

    final public int size() {
        return this.records;
    }

    final public boolean isEmpty() {
        return (this.records == 0) ? true : false;
    }

    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    public Iterator iterator() {
        throw new RuntimeException();
    }

    public Object[] toArray() {
        throw new RuntimeException();
    }

    public Object[] toArray(Object[] a) {
        throw new RuntimeException();
    }

    public boolean remove(Object o) {
        throw new RuntimeException();
    }

    public boolean containsAll(Collection c) {
        throw new RuntimeException();
    }

    public boolean addAll(Collection c) {
        throw new RuntimeException();
    }

    public boolean retainAll(Collection c) {
        throw new RuntimeException();
    }

    public boolean removeAll(Collection c) {
        throw new RuntimeException();
    }

    public void clear() {
        throw new RuntimeException();
    }

}
