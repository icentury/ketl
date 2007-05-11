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
 * Created on Aug 22, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

// TODO: Auto-generated Javadoc
/**
 * The Class SortedList.
 */
final public class SortedList<E> implements List<E> {

    /** The comparator. */
    Comparator mComparator = null;
    // Create a list with an ordered list of items
    /** The sorted list. */
    List sortedList = new ArrayList();

    /**
     * Instantiates a new sorted list.
     */
    public SortedList() {
        super();
    }

    /**
     * Instantiates a new sorted list.
     * 
     * @param comparator the comparator
     */
    public SortedList(Comparator comparator) {
        super();
        this.mComparator = comparator;
    }

    /**
     * Indexed binary search.
     * 
     * @param list the list
     * @param key the key
     * 
     * @return the int
     */
    final private static int indexedBinarySearch(List list, Comparable key) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            int mid = (low + high) >> 1;
            Comparable midVal = (Comparable) list.get(mid);
            int cmp = midVal.compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            }
            else if (cmp > 0) {
                high = mid - 1;
            }
            else {
                return mid; // key found
            }
        }

        return -(low + 1); // key not found
    }

    /**
     * Indexed binary search.
     * 
     * @param list the list
     * @param key the key
     * @param comp the comp
     * 
     * @return the int
     */
    final private static int indexedBinarySearch(List list, Object key, Comparator comp) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            int mid = (low + high) >> 1;
            Object midVal = list.get(mid);
            int cmp = comp.compare(midVal, key);

            if (cmp < 0) {
                low = mid + 1;
            }
            else if (cmp > 0) {
                high = mid - 1;
            }
            else {
                return mid; // key found
            }
        }

        return -(low + 1); // key not found
    }

    /**
     * Add.
     * 
     * @param arg0 the arg0
     */
    final public void add(Comparable arg0) {
        // Search for the non-existent item
        int index = SortedList.indexedBinarySearch(this.sortedList, arg0); // -4

        // Add the non-existent item to the list
        if (index < 0) {
            this.sortedList.add(-index - 1, arg0);
        }
        else {
            this.sortedList.add(index, arg0);
        }
    }

    /* (non-Javadoc)
     * @see java.util.List#add(java.lang.Object)
     */
    final public boolean add(Object arg0) {
        // Search for the non-existent item
        int index = SortedList.indexedBinarySearch(this.sortedList, arg0, this.mComparator); // -4

        // Add the non-existent item to the list
        if (index < 0) {
            this.sortedList.add(-index - 1, arg0);
        }
        else {
            this.sortedList.add(index, arg0);
        }

        return true;
    }

    /**
     * Removes the first.
     * 
     * @return the object
     */
    final public Object removeFirst() {
        if (this.sortedList.isEmpty()) {
            return null;
        }

        return this.sortedList.remove(0);
    }

    /** The size. */
    private int size = 1;
    
    /** The min buffer size. */
    private int minBufferSize = 25000;

    /**
     * Release all.
     */
    public void releaseAll() {
        this.minBufferSize = 0;
    }

    /* (non-Javadoc)
     * @see java.util.List#add(int, java.lang.Object)
     */
    public void add(int index, Object element) {
        this.add(element);
    }

    /* (non-Javadoc)
     * @see java.util.List#addAll(java.util.Collection)
     */
    public boolean addAll(Collection c) {

        for (Object o : c)
            this.add(o);
        return true;
    }

    /* (non-Javadoc)
     * @see java.util.List#addAll(int, java.util.Collection)
     */
    public boolean addAll(int index, Collection c) {
        return this.addAll(c);
    }

    /* (non-Javadoc)
     * @see java.util.List#clear()
     */
    public void clear() {
        this.sortedList.clear();

    }

    /* (non-Javadoc)
     * @see java.util.List#contains(java.lang.Object)
     */
    public boolean contains(Object o) {
        return this.sortedList.contains(o);

    }

    /* (non-Javadoc)
     * @see java.util.List#containsAll(java.util.Collection)
     */
    public boolean containsAll(Collection c) {
        return this.sortedList.containsAll(c);
    }

    /* (non-Javadoc)
     * @see java.util.List#get(int)
     */
    public E get(int index) {
        return (E) this.sortedList.get(index);
    }

    /* (non-Javadoc)
     * @see java.util.List#indexOf(java.lang.Object)
     */
    public int indexOf(Object o) {
        return this.sortedList.indexOf(o);
    }

    /* (non-Javadoc)
     * @see java.util.List#isEmpty()
     */
    public boolean isEmpty() {
        return this.sortedList.isEmpty();
    }

    /* (non-Javadoc)
     * @see java.util.List#iterator()
     */
    public Iterator iterator() {
        return this.sortedList.iterator();
    }

    /* (non-Javadoc)
     * @see java.util.List#lastIndexOf(java.lang.Object)
     */
    public int lastIndexOf(Object o) {
        return this.sortedList.lastIndexOf(o);
    }

    /* (non-Javadoc)
     * @see java.util.List#listIterator()
     */
    public ListIterator listIterator() {
        return this.listIterator();
    }

    /* (non-Javadoc)
     * @see java.util.List#listIterator(int)
     */
    public ListIterator listIterator(int index) {
        return this.listIterator(index);
    }

    /* (non-Javadoc)
     * @see java.util.List#remove(java.lang.Object)
     */
    public boolean remove(Object o) {
        return this.remove(o);
    }

    /* (non-Javadoc)
     * @see java.util.List#remove(int)
     */
    public E remove(int index) {
        return this.remove(index);
    }

    /* (non-Javadoc)
     * @see java.util.List#removeAll(java.util.Collection)
     */
    public boolean removeAll(Collection c) {
        return this.removeAll(c);
    }

    /* (non-Javadoc)
     * @see java.util.List#retainAll(java.util.Collection)
     */
    public boolean retainAll(Collection c) {
        return this.retainAll(c);
    }

    /* (non-Javadoc)
     * @see java.util.List#set(int, java.lang.Object)
     */
    public E set(int index, Object element) {
        return this.set(index, element);
    }

    /* (non-Javadoc)
     * @see java.util.List#size()
     */
    public int size() {
        return this.sortedList.size();
    }

    /* (non-Javadoc)
     * @see java.util.List#subList(int, int)
     */
    public List subList(int fromIndex, int toIndex) {
        return this.sortedList.subList(fromIndex, toIndex);
    }

    /* (non-Javadoc)
     * @see java.util.List#toArray()
     */
    public Object[] toArray() {

        int currentSize = this.sortedList.size();

        if (currentSize == 0 || ((currentSize - this.minBufferSize) < this.size))
            return null;

        if (this.size > currentSize)
            this.size = currentSize;
        Object[][] res = new Object[this.size][];

        for (int i = 0; i < this.size; i++) {
            res[i] = (Object[]) this.removeFirst();
        }
        return res;

    }

    /* (non-Javadoc)
     * @see java.util.List#toArray(T[])
     */
    public <T> T[] toArray(T[] batch) {
        int currentSize = this.sortedList.size();
        int readSize = batch.length;
        if ((currentSize - this.minBufferSize) < this.size)
            return null;

        if (currentSize < readSize) {
            batch = (T[]) java.lang.reflect.Array.newInstance(batch.getClass().getComponentType(), currentSize);
            readSize = currentSize;
        }
        for (int i = 0; i < readSize; i++) {
            batch[i] = (T) this.removeFirst();
        }
        return batch;
    }

    /**
     * Fetch size.
     * 
     * @return the int
     */
    public int fetchSize() {
        if ((this.sortedList.size() - this.minBufferSize) < this.size)
            return 0;

        return (this.sortedList.size() - this.minBufferSize) > 5000 ? 256
                : (this.sortedList.size() - this.minBufferSize);
    }

}
