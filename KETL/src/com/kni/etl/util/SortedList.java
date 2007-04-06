/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

final public class SortedList<E> implements List<E> {

    Comparator mComparator = null;
    // Create a list with an ordered list of items
    List sortedList = new ArrayList();

    public SortedList() {
        super();
    }

    public SortedList(Comparator comparator) {
        super();
        mComparator = comparator;
    }

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

    final public void add(Comparable arg0) {
        // Search for the non-existent item
        int index = indexedBinarySearch(sortedList, arg0); // -4

        // Add the non-existent item to the list
        if (index < 0) {
            sortedList.add(-index - 1, arg0);
        }
        else {
            sortedList.add(index, arg0);
        }
    }

    final public boolean add(Object arg0) {
        // Search for the non-existent item
        int index = indexedBinarySearch(sortedList, arg0, this.mComparator); // -4

        // Add the non-existent item to the list
        if (index < 0) {
            sortedList.add(-index - 1, arg0);
        }
        else {
            sortedList.add(index, arg0);
        }

        return true;
    }

    final public Object removeFirst() {
        if (sortedList.isEmpty()) {
            return null;
        }

        return sortedList.remove(0);
    }

    private int size = 1;
    private int minBufferSize = 25000;

    public void releaseAll() {
        minBufferSize = 0;
    }

    public void add(int index, Object element) {
        this.add(element);
    }

    public boolean addAll(Collection c) {

        for (Object o : c)
            this.add(o);
        return true;
    }

    public boolean addAll(int index, Collection c) {
        return this.addAll(c);
    }

    public void clear() {
        this.sortedList.clear();

    }

    public boolean contains(Object o) {
        return this.sortedList.contains(o);

    }

    public boolean containsAll(Collection c) {
        return this.sortedList.containsAll(c);
    }

    public E get(int index) {
        return (E) this.sortedList.get(index);
    }

    public int indexOf(Object o) {
        return this.sortedList.indexOf(o);
    }

    public boolean isEmpty() {
        return this.sortedList.isEmpty();
    }

    public Iterator iterator() {
        return this.sortedList.iterator();
    }

    public int lastIndexOf(Object o) {
        return this.sortedList.lastIndexOf(o);
    }

    public ListIterator listIterator() {
        return this.listIterator();
    }

    public ListIterator listIterator(int index) {
        return this.listIterator(index);
    }

    public boolean remove(Object o) {
        return this.remove(o);
    }

    public E remove(int index) {
        return this.remove(index);
    }

    public boolean removeAll(Collection c) {
        return this.removeAll(c);
    }

    public boolean retainAll(Collection c) {
        return this.retainAll(c);
    }

    public E set(int index, Object element) {
        return this.set(index, element);
    }

    public int size() {
        return this.sortedList.size();
    }

    public List subList(int fromIndex, int toIndex) {
        return this.sortedList.subList(fromIndex, toIndex);
    }

    public Object[] toArray() {

        int currentSize = sortedList.size();

        if (currentSize == 0 || ((currentSize - minBufferSize) < size))
            return null;

        if (this.size > currentSize)
            this.size = currentSize;
        Object[][] res = new Object[size][];

        for (int i = 0; i < size; i++) {
            res[i] = (Object[]) this.removeFirst();
        }
        return res;

    }

    public <T> T[] toArray(T[] batch) {
        int currentSize = sortedList.size();
        int readSize = batch.length;
        if ((currentSize - minBufferSize) < size)
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

    public int fetchSize() {
        if ((sortedList.size() - minBufferSize) < size)
            return 0;

        return (sortedList.size() - minBufferSize) > 5000 ? 256 : (sortedList.size() - minBufferSize);
    }

}
