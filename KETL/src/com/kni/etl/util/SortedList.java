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
import java.util.Comparator;
import java.util.List;

final public class SortedList {

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
    
    final private static int indexedBinarySearch(List list, Object key,Comparator comp) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            int mid = (low + high) >> 1;
            Object midVal = list.get(mid);
            int cmp = comp.compare(midVal,key);

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
    
    final public void add(Object arg0) {
        // Search for the non-existent item
        int index = indexedBinarySearch(sortedList, arg0,this.mComparator); // -4

        // Add the non-existent item to the list
        if (index < 0) {
            sortedList.add(-index - 1, arg0);
        }
        else {
            sortedList.add(index, arg0);
        }
    }

    final public Object removeFirst() {
        if (sortedList.isEmpty()) {
            return null;
        }

        return sortedList.remove(0);
    }
}
