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
package com.kni.util;

import java.util.Comparator;

// TODO: Auto-generated Javadoc
/**
 * A class that contains several sorting routines, implemented as static methods. Arrays are rearranged with smallest
 * item first, using compareTo.
 * 
 * @author Mark Allen Weiss
 */
public final class Sort {

    /**
     * Simple insertion sort.
     * 
     * @param a an array of Comparable items.
     */
    public static void insertionSort(Comparable[] a) {
        int j;

        /* 1 */for (int p = 1; p < a.length; p++) {
            /* 2 */Comparable tmp = a[p];

            /* 3 */for (j = p; (j > 0) && (tmp.compareTo(a[j - 1]) < 0); j--)

                /* 4 */a[j] = a[j - 1];

            /* 5 */a[j] = tmp;
        }
    }

    /**
     * Shellsort, using Shell's (poor) increments.
     * 
     * @param a an array of Comparable items.
     * @param left the left
     * @param right the right
     */
    public static void shellsort(Comparable[] a, int left, int right) {
        int j;

        /* 1 */for (int gap = right / 2; gap > 0; gap /= 2)

            /* 2 */for (int i = gap; i < right; i++) {
                /* 3 */Comparable tmp = a[i];

                /* 4 */for (j = i; (j >= gap) && (tmp.compareTo(a[j - gap]) < 0); j -= gap)

                    /* 5 */a[j] = a[j - gap];

                /* 6 */a[j] = tmp;
            }
    }

    /**
     * Standard heapsort.
     * 
     * @param a an array of Comparable items.
     */
    public static void heapsort(Comparable[] a) {
        /* 1 */for (int i = a.length / 2; i >= 0; i--)
            /* buildHeap */

            /* 2 */Sort.percDown(a, i, a.length);

        /* 3 */for (int i = a.length - 1; i > 0; i--) {
            /* 4 */Sort.swapReferences(a, 0, i); /* deleteMax */

            /* 5 */Sort.percDown(a, 0, i);
        }
    }

    /**
     * Internal method for heapsort.
     * 
     * @param i the index of an item in the heap.
     * 
     * @return the index of the left child.
     */
    private static int leftChild(int i) {
        return (2 * i) + 1;
    }

    /**
     * Internal method for heapsort that is used in deleteMax and buildHeap.
     * 
     * @param a an array of Comparable items.
     * @param i the i
     * @param n the n
     * 
     * @index i the position from which to percolate down.
     * @int n the logical size of the binary heap.
     */
    private static void percDown(Comparable[] a, int i, int n) {
        int child;
        Comparable tmp;

        /* 1 */for (tmp = a[i]; Sort.leftChild(i) < n; i = child) {
            /* 2 */child = Sort.leftChild(i);

            /* 3 */if ((child != (n - 1)) && (a[child].compareTo(a[child + 1]) < 0)) {
                /* 4 */child++;
            }

            /* 5 */if (tmp.compareTo(a[child]) < 0) {
                /* 6 */a[i] = a[child];
            }
            else {
                /* 7 */break;
            }
        }

        /* 8 */a[i] = tmp;
    }

    /**
     * Mergesort algorithm.
     * 
     * @param a an array of Comparable items.
     */
    public static void mergeSort(Comparable[] a) {
        Comparable[] tmpArray = new Comparable[a.length];

        Sort.mergeSort(a, tmpArray, 0, a.length - 1);
    }

    /**
     * Merge sort.
     * 
     * @param a the a
     * @param left the left
     * @param right the right
     */
    public static void mergeSort(Comparable[] a, int left, int right) {
        Comparable[] tmpArray = new Comparable[right + 1];

        Sort.mergeSort(a, tmpArray, left, right);
    }

    /**
     * Internal method that makes recursive calls.
     * 
     * @param a an array of Comparable items.
     * @param tmpArray an array to place the merged result.
     * @param left the left-most index of the subarray.
     * @param right the right-most index of the subarray.
     */
    private static void mergeSort(Comparable[] a, Comparable[] tmpArray, int left, int right) {
        if (left < right) {
            int center = (left + right) / 2;
            Sort.mergeSort(a, tmpArray, left, center);
            Sort.mergeSort(a, tmpArray, center + 1, right);
            Sort.merge(a, tmpArray, left, center + 1, right);
        }
    }

    /**
     * Internal method that merges two sorted halves of a subarray.
     * 
     * @param a an array of Comparable items.
     * @param tmpArray an array to place the merged result.
     * @param leftPos the left-most index of the subarray.
     * @param rightPos the index of the start of the second half.
     * @param rightEnd the right-most index of the subarray.
     */
    private static void merge(Comparable[] a, Comparable[] tmpArray, int leftPos, int rightPos, int rightEnd) {
        int leftEnd = rightPos - 1;
        int tmpPos = leftPos;
        int numElements = rightEnd - leftPos + 1;

        // Main loop
        while ((leftPos <= leftEnd) && (rightPos <= rightEnd))

            if (a[leftPos].compareTo(a[rightPos]) <= 0) {
                tmpArray[tmpPos++] = a[leftPos++];
            }
            else {
                tmpArray[tmpPos++] = a[rightPos++];
            }

        while (leftPos <= leftEnd)
            // Copy rest of first half

            tmpArray[tmpPos++] = a[leftPos++];

        while (rightPos <= rightEnd)
            // Copy rest of right half

            tmpArray[tmpPos++] = a[rightPos++];

        // Copy tmpArray back
        for (int i = 0; i < numElements; i++, rightEnd--)
            a[rightEnd] = tmpArray[rightEnd];
    }

    /**
     * Quicksort algorithm.
     * 
     * @param a an array of Comparable items.
     */
    public static void quicksort(Comparable[] a) {
        Sort.quicksort(a, 0, a.length - 1);
    }

    /** The Constant CUTOFF. */
    private static final int CUTOFF = 3;

    /**
     * Method to swap to elements in an array.
     * 
     * @param a an array of objects.
     * @param index1 the index of the first object.
     * @param index2 the index of the second object.
     */
    public static final void swapReferences(Object[] a, int index1, int index2) {
        Object tmp = a[index1];
        a[index1] = a[index2];
        a[index2] = tmp;
    }

    /**
     * Return median of left, center, and right. Order these and hide the pivot.
     * 
     * @param a the a
     * @param left the left
     * @param right the right
     * 
     * @return the comparable
     */
    private static Comparable median3(Comparable[] a, int left, int right) {
        int center = (left + right) / 2;

        if (a[center].compareTo(a[left]) < 0) {
            Sort.swapReferences(a, left, center);
        }

        if (a[right].compareTo(a[left]) < 0) {
            Sort.swapReferences(a, left, right);
        }

        if (a[right].compareTo(a[center]) < 0) {
            Sort.swapReferences(a, center, right);
        }

        // Place pivot at position right - 1
        Sort.swapReferences(a, center, right - 1);

        return a[right - 1];
    }

    /**
     * Internal quicksort method that makes recursive calls. Uses median-of-three partitioning and a cutoff of 10.
     * 
     * @param a an array of Comparable items.
     * @param left the left-most index of the subarray.
     * @param right the right-most index of the subarray.
     */
    public static void quicksort(Comparable[] a, int left, int right) {
        /* 1 */if ((left + Sort.CUTOFF) <= right) {
            /* 2 */Comparable pivot = Sort.median3(a, left, right);

            // Begin partitioning

            /* 3 */int i = left;

            // Begin partitioning

            /* 3 */int j = right - 1;

            /* 4 */for (;;) {
                /* 5 */while (a[++i].compareTo(pivot) < 0) {
                }

                /* 6 */while (a[--j].compareTo(pivot) > 0) {
                }

                /* 7 */if (i < j) {
                    /* 8 */Sort.swapReferences(a, i, j);
                }
                else {
                    /* 9 */break;
                }
            }

            /* 10 */Sort.swapReferences(a, i, right - 1); // Restore pivot

            /* 11 */Sort.quicksort(a, left, i - 1); // Sort small elements

            /* 12 */Sort.quicksort(a, i + 1, right); // Sort large elements
        }
        else { // Do an insertion sort on the subarray

            /* 13 */Sort.insertionSort(a, left, right);
        }
    }

    /**
     * Internal insertion sort routine for subarrays that is used by quicksort.
     * 
     * @param a an array of Comparable items.
     * @param left the left-most index of the subarray.
     * @param right the right-most index of the subarray.
     */
    public static void insertionSort(Comparable[] a, int left, int right) {
        for (int p = left + 1; p <= right; p++) {
            Comparable tmp = a[p];
            int j;

            for (j = p; (j > left) && (tmp.compareTo(a[j - 1]) < 0); j--)
                a[j] = a[j - 1];

            a[j] = tmp;
        }
    }

    /**
     * Quick selection algorithm. Places the kth smallest item in a[k-1].
     * 
     * @param a an array of Comparable items.
     * @param k the desired rank (1 is minimum) in the entire array.
     */
    public static void quickSelect(Comparable[] a, int k) {
        Sort.quickSelect(a, 0, a.length - 1, k);
    }

    /**
     * Internal selection method that makes recursive calls. Uses median-of-three partitioning and a cutoff of 10.
     * Places the kth smallest item in a[k-1].
     * 
     * @param a an array of Comparable items.
     * @param left the left-most index of the subarray.
     * @param right the right-most index of the subarray.
     * @param k the desired index (1 is minimum) in the entire array.
     */
    public static void quickSelect(Comparable[] a, int left, int right, int k) {
        /* 1 */if ((left + Sort.CUTOFF) <= right) {
            /* 2 */Comparable pivot = Sort.median3(a, left, right);

            // Begin partitioning

            /* 3 */int i = left;

            // Begin partitioning

            /* 3 */int j = right - 1;

            /* 4 */for (;;) {
                /* 5 */while (a[++i].compareTo(pivot) < 0) {
                }

                /* 6 */while (a[--j].compareTo(pivot) > 0) {
                }

                /* 7 */if (i < j) { // Do an insertion sort on the subarray

                    /* 8 */Sort.swapReferences(a, i, j);
                }
                else {
                    /* 9 */break;
                }
            }

            /* 10 */Sort.swapReferences(a, i, right - 1); // Restore pivot

            /* 11 */if (k <= i) {
                /* 12 */Sort.quickSelect(a, left, i - 1, k);
            }

            /* 13 */else if (k > (i + 1)) {
                /* 14 */Sort.quickSelect(a, i + 1, right, k);
            }
        }
        else { // Do an insertion sort on the subarray

            /* 15 */Sort.insertionSort(a, left, right);
        }
    }

    /**
     * Quick sort2.
     * 
     * @param c the c
     * @param left the left
     * @param right the right
     */
    public static void quickSort2(Comparable[] c, int left, int right) {
        int i;
        int j;
        int stack_pointer = -1;
        int[] stack = new int[128];
        Comparable swap;
        Comparable temp;

        while (true) {
            if ((right - left) <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = c[j];
                    i = j - 1;

                    while ((i >= left) && (c[i].compareTo(swap) > 0))
                        c[i + 1] = c[i--];

                    c[i + 1] = swap;
                }

                if (stack_pointer == -1) {
                    break;
                }

                right = stack[stack_pointer--];
                left = stack[stack_pointer--];
            }
            else {
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;
                swap = c[median];
                c[median] = c[i];
                c[i] = swap;

                /* make sure: c[left] <= c[left+1] <= c[right] */
                if (c[left].compareTo(c[right]) > 0) {
                    swap = c[left];
                    c[left] = c[right];
                    c[right] = swap;
                }

                if (c[i].compareTo(c[right]) > 0) {
                    swap = c[i];
                    c[i] = c[right];
                    c[right] = swap;
                }

                if (c[left].compareTo(c[i]) > 0) {
                    swap = c[left];
                    c[left] = c[i];
                    c[i] = swap;
                }

                temp = c[i];

                while (true) {
                    do
                        i++;
                    while (c[i].compareTo(temp) < 0);

                    do
                        j--;
                    while (c[j].compareTo(temp) > 0);

                    if (j < i) {
                        break;
                    }

                    swap = c[i];
                    c[i] = c[j];
                    c[j] = swap;
                }

                c[left + 1] = c[j];
                c[j] = temp;

                if ((right - i + 1) >= (j - left)) {
                    stack[++stack_pointer] = i;
                    stack[++stack_pointer] = right;
                    right = j - 1;
                }
                else {
                    stack[++stack_pointer] = left;
                    stack[++stack_pointer] = j - 1;
                    left = i;
                }
            }
        }
    }

    /**
     * Quick sort2.
     * 
     * @param c the c
     * @param cmp the cmp
     * @param left the left
     * @param right the right
     */
    public static void quickSort2(Object[] c, Comparator cmp, int left, int right) {
        int i;
        int j;
        int stack_pointer = -1;
        int[] stack = new int[128];
        Object swap;
        Object temp;

        while (true) {
            if ((right - left) <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = c[j];
                    i = j - 1;

                    while ((i >= left) && (cmp.compare(c[i], swap) > 0))
                        c[i + 1] = c[i--];

                    c[i + 1] = swap;
                }

                if (stack_pointer == -1) {
                    break;
                }

                right = stack[stack_pointer--];
                left = stack[stack_pointer--];
            }
            else {
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;
                swap = c[median];
                c[median] = c[i];
                c[i] = swap;

                /* make sure: c[left] <= c[left+1] <= c[right] */
                if (cmp.compare(c[left], c[right]) > 0) {
                    swap = c[left];
                    c[left] = c[right];
                    c[right] = swap;
                }

                if (cmp.compare(c[i], c[right]) > 0) {
                    swap = c[i];
                    c[i] = c[right];
                    c[right] = swap;
                }

                if (cmp.compare(c[left], c[i]) > 0) {
                    swap = c[left];
                    c[left] = c[i];
                    c[i] = swap;
                }

                temp = c[i];

                while (true) {
                    do
                        i++;
                    while (cmp.compare(c[i], temp) < 0);

                    do
                        j--;
                    while (cmp.compare(c[j], temp) > 0);

                    if (j < i) {
                        break;
                    }

                    swap = c[i];
                    c[i] = c[j];
                    c[j] = swap;
                }

                c[left + 1] = c[j];
                c[j] = temp;

                if ((right - i + 1) >= (j - left)) {
                    stack[++stack_pointer] = i;
                    stack[++stack_pointer] = right;
                    right = j - 1;
                }
                else {
                    stack[++stack_pointer] = left;
                    stack[++stack_pointer] = j - 1;
                    left = i;
                }
            }
        }
    }
}
