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
 * Created on Mar 18, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.util;

import java.util.Collection;
import java.util.Iterator;

// TODO: Auto-generated Javadoc
/**
 * The Class ResizingArray.
 * 
 * @author nwakefield Creation Date: Mar 18, 2003
 */
public class ResizingArray implements Collection {

    /** The DEFAUL t_ SIZE. */
    private static int DEFAULT_SIZE = 5;
    
    /** The a free elements. */
    private int[] aFreeElements;
    
    /** The a objects. */
    private Object[] aObjects;

    /** The i max released element. */
    private int iMaxReleasedElement = -1;
    
    /** The i max size. */
    private int iMaxSize = ResizingArray.DEFAULT_SIZE;
    
    /** The i used elements. */
    private int iUsedElements = 0;
    
    /** The i next end elements. */
    private int iNextEndElements = 0;

    /**
     * Instantiates a new resizing array.
     */
    public ResizingArray() {
        super();
        this.aObjects = new Object[ResizingArray.DEFAULT_SIZE];
        this.aFreeElements = new int[ResizingArray.DEFAULT_SIZE];
    }

    /**
     * Instantiates a new resizing array.
     * 
     * @param defaultSize the default size
     */
    public ResizingArray(int defaultSize) {
        super();

        this.aObjects = new Object[defaultSize];
        this.aFreeElements = new int[defaultSize];
        this.iMaxSize = defaultSize;
    }

    /**
     * The main method.
     * 
     * @param args the arguments
     */
    public static void main(String[] args) {
        ResizingArray aResize = new ResizingArray();
        Integer test = new Integer(0);

        aResize.add(new Integer(1));
        aResize.add(new Integer(2));
        aResize.add(new Integer(3));
        aResize.add(new Integer(4));
        aResize.add(new Integer(5));
        aResize.add(new Integer(6));
        aResize.add(new Integer(7));
        aResize.add(new Integer(8));
        aResize.add(new Integer(9));
        aResize.add(new Integer(10));
        aResize.add(test);
        aResize.add(new Integer(11));

        aResize.remove(2);
        aResize.remove(4);
        aResize.remove(8);
        aResize.remove(1);
        aResize.remove(3);
        aResize.add(8, new Integer(1));
        aResize.add(4, new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(16, new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(new Integer(2));
        aResize.add(new Integer(3));
        aResize.add(new Integer(4));
        aResize.add(new Integer(5));
        aResize.add(new Integer(6));
        aResize.add(new Integer(7));
        aResize.add(new Integer(8));
        aResize.add(new Integer(9));
        aResize.add(new Integer(10));
        aResize.add(new Integer(11));
        aResize.add(new Integer(6));
        aResize.add(new Integer(7));
        aResize.add(new Integer(8));
        aResize.add(new Integer(9));
        aResize.add(new Integer(10));
        aResize.add(new Integer(11));
        aResize.add(new Integer(6));
        aResize.add(new Integer(7));
        aResize.add(new Integer(8));
        aResize.add(new Integer(9));
        aResize.add(new Integer(10));
        aResize.add(new Integer(11));
        aResize.clear();
        aResize.remove(2);
        aResize.remove(4);
        aResize.remove(8);
        aResize.remove(1);
        aResize.remove(3);
        aResize.add(8, new Integer(1));
        aResize.add(4, new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(16, new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(new Integer(2));
        aResize.add(new Integer(3));
        aResize.add(new Integer(4));
        aResize.add(new Integer(5));
        aResize.remove(2);
        aResize.remove(4);
        aResize.remove(8);
        aResize.remove(1);
        aResize.remove(3);
        aResize.add(8, new Integer(1));
        aResize.add(4, new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(16, new Integer(1));
        aResize.add(new Integer(1));
        aResize.add(new Integer(2));
        aResize.add(new Integer(3));
        aResize.add(new Integer(4));
        aResize.add(new Integer(5));
    }

    /**
     * Add.
     * 
     * @param index the index
     * @param arg0 the arg0
     * 
     * @return true, if successful
     */
    public boolean add(int index, Object arg0) {
        if (index >= this.iMaxSize) {
            // grow array
            int newSize = this.iMaxSize + (this.iMaxSize / 2);

            if (newSize < index) {
                newSize = index + this.iMaxSize;
            }

            Object[] tmpObjects = new Object[newSize];
            int[] tmpFreeElements = new int[newSize];
            System.arraycopy(this.aFreeElements, 0, tmpFreeElements, 0, this.iMaxSize);
            this.aFreeElements = tmpFreeElements;
            System.arraycopy(this.aObjects, 0, tmpObjects, 0, this.iMaxSize);
            this.aObjects = tmpObjects;
            this.iMaxSize = newSize;
        }

        if ((this.aObjects[index] == null) && (index < this.iNextEndElements)) {
            for (int i = 0; i < this.iMaxReleasedElement; i++) {
                if (this.aFreeElements[i] == index) {
                    // shuffle array back one place
                    System.arraycopy(this.aFreeElements, i + 1, this.aFreeElements, i, this.iMaxReleasedElement - i);
                    this.iMaxReleasedElement--;
                    this.iUsedElements++;

                    break;
                }
            }
        }
        else if (index > this.iNextEndElements) {
            for (int i = this.iNextEndElements; i < index; i++) {
                this.iMaxReleasedElement++;
                this.aFreeElements[this.iMaxReleasedElement] = i;
            }

            this.iUsedElements++;
            this.iNextEndElements = index + 1;
        }
        else {
            this.iUsedElements++;
        }

        this.aObjects[index] = arg0;

        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#add(java.lang.Object)
     */
    public boolean add(Object arg0) {
        if (this.iMaxReleasedElement != -1) {
            this.aObjects[this.aFreeElements[this.iMaxReleasedElement]] = arg0;
            this.iMaxReleasedElement--;
            this.iUsedElements++;

            return true;
        }

        if (this.iUsedElements == this.iMaxSize) {
            // grow array
            Object[] tmpObjects = new Object[this.iMaxSize + (this.iMaxSize / 2)];
            int[] tmpFreeElements = new int[this.iMaxSize + (this.iMaxSize / 2)];
            System.arraycopy(this.aFreeElements, 0, tmpFreeElements, 0, this.iMaxSize);
            this.aFreeElements = tmpFreeElements;
            System.arraycopy(this.aObjects, 0, tmpObjects, 0, this.iMaxSize);
            this.aObjects = tmpObjects;
            this.iMaxSize = this.iMaxSize + (this.iMaxSize / 2);
        }

        this.aObjects[this.iNextEndElements++] = arg0;
        this.iUsedElements++;

        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#clear()
     */
    public void clear() {
        // NOTE: not sure if recreating array is quicker than clearing each node
        for (int i = 0; i < this.iMaxSize; i++)
            this.aObjects[i] = null;

        this.iMaxReleasedElement = -1;
        this.iUsedElements = 0;
        this.iNextEndElements = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#contains(java.lang.Object)
     */
    public boolean contains(Object arg0) {
        if (this.indexOf(arg0) != -1) {
            return true;
        }

        return false;
    }

    /**
     * Get.
     * 
     * @param index the index
     * 
     * @return the object
     */
    public Object get(int index) {
        if (index >= this.iMaxSize) {
            return null;
        }

        return this.aObjects[index];
    }

    /**
     * Index of.
     * 
     * @param arg0 the arg0
     * 
     * @return the int
     */
    public int indexOf(Object arg0) {
        for (int i = 0; i < this.iMaxSize; i++) {
            if ((this.aObjects[i] != null) && this.aObjects[i].equals(arg0)) {
                return i;
            }
        }

        return -1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#isEmpty()
     */
    public boolean isEmpty() {
        if (this.iUsedElements == 0) {
            return true;
        }

        return false;
    }

    /**
     * Remove.
     * 
     * @param index the index
     * 
     * @return true, if successful
     */
    public boolean remove(int index) {
        if (index < this.iMaxSize) {
            if (this.aObjects[index] != null) {
                this.iUsedElements--;
                this.iMaxReleasedElement++;
                this.aFreeElements[this.iMaxReleasedElement] = index;
                this.aObjects[index] = null;

                return true;
            }
        }

        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#remove(java.lang.Object)
     */
    public boolean remove(Object arg0) {
        int idx = this.indexOf(arg0);

        if (idx != -1) {
            this.iUsedElements--;
            this.iMaxReleasedElement++;
            this.aFreeElements[this.iMaxReleasedElement] = idx;
            this.aObjects[idx] = null;

            return true;
        }

        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#size()
     */
    public int size() {
        return this.iUsedElements;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#addAll(java.util.Collection)
     */
    public boolean addAll(Collection arg0) {
        throw new UnsupportedOperationException();

        // return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#containsAll(java.util.Collection)
     */
    public boolean containsAll(Collection arg0) {
        throw new UnsupportedOperationException();

        // return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#iterator()
     */
    public Iterator iterator() {
        throw new UnsupportedOperationException();

        // return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#removeAll(java.util.Collection)
     */
    public boolean removeAll(Collection arg0) {
        throw new UnsupportedOperationException();

        // return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#retainAll(java.util.Collection)
     */
    public boolean retainAll(Collection arg0) {
        throw new UnsupportedOperationException();

        // return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#toArray()
     */
    public Object[] toArray() {
        throw new UnsupportedOperationException();

        // return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Collection#toArray(java.lang.Object[])
     */
    public Object[] toArray(Object[] arg0) {
        throw new UnsupportedOperationException();

        // return null;
    }
}
