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

// TODO: Auto-generated Javadoc
/**
 * The Class DefaultComparator.
 */
final class DefaultComparator implements Comparator {

    /** The elements. */
    Integer[] elements;
    
    /** The len. */
    int len;
    
    /** The order. */
    Boolean[] order;

    /**
     * Instantiates a new default comparator.
     * 
     * @param elements the elements
     */
    public DefaultComparator(Integer[] elements) {
        super();
        this.elements = elements;
        this.order = new Boolean[elements.length];
        java.util.Arrays.fill(this.order, new Boolean(true));
        this.len = this.elements.length;
    }

    /**
     * Instantiates a new default comparator.
     * 
     * @param elements the elements
     * @param order the order
     */
    public DefaultComparator(Integer[] elements, Boolean[] order) {
        super();
        this.elements = elements;
        this.order = order;
        this.len = this.elements.length;
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(Object o1, Object o2) {

        Object[] left = (Object[]) o1;
        Object[] right = (Object[]) o2;

        for (int i = 0; i < this.len; i++) {

            Comparable l = (Comparable) (this.order[i] ? left : right)[this.elements[i]];
            Comparable r = (Comparable) (this.order[i] ? right : left)[this.elements[i]];
            int res;
            if (l == null && r == null)
                res = 0;
            else if (l == null && r != null)
                res = -1;
            else if (l != null && r == null)
                res = 1;
            else
                res = l.compareTo(r);

            if (res != 0)
                return res;
        }

        return 0;
    }

}