/**
 *  Copyright (C) 2007 Kinetic Networks, Inc. All Rights Reserved. 
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

final class DefaultComparator implements Comparator {

    Integer[] elements;
    int len;
    Boolean[] order;

    public DefaultComparator(Integer[] elements) {
        super();
        this.elements = elements;
        this.order = new Boolean[elements.length];
        java.util.Arrays.fill(order, new Boolean(true));
        len = this.elements.length;
    }

    public DefaultComparator(Integer[] elements, Boolean[] order) {
        super();
        this.elements = elements;
        this.order = order;
        len = this.elements.length;
    }

    public int compare(Object o1, Object o2) {

        Object[] left = (Object[]) o1;
        Object[] right = (Object[]) o2;

        for (int i = 0; i < len; i++) {

            Comparable l = (Comparable) (order[i] ? left : right)[elements[i]];
            Comparable r = (Comparable) (order[i] ? right : left)[elements[i]];
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