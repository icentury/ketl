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
package com.kni.etl.util.aggregator;

// TODO: Auto-generated Javadoc
/**
 * The Class Min.
 */
public class Min extends Aggregator {

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#setValueClass(java.lang.Class)
     */
    @Override
    public void setValueClass(Class cl) throws AggregateException {
        super.setValueClass(cl);
        if (Comparable.class.isAssignableFrom(cl) == false)
            throw new AggregateException("Cannot perform max on class type: " + cl.getName());
    }

    /** The cmp. */
    Comparable cmp = null;

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#add(java.lang.Object)
     */
    @Override
    public void add(Object arg0) {
        if (arg0 == null)
            return;
        if (this.cmp == null || this.cmp.compareTo(arg0) > 0)
            this.cmp = (Comparable) arg0;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValue()
     */
    @Override
    public Object getValue() {
        Object res = this.cmp;
        this.cmp = null;
        return res;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValueClass()
     */
    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
