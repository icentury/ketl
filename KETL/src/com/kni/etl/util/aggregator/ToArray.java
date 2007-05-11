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
 * The Class ToArray.
 */
public class ToArray extends Aggregator {

    /** The array limit. */
    int arrayLimit = 1024 * 64;
    
    /** The o. */
    Object[] o = new Object[this.arrayLimit];
    
    /** The pos. */
    int pos = 0;

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#add(java.lang.Object)
     */
    @Override
    public void add(Object arg0) {
        if (this.pos == this.arrayLimit)
            throw new AggregateException(
                    "Max elements for ToArray has been reached, increase MAXELEMENTS value, current limit = "
                            + this.arrayLimit);
        this.o[this.pos++] = arg0;
    }

    /**
     * Sets the array limit.
     * 
     * @param limit the new array limit
     */
    public void setArrayLimit(int limit) {
        if (limit < 1)
            throw new AggregateException("Array limit must be greater than 0");
        this.arrayLimit = limit;
        this.o = new Object[this.arrayLimit];
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValue()
     */
    @Override
    public Object getValue() {
        Object[] res = (Object[]) java.lang.reflect.Array.newInstance(this.mOutputClass, this.pos);
        System.arraycopy(this.o, 0, res, 0, this.pos);
        this.pos = 0;
        return res;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValueClass()
     */
    @Override
    public Class getValueClass() {
        return java.lang.reflect.Array.newInstance(this.mOutputClass, 0).getClass();
    }

}
