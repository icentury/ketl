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
 * The Class Count.
 */
final public class Count extends Aggregator {

    /** The counter. */
    private int counter = 0;

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#add(java.lang.Object)
     */
    @Override
    public void add(Object arg0) {
        this.counter++;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValue()
     */
    @Override
    public Object getValue() {
        Integer res = this.counter;
        this.counter = 0;
        return res;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValueClass()
     */
    @Override
    public Class getValueClass() {
        return Integer.class;
    }

}
