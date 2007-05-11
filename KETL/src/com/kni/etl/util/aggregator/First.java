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
 * The Class First.
 */
public class First extends Aggregator {

    /** The o. */
    Object o = null;

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#add(java.lang.Object)
     */
    @Override
    public void add(Object arg0) {
        if (this.o == null)
            this.o = arg0;

    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValue()
     */
    @Override
    public Object getValue() {
        Object res = this.o;
        this.o = null;
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
