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
 * The Class Sum.
 */
public class Sum extends Aggregator {

    /** The bd. */
    double bd;
    
    /** The counter. */
    double counter = 0;
    
    /** The type. */
    int type;

    /** The Constant validClasses. */
    private static final Class[] validClasses = { int.class, float.class, double.class, short.class, long.class,
            byte.class, Integer.class, Float.class, Double.class, Short.class, Long.class, Byte.class };
    
    /** The Constant validClassTypes. */
    private static final int[] validClassTypes = { 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5 };

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#setValueClass(java.lang.Class)
     */
    @Override
    public void setValueClass(Class pOutputClass) throws AggregateException {
        super.setValueClass(pOutputClass);
        int res = com.kni.util.Arrays.searchArray(Sum.validClasses, pOutputClass);
        if (res < 0)
            throw new AggregateException("Cannot perform sum on class type: " + pOutputClass.getName());

        this.type = Sum.validClassTypes[res];
    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#add(java.lang.Object)
     */
    @Override
    public void add(Object arg0) {
        if (this.counter == 0) {
            this.bd = ((Number) arg0).doubleValue();
        }
        else
            this.bd += ((Number) arg0).doubleValue();
        this.counter++;

    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValue()
     */
    @Override
    public Object getValue() {

        Double res = this.bd;
        this.counter = 0;

        switch (this.type) {
        case 0:
            return res.intValue();
        case 1:
            return res.floatValue();
        case 2:
            return res.doubleValue();
        case 3:
            return res.shortValue();
        case 4:
            return res.longValue();
        case 5:
            return res.byteValue();
        default:
            return res.doubleValue();
        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.util.aggregator.Aggregator#getValueClass()
     */
    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
