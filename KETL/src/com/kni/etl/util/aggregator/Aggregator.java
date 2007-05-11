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
 * The Class Aggregator.
 */
public abstract class Aggregator {

    /** The output class. */
    Class mOutputClass;

    /**
     * Sets the value class.
     * 
     * @param cl the new value class
     * 
     * @throws AggregateException the aggregate exception
     */
    public void setValueClass(Class cl) throws AggregateException {
        this.mOutputClass = cl;
    }

    /**
     * Gets the value class.
     * 
     * @return the value class
     */
    abstract public Class getValueClass();

    /**
     * Add.
     * 
     * @param arg0 the arg0
     */
    abstract public void add(Object arg0);

    /**
     * Gets the value.
     * 
     * @return the value
     */
    abstract public Object getValue();

}
