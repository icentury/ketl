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

import java.util.concurrent.LinkedBlockingQueue;

// TODO: Auto-generated Javadoc
/**
 * The Class ManagedBlockingQueue.
 */
public abstract class  ManagedBlockingQueue extends LinkedBlockingQueue {

    /**
     * Instantiates a new managed blocking queue.
     * 
     * @param capacity the capacity
     */
    public ManagedBlockingQueue(int capacity) {
        super(capacity);
    }

    /**
     * Sets the name.
     * 
     * @param arg0 the new name
     */
    public abstract void setName(String arg0);

    public abstract String getName();
    
    /**
     * Register reader.
     * 
     * @param worker the worker
     */
    public abstract void registerReader(ETLWorker worker);

    /**
     * Register writer.
     * 
     * @param worker the worker
     */
    public abstract void registerWriter(ETLWorker worker);

    
}