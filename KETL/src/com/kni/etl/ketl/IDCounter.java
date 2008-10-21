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
 * Created on Jun 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl;

import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;

// TODO: Auto-generated Javadoc
/**
 * The Class IDCounter.
 * 
 * @author nwakefield Creation Date: Jun 17, 2003
 */
public class IDCounter {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3257849878729340726L;
    
    /** The id. */
    private long id = 0;
    
    /** The batch size. */
    private long batchSize = 0;
    
    /** The md. */
    transient private Metadata md = null;
    
    /** The ids left. */
    private long idsLeft = 0;
    
    /** The ID name. */
    private String IDName;

    /**
     * Instantiates a new ID counter.
     */
    public IDCounter() {
        super();
    }

    /**
     * Instantiates a new ID counter.
     * 
     * @param strIDName the str ID name
     * @param param the param
     * 
     * @throws Exception the exception
     */
    public IDCounter(String strIDName, long param) throws Exception {
        super();
        this.batchSize = param;
        this.md = ResourcePool.getMetadata();
        this.idsLeft = this.batchSize;

        // Get max temp session id
        this.IDName = strIDName;

        try {
            // System.out.println("IDS " + Thread.currentThread().getName() + ": ID in " + id + " batchsize:" +
            // batchSize);
            this.id = this.md.getBatchOfIDValues(this.IDName, this.batchSize);

            // System.out.println("IDS " + Thread.currentThread().getName() + ": ID out " + id + " batchsize:" +
            // batchSize);
        } catch (Exception e) {
            this.md = null;
        }

        if (this.md == null) {
            ResourcePool.LogMessage(this, "Sequence " + strIDName + " defaulting to 0 as metadata not available!");
            this.id = 0;
        }
    }

    /**
     * Increment ID.
     * 
     * @return the long
     */
    public synchronized final long incrementID() {
        this.idsLeft--;

        if (this.idsLeft <= 0) {
            if (this.md != null) {
                try {
                    // System.out.println("IDS " + Thread.currentThread().getName() + ": ID in " + id + " batchsize:" +
                    // batchSize);
                    this.id = this.md.getBatchOfIDValues(this.IDName, this.batchSize);

                    // System.out.println("IDS " + Thread.currentThread().getName() + ": ID out " + id + " batchsize:" +
                    // batchSize);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            this.idsLeft = this.batchSize - 1;
        }

        return this.id++;
    }

    /**
     * Sets the to current ID.
     * 
     * @return the long
     */
    public final long setToCurrentID() {
        if (this.md != null) {
            this.md.setMaxIDValue(this.IDName, this.id++);
        }

        return this.id;
    }

    /**
     * Gets the ID.
     * 
     * @return the ID
     */
    public final long getID() {
        return this.id;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return Long.toString(this.id);
    }
}
