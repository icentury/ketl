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
package com.kni.etl.ketl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.stringtools.NumberFormatter;

// TODO: Auto-generated Javadoc
/**
 * The Class RegisteredLookup.
 */
public class RegisteredLookup implements Externalizable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The name. */
    String name;
    
    /** The lookup. */
    PersistentMap lookup;
    
    /** The writers. */
    public int writers = 0;
    
    /** The persistence. */
    public int persistence;
    
    /** The source job execution ID. */
    public int mSourceLoadID = -1, mSourceJobExecutionID = -1;

    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the associated load ID.
     * 
     * @return the associated load ID
     */
    public int getAssociatedLoadID() {
        return this.mSourceLoadID;
    }

    /**
     * Delete.
     */
    public void delete() {
        this.lookup.delete();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.name + "\n\tSize: " + this.lookup.size() + this.lookup.toString();
    }

    /** The corrupt. */
    boolean corrupt = false;
    
    /* (non-Javadoc)
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.name = in.readUTF();
        int sizecheck = in.readInt();
        this.persistence = in.readInt();
        this.mSourceJobExecutionID = in.readInt();
        this.mSourceLoadID = in.readInt();

        String className = in.readUTF();
        int size = NumberFormatter.convertToBytes(EngineConstants.getDefaultCacheSize());

        Class[] keyTypes = (Class[]) in.readObject();
        Class[] valueTypes =  (Class[]) in.readObject();
        String[] valueFields = (String[]) in.readObject();
        int persistanceID = this.persistence == EngineConstants.JOB_PERSISTENCE ? this.mSourceJobExecutionID
                : this.mSourceLoadID;
        try {
            this.lookup = EngineConstants.getInstanceOfPersistantMap(className, this.name, size, persistanceID,
                    EngineConstants.CACHE_PATH,keyTypes ,valueTypes, valueFields, false);
        } catch (Throwable e) {
            throw new IOException(e.getMessage());
        }
        
        this.corrupt = this.lookup.size()+RegisteredLookup.SIZE_CHECK_OFFSET != sizecheck;
            

    }

    /** The Constant SIZE_CHECK_OFFSET. */
    private static final int SIZE_CHECK_OFFSET = +1000;
    
    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.lookup.size()+RegisteredLookup.SIZE_CHECK_OFFSET);
        out.writeInt(this.persistence);
        out.writeInt(this.mSourceJobExecutionID);
        out.writeInt(this.mSourceLoadID);
        out.writeUTF(this.lookup.getStorageClass().getCanonicalName());
        out.writeObject(this.lookup.getKeyTypes());
        out.writeObject(this.lookup.getValueTypes());
        out.writeObject(this.lookup.getValueFields());
    }

    /**
     * Flush.
     */
    public void flush() {
        this.lookup.commit(true);        
    }

	/**
	 * Close.
	 */
	public void close() {
		this.lookup.close();
	}

    /**
     * Corrupt.
     * 
     * @return true, if successful
     */
    public boolean corrupt() {
        return this.corrupt;
    }

    /**
     * Close caches.
     */
    public void closeCaches() {
        this.lookup.closeCacheEnvironment();
    }
}