/**
 * 
 */
package com.kni.etl.ketl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.stringtools.NumberFormatter;

public class RegisteredLookup implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    String name;
    PersistentMap lookup;
    public int writers = 0;
    public int persistence;
    public int mSourceLoadID = -1, mSourceJobExecutionID = -1;

    public String getName() {
        return name;
    }

    public int getAssociatedLoadID() {
        return mSourceLoadID;
    }

    public void delete() {
        lookup.delete();
    }

    public String toString() {
        return name + " Size: " + lookup.size() + " Other: " + lookup.toString();
    }

    boolean corrupt = false;
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
            this.lookup = EngineConstants.getInstanceOfPersistantMap(className, name, size, persistanceID,
                    EngineConstants.CACHE_PATH,keyTypes ,valueTypes, valueFields, false);
        } catch (Throwable e) {
            throw new IOException(e.getMessage());
        }
        
        corrupt = this.lookup.size()+SIZE_CHECK_OFFSET != sizecheck;
            

    }

    private static final int SIZE_CHECK_OFFSET = +1000;
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(this.lookup.size()+SIZE_CHECK_OFFSET);
        out.writeInt(this.persistence);
        out.writeInt(this.mSourceJobExecutionID);
        out.writeInt(this.mSourceLoadID);
        out.writeUTF(this.lookup.getStorageClass().getCanonicalName());
        out.writeObject(this.lookup.getKeyTypes());
        out.writeObject(this.lookup.getValueTypes());
        out.writeObject(this.lookup.getValueFields());
    }

    public void flush() {
        this.lookup.commit(true);        
    }

	public void close() {
		this.lookup.close();
	}

    public boolean corrupt() {
        return this.corrupt;
    }
}