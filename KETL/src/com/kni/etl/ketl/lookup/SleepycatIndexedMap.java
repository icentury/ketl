/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Sep 25, 2006
 * 
 */
package com.kni.etl.ketl.lookup;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.util.Bytes;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
final public class SleepycatIndexedMap implements PersistentMap {

	public void close() {
		// TODO Auto-generated method stub
		
	}
    public Class getStorageClass() {
        return this.getClass();
    }

    
    final class HashWrapper implements Externalizable {

        private static final long serialVersionUID = 1L;
        Object[] data;

        public HashWrapper(Object[] data) {
            super();
            this.data = data;
        }

        @Override
        public boolean equals(Object obj) {
            return java.util.Arrays.equals(data, ((HashWrapper) obj).data);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(data);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            data = new Object[in.readInt()];
            for (int i = data.length; i > 0; i--) {
                data[i - 1] = in.readObject();
            }

        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(data.length);
            for (int i = data.length; i > 0; i--) {
                out.writeObject(data[i - 1]);
            }
        }

    }

    private int commitCount = 0;
    private int count = 0;
    private HashMap fieldIndex = new HashMap();

    private String mCacheDir = null;
    private boolean mKeyIsArray, mValuesIsArray;
    private Class[] mKeyTypes;
    private String mName;
    private Integer mPersistanceID;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#get(java.lang.Object)
     */

    private String[] mValueFields;

    private Class[] mValueTypes;

    private Statement stmt;
    private int mSize;

    private static Environment myEnvironment = null;

    public SleepycatIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes,
            Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
        super();

        mSize = pSize;
        mPersistanceID = pPersistanceID;
        mName = pName;
        this.mCacheDir = pCacheDir;
        this.mKeyTypes = pKeyTypes;
        this.mValueTypes = pValueTypes;
        this.mKeyIsArray = true;// pKeyTypes.length == 1 ? false : true;
        this.mValuesIsArray = true; // pValueTypes.length == 1 ? false : true;
        this.mValueFields = pValueFields;
        for (int i = 0; i < pValueFields.length; i++) {
            fieldIndex.put(pValueFields[i], i);
        }

        try {
            init();
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

    private boolean dbClearPending = false;

    public void clear() {

        {
            // try {
            // this.myClassDb.close();
            // this.myDatabase.close();
            // ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE,"Removed " +
            // this.myEnvironment.truncateDatabase(null, mName, true) + " tuples from the cache");
            // myDatabase = myEnvironment.openDatabase(null, mName, myDbConfig);

            // } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            // throw new KETLError(e);
            // }

        }
    }

    public synchronized void commit(boolean force) {
        if (commitCount > 0) {

            Runtime r = Runtime.getRuntime();
            long free = (r.maxMemory() - (r.totalMemory() - r.freeMemory()));
            if (free < (10 * 1024 * 1024)) {
                try {
                    commitCount = 0;
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + mName
                            + "' Size: " + NumberFormatter.format(new File(this.getCacheDirectory()).length())
                            + ", Approximate Count: " + this.myDatabase.count());

                    this.myDatabase.sync();
                } catch (DatabaseException e) {
                    // TODO Auto-generated catch block
                    throw new KETLError(e);
                }
            }

        }
    }

    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public synchronized void delete() {
        this.deleteCache();
    }

    public synchronized void deleteCache() {

        if (myDatabase != null) {
            try {
                myDatabase.close();
                myClassDb.close();
                myEnvironment.removeDatabase(null, myDatabase.getDatabaseName());
                myEnvironment.removeDatabase(null, myClassDb.getDatabaseName());
            } catch (DatabaseException e) {
                throw new KETLError(e);
            } finally {
                myDatabase = null;
                myClassDb = null;
            }
        }
    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    public Object get(Object key) {
        return this.get(key, null);
    }

    public Object get(Object pkey, String pField) {

        Object res;

        // local hashed cached
        {

            {
                try {

                    res = this.getItem(pkey);

                } catch (Exception e) {
                    throw new KETLError(e);
                }
                if (res == null)
                    return null;

            }

            if (this.mValuesIsArray)
                res = ((HashWrapper) res).data;
        }

        if (this.mValuesIsArray == false)
            return res;

        Object idx = this.fieldIndex.get(pField);

        if (idx == null)
            throw new KETLError("Key " + pField + " does not exist in lookup");

        return ((Object[]) res)[(Integer) idx];

    }

    private byte[] getAsKey(Object obj, Class cl) throws Error {

        if (obj == null)
            return null;

        if (cl == Double.class)
            return Bytes.packF8((Double) obj);
        if (cl == Integer.class)
            return Bytes.pack4((Integer) obj);
        if (cl == String.class)
            try {
                return Bytes.packStr((String) obj, null);
            } catch (UnsupportedEncodingException e) {
                throw new KETLError(e);
            }
        if (cl == Long.class)
            return Bytes.pack8((Long) obj);
        if (cl == Short.class)
            return Bytes.pack2((Short) obj);
        if (cl == Float.class)
            return Bytes.packF4((Float) obj);
        if (cl == java.util.Date.class)
            return Bytes.pack8(((java.util.Date) obj).getTime());

        if (cl == java.sql.Date.class)
            return Bytes.pack8(((java.sql.Date) obj).getTime());
        if (cl == java.sql.Time.class)
            return Bytes.pack8(((java.sql.Time) obj).getTime());

        if (cl == java.sql.Timestamp.class)
            return Bytes.append(Bytes.pack8(((java.sql.Timestamp) obj).getTime()), Bytes
                    .pack4(((java.sql.Timestamp) obj).getNanos()));

        if (cl == byte[].class)
            return (byte[]) obj;
        if (cl == BigDecimal.class) {
            return Bytes.append(Bytes.packF8(((BigDecimal) obj).doubleValue()), Bytes.pack4(((BigDecimal) obj)
                    .hashCode()));
        }

        throw new KETLError("Unsupported key type " + cl.getName());
    }

    private String getCacheDirectory() {
        File fDir;
        if (this.mPersistanceID == null)
            fDir = new File(mCacheDir + File.separator + "KETL." + mName + "cache");
        else
            fDir = new File(mCacheDir + File.separator + "KETL." + mName + "." + this.mPersistanceID + ".cache");

        if (fDir.exists() && fDir.isDirectory())
            return fDir.getAbsolutePath();
        else if (fDir.exists() == false) {
            fDir.mkdir();
            return fDir.getAbsolutePath();
        }
        else
            throw new KETLError("Cache directory is already in use");

    }

    private Database myClassDb, myDatabase;
    private EntryBinding dataBinding;
    private DatabaseConfig myDbConfig;

    void init() throws ClassNotFoundException, SQLException {

        this.deleteCache();

        if (myEnvironment == null) {
            try {
                configureEnvironment();
            } catch (DatabaseException dbe) {
                throw new KETLError(dbe);
            }
        }

        try {

            myDbConfig = new DatabaseConfig();
            myDbConfig.setAllowCreate(true);
            myDbConfig.setDeferredWrite(true);
            myDatabase = myEnvironment.openDatabase(null, mName, myDbConfig);
            myClassDb = myEnvironment.openDatabase(null, mName + "classDb", myDbConfig);

            // Instantiate the class catalog
            StoredClassCatalog classCatalog = new StoredClassCatalog(myClassDb);

            // Create the binding
            dataBinding = new SerialBinding(classCatalog, HashWrapper.class);

            if (dbClearPending) {
                this.clear();
                dbClearPending = false;
            }
        } catch (DatabaseException dbe) {
            throw new KETLError(dbe);
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
                + this.mName + ", Key Type(s):" + java.util.Arrays.toString(this.mKeyTypes) + ", Key Field(s):"
                + java.util.Arrays.toString(this.mValueFields) + ", Result Type(s):"
                + java.util.Arrays.toString(this.mValueTypes));
    }

    private void configureEnvironment() throws DatabaseException {

        File dir = new File(this.getCacheDirectory());

        if (dir.exists())
            deleteDir(dir);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        myEnvironment = new Environment(new File(this.getCacheDirectory()), envConfig);

        EnvironmentMutableConfig envMutableConfig = new EnvironmentMutableConfig();
        int pct = (int) (EngineConstants.getCacheMemoryRatio() * 100);

        envMutableConfig.setCachePercent(pct);
        envMutableConfig.setTxnWriteNoSync(true);
        myEnvironment.setMutableConfig(envMutableConfig);

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Setting cache size to "
                + NumberFormatter.format((long) (Runtime.getRuntime().maxMemory() * EngineConstants
                        .getCacheMemoryRatio())));
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    // Deletes all files and subdirectories under dir.
    // Returns true if all deletions were successful.
    // If a deletion fails, the method stops attempting to delete and returns false.
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * private Object formKey() { }
     */

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object pkey, Object pValue) {

        // local hashed cached

        // HSQLDB based cache
        try {
            byte[] key = null;
            if (this.mKeyIsArray) {
                Object[] tmp = (Object[]) pkey;

                for (int i = 0; i < tmp.length; i++) {
                    if (i > 0)
                        key = Bytes.append(key, getAsKey(tmp[i], this.mKeyTypes[i]));
                    else
                        key = getAsKey(tmp[i], this.mKeyTypes[i]);
                }
            }
            else {
                key = getAsKey(pkey, pkey == null ? null : pkey.getClass());
            }

            // Create the DatabaseEntry for the key
            DatabaseEntry theKey = new DatabaseEntry(key);

            // Create the DatabaseEntry for the data. Use the EntryBinding object
            // that was just created to populate the DatabaseEntry
            DatabaseEntry theData = new DatabaseEntry();
            dataBinding.objectToEntry((this.mValuesIsArray ? new HashWrapper((Object[]) pValue) : pValue), theData);

            // Put it as normal
            if (myDatabase.putNoOverwrite(null, theKey, theData) == OperationStatus.SUCCESS)
                commitCount++;

            if (this.commitCount > 200000) {
                this.commit(false);
            }

        } catch (Exception e) {
            throw new KETLError(e);
        }

        return null;
    }

    public void putAll(Map t) {

    }

    public Object remove(Object pkey) {

        byte[] key = null;
        if (this.mKeyIsArray) {
            Object[] tmp = (Object[]) pkey;

            for (int i = 0; i < tmp.length; i++) {
                if (i > 0)
                    key = Bytes.append(key, getAsKey(tmp[i], this.mKeyTypes[i]));
                else
                    key = getAsKey(tmp[i], this.mKeyTypes[i]);
            }
        }
        else {
            key = getAsKey(pkey, pkey == null ? null : pkey.getClass());
        }

        // Create the DatabaseEntry for the key
        DatabaseEntry theKey = new DatabaseEntry(key);

        // Do the get as normal
        try {
            myDatabase.delete(null, theKey);
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            throw new KETLError(e);
        }

        return null;
    }

    public int size() {
        try {
            ResultSet rs = stmt.executeQuery("select count(*) from " + mName);

            int size = -1;
            while (rs.next()) {
                size = rs.getInt(1);
            }
            return size;
        } catch (SQLException e) {
            throw new KETLError(e);

        }
    }

    public void switchToReadOnlyMode() {

    }

    @Override
    public String toString() {
        String exampleValue = "N/A", exampleKey = "N/A";
        String strSize;
        try {
            strSize = NumberFormatter.format(myEnvironment == null ? 0 : myEnvironment.getStats(null)
                    .getCacheTotalBytes());
        } catch (DatabaseException e) {
            strSize = "N/A";
        }
        // TODO Auto-generated method stub
        return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
                + "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
                + java.util.Arrays.toString(this.mValueTypes) + "\n\tExample: Key->" + exampleKey + " Value->"
                + exampleValue + "\n\tCount: " + this.count + "\n\tConsolidated Cache Size: " + strSize;

    }

    public Collection values() {
        return null;
    }

    public Object getItem(Object pkey) throws DatabaseException {
        byte[] key = null;
        if (this.mKeyIsArray) {
            Object[] tmp = (Object[]) pkey;

            for (int i = 0; i < tmp.length; i++) {
                if (i > 0) {
                    key = Bytes.append(key, new byte[] { 0 });
                    key = Bytes.append(key, getAsKey(tmp[i], this.mKeyTypes[i]));
                }
                else
                    key = getAsKey(tmp[i], this.mKeyTypes[i]);
            }
        }
        else {
            key = getAsKey(pkey, pkey == null ? null : pkey.getClass());
        }
        // Create the DatabaseEntry for the key
        DatabaseEntry theKey = new DatabaseEntry(key);
        DatabaseEntry theData = new DatabaseEntry();

        // Do the get as normal
        if (myDatabase.get(null, theKey, theData, LockMode.READ_UNCOMMITTED) == OperationStatus.NOTFOUND)
            return null;

        // Recreate the MyData object from the retrieved DatabaseEntry using
        // the EntryBinding created above
        return dataBinding.entryToObject(theData);
    }

    public Class[] getKeyTypes() {
        return this.mKeyTypes;
    }

    public String[] getValueFields() {
        return this.mValueFields;
    }

    public Class[] getValueTypes() {
        return this.mValueTypes;
    }

    public int getCacheSize() {
        return this.mSize;
    }

    public String getName() {
        return this.mName;
    }

}
