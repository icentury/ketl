/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Sep 25, 2006
 * 
 */
package com.kni.etl.ketl.lookup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.util.Bytes;
import com.sleepycat.bind.tuple.BigIntegerBinding;
import com.sleepycat.bind.tuple.BooleanBinding;
import com.sleepycat.bind.tuple.DoubleBinding;
import com.sleepycat.bind.tuple.FloatBinding;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.ShortBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

final public class SleepycatIndexedMap implements PersistentMap {

    final static byte ARRAY = 8;
    final static byte BIGDECIMAL = 13;
    final static byte DATE = 9;
    final static byte DOUBLE = 4;
    final static byte FLOAT = 3;
    final static byte INT = 1;
    final static byte LONG = 2;
    final static byte NULL = 7;
    final static byte OBJECT = 6;
    final static byte SHORT = 0;
    final static byte SQLDATE = 11;
    final static byte STRING = 5;
    final static byte TIME = 10;
    final static byte TIMESTAMP = 12;

    public static Object byteArrayToObject(byte[] buf, int off, int length) throws IOException, ClassNotFoundException {

        byte type = buf[off++];

        if (type == NULL)
            return null;
        if (type == ARRAY) {
            int len = (int) buf[off++];
            Object[] ar = new Object[len];
            int pos = off;
            for (int i = 0; i < len; i++) {
                int size = buf[pos++];
                ar[i] = byteArrayToObject(buf, pos, size);
                pos += size;
            }

            return ar;
        }
        if (type == INT) {
            return Bytes.unpack4(buf, off);
        }
        if (type == STRING) {
            return new String(buf, off, length - 1);
        }
        if (type == SHORT) {
            return Bytes.unpack2(buf, off);
        }
        if (type == DOUBLE) {
            return Bytes.unpackF8(buf, off);
        }
        if (type == LONG) {
            return Bytes.unpack8(buf, off);
        }
        if (type == FLOAT) {
            return Bytes.unpackF4(buf, off);
        }
        if (type == TIMESTAMP) {
            long t = Bytes.unpack8((byte[]) buf, off);
            java.sql.Timestamp res = new java.sql.Timestamp(t);
            res.setNanos(Bytes.unpack4((byte[]) buf, off + 8));
            return res;
        }
        if (type == BIGDECIMAL) {
            byte[] tmp = new byte[length - 5];
            int scale = Bytes.unpack4(buf, off);
            System.arraycopy(buf, off + 4, tmp, 0, length - 5);
            BigInteger bi = new BigInteger(tmp);
            return new BigDecimal(bi, scale);
        }
        if (type == SQLDATE) {
            return new java.sql.Date(Bytes.unpack8(buf, off));
        }
        if (type == TIME) {
            return new java.sql.Time(Bytes.unpack8(buf, off));
        }
        if (type == OBJECT) {
            ByteArrayInputStream outStream = new ByteArrayInputStream(buf, off, length - 1);
            ObjectInputStream objStream = new ObjectInputStream(outStream);
            Object obj = objStream.readObject();
            objStream.close();
            outStream.close();
            return obj;
        }
        throw new IOException("Invalid type found in data stream");
    }

    // Deletes all files and subdirectories under dir.
    // Returns true if all deletions were successful.
    // If a deletion fails, the method stops attempting to delete and returns
    // false.
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

    public static byte[] objToByteArray(Object obj) throws IOException {

        if (obj == null)
            return new byte[] { NULL };

        Class cl = obj.getClass();
        byte[] buf;

        if (cl.isArray()) {
            Object[] ar = (Object[]) obj;
            int len = ar.length;

            buf = new byte[] { ARRAY, (byte) len };
            for (int i = 0; i < len; i++) {
                byte[] tmp = objToByteArray(ar[i]);
                buf = Bytes.join(buf, new byte[] { (byte) tmp.length });
                buf = Bytes.join(buf, tmp);
            }

            return buf;
        }

        if (cl == Integer.class) {
            buf = new byte[5];
            buf[0] = INT;
            Bytes.pack4(buf, 1, ((Integer) obj));
        }
        else if (cl == String.class) {
            byte[] tmp = ((String) obj).getBytes();
            buf = new byte[tmp.length + 1];
            buf[0] = STRING;
            System.arraycopy(tmp, 0, buf, 1, tmp.length);
        }
        else if (cl == java.sql.Timestamp.class) {
            java.sql.Timestamp tmp = (java.sql.Timestamp) obj;

            buf = new byte[13];
            buf[0] = TIMESTAMP;
            Bytes.pack8(buf, 1, tmp.getTime());
            Bytes.pack4(buf, 9, tmp.getNanos());
        }
        else if (cl == java.math.BigDecimal.class) {
            BigDecimal bd = (BigDecimal) obj;
            byte[] tmp = bd.unscaledValue().toByteArray();
            buf = new byte[tmp.length + 5];
            buf[0] = BIGDECIMAL;
            int scale = bd.scale();
            Bytes.pack4(buf, 1, scale);
            System.arraycopy(tmp, 0, buf, 5, tmp.length);
        }
        else if (cl == java.sql.Date.class) {
            buf = new byte[9];
            buf[0] = SQLDATE;
            Bytes.pack8(buf, 1, ((java.sql.Date) obj).getTime());
        }
        else if (cl == java.sql.Time.class) {
            buf = new byte[9];
            buf[0] = TIME;
            Bytes.pack8(buf, 1, ((java.sql.Time) obj).getTime());
        }
        else if (cl == Short.class) {
            buf = new byte[3];
            buf[0] = SHORT;
            Bytes.pack2(buf, 1, ((Short) obj));
        }
        else if (cl == Long.class) {
            buf = new byte[9];
            buf[0] = LONG;
            Bytes.pack8(buf, 1, ((Long) obj));
        }
        else if (cl == Float.class) {
            buf = new byte[5];
            buf[0] = FLOAT;
            Bytes.packF4(buf, 1, ((Float) obj));
        }
        else if (cl == Double.class) {
            buf = new byte[9];
            buf[0] = DOUBLE;
            Bytes.packF8(buf, 1, ((Double) obj));
        }
        else {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(outStream);
            objStream.writeObject(obj);
            objStream.flush();
            outStream.flush();
            byte[] tmp = outStream.toByteArray();
            buf = new byte[tmp.length+1];
            buf[0] = OBJECT;
            System.arraycopy(tmp, 0,buf,1, tmp.length);
            objStream.close();
            outStream.close();
        }
        return buf;
    }

    private static Environment myEnvironment = null;
    private String classDb;
    private int commitCount = 0;
    private int count = 0;
    private String dataDb;
    private HashMap fieldIndex = new HashMap();
    private String mCacheDir = null;
    private TupleBinding mKeyBinding;
    private boolean mKeyIsArray;
    private Class[] mKeyTypes;
    private String mName;
    private int mSize;
    private String[] mValueFields;
    private boolean mValuesIsArray;
    private Class[] mValueTypes;
    private Database myClassDb, myDatabase;
    private DatabaseConfig myDbConfig;
	private int mKeyLength;
    private static List mValidCacheList;

    public SleepycatIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes,
            Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
        super();

        this.mSize = pSize;
        this.mName = pName;
        this.dataDb = mName + (pPersistanceID == null ? "" : pPersistanceID);
        this.classDb = mName + "classDb" + (pPersistanceID == null ? "" : pPersistanceID);
        this.mCacheDir = pCacheDir;
        this.mKeyTypes = pKeyTypes;
        this.mValueTypes = pValueTypes;
        this.mKeyIsArray = pKeyTypes.length == 1 ? false : true;
        this.mValueFields = pValueFields;
        this.mKeyLength = this.mKeyTypes.length;

        if (this.mKeyIsArray == false) {
            Class cl = pKeyTypes[0];
            if (cl == BigInteger.class)
                this.mKeyBinding = new BigIntegerBinding();
            if (cl == Boolean.class)
                this.mKeyBinding = new BooleanBinding();
            if (cl == Double.class)
                this.mKeyBinding = new DoubleBinding();
            if (cl == Float.class)
                this.mKeyBinding = new FloatBinding();
            if (cl == Integer.class)
                this.mKeyBinding = new IntegerBinding();
            if (cl == Long.class)
                this.mKeyBinding = new LongBinding();
            if (cl == Short.class)
                this.mKeyBinding = new ShortBinding();
            if (cl == String.class)
                this.mKeyBinding = new StringBinding();
        }

        this.mValuesIsArray = pValueTypes.length == 1 ? false : true;
        for (int i = 0; i < pValueFields.length; i++) {
            fieldIndex.put(pValueFields[i], i);
        }

        // initialize the cache
        init(pPurgeCache);

    }

    public void clear() {
        // reinitialize the cache
        this.init(true);
    }

    public void close() {
        if (myDatabase != null) {
            try {
                this.myDatabase.sync();
                this.myClassDb.sync();
                this.myDatabase.close();
                this.myClassDb.close();
            } catch (DatabaseException e) {
                ResourcePool.LogException(e, Thread.currentThread());
            }
        }

    }

    public synchronized void commit(boolean force) {

        if (force || commitCount > 0) {

            if (force == false) {
                Runtime r = Runtime.getRuntime();
                long free = (r.maxMemory() - (r.totalMemory() - r.freeMemory()));
                force = free < (10 * 1024 * 1024);
            }
            if (force) {
                try {
                    commitCount = 0;
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + mName
                            + ", Approximate Count: " + this.myDatabase.count());

                    this.myDatabase.sync();
                } catch (DatabaseException e) {
                    throw new KETLError(e);
                }
            }

        }
    }

    private static void configureEnvironment(String cacheDir) throws DatabaseException {

        synchronized (lock) {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            myEnvironment = new Environment(new File(cacheDir), envConfig);

            EnvironmentMutableConfig envMutableConfig = new EnvironmentMutableConfig();
            int pct = (int) (EngineConstants.getCacheMemoryRatio() * 100);

            envMutableConfig.setCachePercent(pct);
            envMutableConfig.setTxnWriteNoSync(true);
            myEnvironment.setMutableConfig(envMutableConfig);

            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Setting cache size to "
                    + NumberFormatter.format((long) (Runtime.getRuntime().maxMemory() * EngineConstants
                            .getCacheMemoryRatio())));
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

        try {
            if (myDatabase != null) {
                this.myDatabase.sync();
                this.myClassDb.sync();
                this.myDatabase.close();
                this.myClassDb.close();
            }

            removeDatabase(dataDb, classDb);
        } catch (DatabaseException e) {
            throw new KETLError(e);
        } finally {
            myDatabase = null;
            myClassDb = null;
        }

    }

    private static void removeDatabase(String dataDb, String classDb) throws DatabaseException {
        synchronized (lock) {
            List dbs = myEnvironment.getDatabaseNames();
            if (dbs.contains(dataDb))
                myEnvironment.removeDatabase(null, dataDb);
            if (dbs.contains(classDb))
                myEnvironment.removeDatabase(null, classDb);
        }
    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    public Object get(Object key) {
        return this.get(key, null);
    }

    public Object get(Object pkey, String pField) {

        Object[] res;

        // local hashed cached
        try {
            res = (Object[]) this.getItem(pkey);
        } catch (Exception e) {
            throw new KETLError(e);
        }
        if (res == null)
            return null;

        if (this.mValuesIsArray == false)
            return res[0];

        Object idx = this.fieldIndex.get(pField);

        if (idx == null)
            throw new KETLError("Key " + pField + " does not exist in lookup");

        return ((Object[]) res)[(Integer) idx];

    }

    private String getCacheDirectory() {
        File fDir = new File(mCacheDir + File.separator + "KETL." + "cache");

        if (fDir.exists() && fDir.isDirectory())
            return fDir.getAbsolutePath();
        else if (fDir.exists() == false) {
            fDir.mkdir();
            return fDir.getAbsolutePath();
        }
        else
            throw new KETLError("Cache directory is already in use");

    }

    public int getCacheSize() {
        return this.mSize;
    }

    public Object getItem(Object pkey) throws DatabaseException, IOException, ClassNotFoundException {
        // Create the DatabaseEntry for the key

        DatabaseEntry theKey = this.getKey(pkey);
        DatabaseEntry theData = new DatabaseEntry();

        // Do the get as normal
        if (myDatabase.get(null, theKey, theData, LockMode.READ_UNCOMMITTED) == OperationStatus.NOTFOUND)
            return null;

        // Recreate the MyData object from the retrieved DatabaseEntry using
        // the EntryBinding created above

        byte[] res = theData.getData();
        if (res == null)
            return null;
        return this.mValuesIsArray==false?new Object[] {byteArrayToObject(res, 0, res.length)}:byteArrayToObject(res, 0, res.length);
    }

    private Object castKey(Object[] key) throws IOException {
		if (key == null)
			return null;

		for (int i = 0; i < mKeyLength; i++) {
			key[i] = castKeyElement(key[i], this.mKeyTypes[i]);
		}

		return key;
	}
    
    private  Object castKeyElement(Object obj, Class cl)
			throws IOException {

		if (obj == null)
			return null;

		Class clFrom = obj.getClass();

		if (cl == clFrom) {
			return obj;
		}

		if (cl.isArray()) {
			throw new IOException(
					"Key cannot be cast to an Array, incoming class "
							+ clFrom.getCanonicalName());
		}

		if (clFrom == Double.class || clFrom == Long.class
				|| clFrom == Integer.class || clFrom == Short.class
				|| clFrom == Float.class
				|| clFrom == java.math.BigDecimal.class
				|| obj instanceof java.lang.Number) {
			java.lang.Number bd = (java.lang.Number) obj;
			if (cl == Integer.class)
				return bd.intValue();
			if (cl == Double.class)
				return bd.doubleValue();
			if (cl == Short.class)
				return bd.shortValue();
			if (cl == Long.class)
				return bd.longValue();
			if (cl == Float.class)
				return bd.floatValue();
			if (cl == java.math.BigDecimal.class) {
				if (clFrom == Float.class || clFrom == Double.class)
					return new java.math.BigDecimal(bd.doubleValue());
				return new java.math.BigDecimal(bd.longValue());

			}
		}

		try {
			if (cl == Integer.class) {
				return (Integer) obj;
			}

			if (cl == String.class) {
				return (String) obj;
			}

			if (cl == java.sql.Timestamp.class) {
				return (java.sql.Timestamp) obj;
			}

			if (cl == java.math.BigDecimal.class) {
				return (java.math.BigDecimal) obj;
			}

			if (cl == java.sql.Date.class) {
				return (java.sql.Date) obj;
			}

			if (cl == java.sql.Time.class) {
				return (java.sql.Time) obj;
			}

			if (cl == Short.class) {
				return (Short) obj;
			}
			
			if (cl == Long.class) {
				return (Long) obj;
			}
			
			if (cl == Float.class) {
				return (Float) obj;
			}
			
			if (cl == Double.class) {
				return (Double) obj;
			}
		} catch (ClassCastException e) {
			throw new IOException("Cache '"+this.getName()+"' error: Class of incoming key "
					+ clFrom.getCanonicalName()
					+ " cannot be cast to expected to data type "
					+ cl.getCanonicalName() + ", " + e.getMessage());
		}

		throw new IOException("Cache '"+this.getName()+"' error: Class of incoming key "
				+ clFrom.getCanonicalName()
				+ " cannot be cast to expected to data type "
				+ cl.getCanonicalName());

	}

	private DatabaseEntry getKey(Object val) throws IOException {
    	
		val = this.castKey((Object[]) val);
		
        if (this.mKeyBinding == null)
            return new DatabaseEntry(objToByteArray((val)));

        DatabaseEntry entry = new DatabaseEntry();
        this.mKeyBinding.objectToEntry((val == null ? null : (Object[]) val)[0], entry);
        return entry;

    }

    public Class[] getKeyTypes() {
        return this.mKeyTypes;
    }

    public String getName() {
        return this.mName;
    }

    public Class getStorageClass() {
        return this.getClass();
    }

    public String[] getValueFields() {
        return this.mValueFields;
    }

    public Class[] getValueTypes() {
        return this.mValueTypes;
    }

    void init(boolean purge) {

        initEnvironment(this.getCacheDirectory());

        mValidCacheList.add(this.dataDb);
        mValidCacheList.add(this.classDb);

        if(purge)
            this.deleteCache();

        if (this.myDbConfig == null) {
            myDbConfig = new DatabaseConfig();
            myDbConfig.setAllowCreate(true);
            myDbConfig.setDeferredWrite(true);
        }

        try {
            synchronized (lock) {
                myDatabase = myEnvironment.openDatabase(null, this.dataDb, myDbConfig);
                myClassDb = myEnvironment.openDatabase(null, this.classDb, myDbConfig);
                List ls = myEnvironment.getDatabaseNames();
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "Cache db's: "
                        + ls.toString());
                this.myDatabase.sync();
                this.myClassDb.sync();
            }

        } catch (DatabaseException dbe) {
            throw new KETLError(dbe);
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
                + this.mName + ", Key Type(s):" + java.util.Arrays.toString(this.mKeyTypes) + ", Key Field(s):"
                + java.util.Arrays.toString(this.mValueFields) + ", Result Type(s):"
                + java.util.Arrays.toString(this.mValueTypes));
    }

    private static synchronized void initEnvironment(String cacheDir) {
        synchronized (lock) {
            if (myEnvironment == null) {
                mValidCacheList = java.util.Collections.synchronizedList(new ArrayList());
                try {
                    configureEnvironment(cacheDir);
                } catch (DatabaseException dbe) {
                    throw new KETLError(dbe);
                }
            }
        }
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    public Object put(Object pkey, Object pValue) {

        try {

            // Create the DatabaseEntry for the key
            DatabaseEntry theKey = this.getKey(pkey);

            // Create the DatabaseEntry for the data. Use the EntryBinding
            // object
            // that was just created to populate the DatabaseEntry
            DatabaseEntry theData = new DatabaseEntry(objToByteArray(this.mValuesIsArray ? pValue
                    : ((Object[]) pValue)[0]));

            // Put it as normal
            if (myDatabase.putNoOverwrite(null, theKey, theData) == OperationStatus.SUCCESS)
                commitCount++;

            if (this.commitCount >= 200000) {
                this.commit(false);
            }

        } catch (Exception e) {
            throw new KETLError(e);
        }

        return null;
    }

    public void putAll(Map t) {
        throw new UnsupportedOperationException();
    }

    public Object remove(Object pkey) {

        // Create the DatabaseEntry for the key
        DatabaseEntry theKey;
        try {
            theKey = this.getKey(pkey);
        } catch (IOException e1) {
            throw new KETLError(e1);
        }

        // Do the get as normal
        try {
            myDatabase.delete(null, theKey);
        } catch (DatabaseException e) {
            throw new KETLError(e);
        }

        return null;

    }

    public int size() {
        try {
            this.myDatabase.sync();
            return (int) this.myDatabase.count();
        } catch (DatabaseException e) {
            e.printStackTrace();
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
            synchronized (lock) {
                strSize = NumberFormatter.format(myEnvironment == null ? 0 : myEnvironment.getStats(null)
                        .getCacheTotalBytes());
            }
        } catch (DatabaseException e) {
            strSize = "N/A";
        }

        return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
                + "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
                + java.util.Arrays.toString(this.mValueTypes) + "\n\tExample: Key->" + exampleKey + " Value->"
                + exampleValue + "\n\tCount: " + this.count + "\n\tConsolidated Cache Size: " + strSize;

    }

    public Collection values() {
        return null;
    }

    private static Object lock = new Object();

    private static void closeEnv() {
        synchronized (lock) {
            try {
                List dbs = myEnvironment.getDatabaseNames();

                dbs.removeAll(mValidCacheList);
                for (Object o : dbs) {
                    try {
                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                "Removing unreferenced cache " + o);
                        myEnvironment.removeDatabase(null, (String) o);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
                myEnvironment.sync();
                myEnvironment.close();
                myEnvironment = null;
            } catch (DatabaseException e) {
                ResourcePool.LogException(e, Thread.currentThread());
            }
        }
    }

    public void closeCacheEnvironment() {
        closeEnv();
    }

}
