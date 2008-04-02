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

// TODO: Auto-generated Javadoc
/**
 * The Class SleepycatIndexedMap.
 */
final public class SleepycatIndexedMap implements PersistentMap {

	/** The Constant ARRAY. */
	final static byte ARRAY = 8;

	/** The Constant BIGDECIMAL. */
	final static byte BIGDECIMAL = 13;

	/** The Constant DATE. */
	final static byte DATE = 9;

	/** The Constant DOUBLE. */
	final static byte DOUBLE = 4;

	/** The Constant FLOAT. */
	final static byte FLOAT = 3;

	/** The Constant INT. */
	final static byte INT = 1;

	/** The Constant LONG. */
	final static byte LONG = 2;

	/** The Constant NULL. */
	final static byte NULL = 7;

	/** The Constant OBJECT. */
	final static byte OBJECT = 6;

	/** The Constant SHORT. */
	final static byte SHORT = 0;

	/** The Constant SQLDATE. */
	final static byte SQLDATE = 11;

	/** The Constant STRING. */
	final static byte STRING = 5;

	/** The Constant TIME. */
	final static byte TIME = 10;

	/** The Constant TIMESTAMP. */
	final static byte TIMESTAMP = 12;

	/**
	 * Byte array to object.
	 * 
	 * @param buf
	 *            the buf
	 * @param off
	 *            the off
	 * @param length
	 *            the length
	 * 
	 * @return the object
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	private static Object byteArrayToObject(final byte[] buf, int off, final int length) throws IOException,
			ClassNotFoundException {

		byte type = buf[off++];

		if (type == SleepycatIndexedMap.NULL)
			return null;
		if (type == SleepycatIndexedMap.ARRAY) {
			int len = buf[off++];
			Object[] ar = new Object[len];
			int pos = off;
			for (int i = 0; i < len; i++) {
				int size = buf[pos++];
				ar[i] = SleepycatIndexedMap.byteArrayToObject(buf, pos, size);
				pos += size;
			}

			return ar;
		}
		if (type == SleepycatIndexedMap.INT) {
			return Bytes.unpack4(buf, off);
		}
		if (type == SleepycatIndexedMap.STRING) {
			return new String(buf, off, length - 1);
		}
		if (type == SleepycatIndexedMap.SHORT) {
			return Bytes.unpack2(buf, off);
		}
		if (type == SleepycatIndexedMap.DOUBLE) {
			return Bytes.unpackF8(buf, off);
		}
		if (type == SleepycatIndexedMap.LONG) {
			return Bytes.unpack8(buf, off);
		}
		if (type == SleepycatIndexedMap.FLOAT) {
			return Bytes.unpackF4(buf, off);
		}
		if (type == SleepycatIndexedMap.TIMESTAMP) {
			long t = Bytes.unpack8(buf, off);
			java.sql.Timestamp res = new java.sql.Timestamp(t);
			res.setNanos(Bytes.unpack4(buf, off + 8));
			return res;
		}
		if (type == SleepycatIndexedMap.BIGDECIMAL) {
			byte[] tmp = new byte[length - 5];
			int scale = Bytes.unpack4(buf, off);
			System.arraycopy(buf, off + 4, tmp, 0, length - 5);
			BigInteger bi = new BigInteger(tmp);
			return new BigDecimal(bi, scale);
		}
		if (type == SleepycatIndexedMap.SQLDATE) {
			return new java.sql.Date(Bytes.unpack8(buf, off));
		}
		if (type == SleepycatIndexedMap.TIME) {
			return new java.sql.Time(Bytes.unpack8(buf, off));
		}
		if (type == SleepycatIndexedMap.OBJECT) {
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
	/**
	 * Delete dir.
	 * 
	 * @param dir
	 *            the dir
	 * 
	 * @return true, if successful
	 */
	public static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (String element : children) {
				boolean success = SleepycatIndexedMap.deleteDir(new File(dir, element));
				if (!success) {
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}

	/**
	 * Obj to byte array.
	 * 
	 * @param obj
	 *            the obj
	 * 
	 * @return the byte[]
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private static byte[] objToByteArray(final Object obj) throws IOException {

		if (obj == null)
			return new byte[] { SleepycatIndexedMap.NULL };

		Class cl = obj.getClass();
		byte[] buf;

		if (cl.isArray()) {
			Object[] ar = (Object[]) obj;
			int len = ar.length;

			buf = new byte[] { SleepycatIndexedMap.ARRAY, (byte) len };
			for (int i = 0; i < len; i++) {
				byte[] tmp = SleepycatIndexedMap.objToByteArray(ar[i]);
				buf = Bytes.join(buf, new byte[] { (byte) tmp.length });
				buf = Bytes.join(buf, tmp);
			}

			return buf;
		}

		if (cl == Integer.class) {
			buf = new byte[5];
			buf[0] = SleepycatIndexedMap.INT;
			Bytes.pack4(buf, 1, ((Integer) obj));
		} else if (cl == String.class) {
			byte[] tmp = ((String) obj).getBytes();
			buf = new byte[tmp.length + 1];
			buf[0] = SleepycatIndexedMap.STRING;
			System.arraycopy(tmp, 0, buf, 1, tmp.length);
		} else if (cl == java.sql.Timestamp.class) {
			java.sql.Timestamp tmp = (java.sql.Timestamp) obj;

			buf = new byte[13];
			buf[0] = SleepycatIndexedMap.TIMESTAMP;
			Bytes.pack8(buf, 1, tmp.getTime());
			Bytes.pack4(buf, 9, tmp.getNanos());
		} else if (cl == java.math.BigDecimal.class) {
			BigDecimal bd = (BigDecimal) obj;
			byte[] tmp = bd.unscaledValue().toByteArray();
			buf = new byte[tmp.length + 5];
			buf[0] = SleepycatIndexedMap.BIGDECIMAL;
			int scale = bd.scale();
			Bytes.pack4(buf, 1, scale);
			System.arraycopy(tmp, 0, buf, 5, tmp.length);
		} else if (cl == java.sql.Date.class) {
			buf = new byte[9];
			buf[0] = SleepycatIndexedMap.SQLDATE;
			Bytes.pack8(buf, 1, ((java.sql.Date) obj).getTime());
		} else if (cl == java.sql.Time.class) {
			buf = new byte[9];
			buf[0] = SleepycatIndexedMap.TIME;
			Bytes.pack8(buf, 1, ((java.sql.Time) obj).getTime());
		} else if (cl == Short.class) {
			buf = new byte[3];
			buf[0] = SleepycatIndexedMap.SHORT;
			Bytes.pack2(buf, 1, ((Short) obj));
		} else if (cl == Long.class) {
			buf = new byte[9];
			buf[0] = SleepycatIndexedMap.LONG;
			Bytes.pack8(buf, 1, ((Long) obj));
		} else if (cl == Float.class) {
			buf = new byte[5];
			buf[0] = SleepycatIndexedMap.FLOAT;
			Bytes.packF4(buf, 1, ((Float) obj));
		} else if (cl == Double.class) {
			buf = new byte[9];
			buf[0] = SleepycatIndexedMap.DOUBLE;
			Bytes.packF8(buf, 1, ((Double) obj));
		} else {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			ObjectOutputStream objStream = new ObjectOutputStream(outStream);
			objStream.writeObject(obj);
			objStream.flush();
			outStream.flush();
			byte[] tmp = outStream.toByteArray();
			buf = new byte[tmp.length + 1];
			buf[0] = SleepycatIndexedMap.OBJECT;
			System.arraycopy(tmp, 0, buf, 1, tmp.length);
			objStream.close();
			outStream.close();
		}
		return buf;
	}

	/** The my environment. */
	private static Environment myEnvironment = null;

	/** The class db. */
	private String classDb;

	/** The commit count. */
	private int commitCount = 0;

	/** The count. */
	private int count = 0;

	/** The data db. */
	private String dataDb;

	/** The field index. */
	private HashMap fieldIndex = new HashMap();

	/** The cache dir. */
	private String mCacheDir = null;

	/** The key binding. */
	private TupleBinding mKeyBinding;

	/** The key is array. */
	private boolean mKeyIsArray;

	/** The key types. */
	private Class[] mKeyTypes;

	/** The name. */
	private String mName;

	/** The size. */
	private int mSize;

	/** The value fields. */
	private String[] mValueFields;

	/** The values is array. */
	private boolean mValuesIsArray;

	/** The value types. */
	private Class[] mValueTypes;

	/** The my database. */
	private Database myClassDb, myDatabase;

	/** The my db config. */
	private DatabaseConfig myDbConfig;

	/** The key length. */
	private int mKeyLength;

	/** The valid cache list. */
	private static List mValidCacheList;

	/**
	 * Instantiates a new sleepycat indexed map.
	 * 
	 * @param pName
	 *            the name
	 * @param pSize
	 *            the size
	 * @param pPersistanceID
	 *            the persistance ID
	 * @param pCacheDir
	 *            the cache dir
	 * @param pKeyTypes
	 *            the key types
	 * @param pValueTypes
	 *            the value types
	 * @param pValueFields
	 *            the value fields
	 * @param pPurgeCache
	 *            the purge cache
	 */
	public SleepycatIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes,
			Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
		super();

		this.mSize = pSize;
		this.mName = pName;
		this.dataDb = this.mName + (pPersistanceID == null ? "" : pPersistanceID);
		this.classDb = this.mName + "classDb" + (pPersistanceID == null ? "" : pPersistanceID);
		this.mCacheDir = pCacheDir;
		this.mKeyTypes = pKeyTypes;
		this.mValueTypes = pValueTypes;
		this.mKeyIsArray = pKeyTypes.length == 1 ? false : true;
		this.mValueFields = pValueFields;
		this.mKeyLength = this.mKeyTypes.length;
		/*
		 * if (this.mKeyIsArray == false) { Class cl = pKeyTypes[0]; if (cl ==
		 * BigInteger.class) this.mKeyBinding = new BigIntegerBinding(); if (cl ==
		 * Boolean.class) this.mKeyBinding = new BooleanBinding(); if (cl ==
		 * Double.class) this.mKeyBinding = new DoubleBinding(); if (cl ==
		 * Float.class) this.mKeyBinding = new FloatBinding(); if (cl ==
		 * Integer.class) this.mKeyBinding = new IntegerBinding(); if (cl ==
		 * Long.class) this.mKeyBinding = new LongBinding(); if (cl ==
		 * Short.class) this.mKeyBinding = new ShortBinding(); if (cl ==
		 * String.class) this.mKeyBinding = new StringBinding(); }
		 */
		this.mValuesIsArray = pValueTypes.length == 1 ? false : true;
		for (int i = 0; i < pValueFields.length; i++) {
			this.fieldIndex.put(pValueFields[i], i);
		}

		// initialize the cache
		this.init(pPurgeCache);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#clear()
	 */
	public void clear() {
		// reinitialize the cache
		this.init(true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#close()
	 */
	public void close() {
		if (this.myDatabase != null) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#commit(boolean)
	 */
	public synchronized void commit(boolean force) {

		if (force || this.commitCount > 0) {

			if (force == false) {
				Runtime r = Runtime.getRuntime();
				long free = (r.maxMemory() - (r.totalMemory() - r.freeMemory()));
				force = free < (10 * 1024 * 1024);
			}
			if (force) {
				try {
					this.commitCount = 0;
					ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + this.mName
							+ ", Approximate Count: " + this.myDatabase.count());

					this.myDatabase.sync();
				} catch (DatabaseException e) {
					throw new KETLError(e);
				}
			}

		}
	}

	/**
	 * Configure environment.
	 * 
	 * @param cacheDir
	 *            the cache dir
	 * 
	 * @throws DatabaseException
	 *             the database exception
	 */
	private static void configureEnvironment(String cacheDir) throws DatabaseException {

		synchronized (SleepycatIndexedMap.lock) {
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			SleepycatIndexedMap.myEnvironment = new Environment(new File(cacheDir), envConfig);

			EnvironmentMutableConfig envMutableConfig = new EnvironmentMutableConfig();
			int pct = (int) (EngineConstants.getCacheMemoryRatio() * 100);

			envMutableConfig.setCachePercent(pct);
			envMutableConfig.setTxnWriteNoSync(true);
			SleepycatIndexedMap.myEnvironment.setMutableConfig(envMutableConfig);

			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Setting cache size to "
					+ NumberFormatter.format((long) (Runtime.getRuntime().maxMemory() * EngineConstants
							.getCacheMemoryRatio())));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	public boolean containsKey(Object key) {
		return this.get(key) != null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#delete()
	 */
	public synchronized void delete() {
		this.deleteCache();
	}

	/**
	 * Delete cache.
	 */
	public synchronized void deleteCache() {

		try {
			if (this.myDatabase != null) {
				this.myDatabase.sync();
				this.myClassDb.sync();
				this.myDatabase.close();
				this.myClassDb.close();
			}

			SleepycatIndexedMap.removeDatabase(this.dataDb, this.classDb);
		} catch (DatabaseException e) {
			throw new KETLError(e);
		} finally {
			this.myDatabase = null;
			this.myClassDb = null;
		}

	}

	/**
	 * Removes the database.
	 * 
	 * @param dataDb
	 *            the data db
	 * @param classDb
	 *            the class db
	 * 
	 * @throws DatabaseException
	 *             the database exception
	 */
	private static void removeDatabase(String dataDb, String classDb) throws DatabaseException {
		synchronized (SleepycatIndexedMap.lock) {
			List dbs = SleepycatIndexedMap.myEnvironment.getDatabaseNames();
			if (dbs.contains(dataDb))
				SleepycatIndexedMap.myEnvironment.removeDatabase(null, dataDb);
			if (dbs.contains(classDb))
				SleepycatIndexedMap.myEnvironment.removeDatabase(null, classDb);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#entrySet()
	 */
	public Set entrySet() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#get(java.lang.Object)
	 */
	public Object get(Object key) {
		return this.get(key, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#get(java.lang.Object,
	 *      java.lang.String)
	 */
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

		return (res)[(Integer) idx];

	}

	/**
	 * Gets the cache directory.
	 * 
	 * @return the cache directory
	 */
	private String getCacheDirectory() {
		File fDir = new File(this.mCacheDir + File.separator + "KETL." + ResourcePool.getCacheIndexPrefix() + ".cache");

		if (fDir.exists() && fDir.isDirectory())
			return fDir.getAbsolutePath();
		else if (fDir.exists() == false) {
			fDir.mkdir();
			return fDir.getAbsolutePath();
		} else
			throw new KETLError("Cache directory is already in use");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getCacheSize()
	 */
	public int getCacheSize() {
		return this.mSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getItem(java.lang.Object)
	 */
	public Object getItem(Object pkey) throws DatabaseException, IOException, ClassNotFoundException {
		// Create the DatabaseEntry for the key

		DatabaseEntry theKey = this.getKey(pkey);
		DatabaseEntry theData = new DatabaseEntry();

		// Do the get as normal
		if (this.myDatabase.get(null, theKey, theData, LockMode.READ_UNCOMMITTED) == OperationStatus.NOTFOUND)
			return null;

		// Recreate the MyData object from the retrieved DatabaseEntry using
		// the EntryBinding created above

		byte[] res = theData.getData();
		if (res == null)
			return null;
		return this.mValuesIsArray == false ? new Object[] { SleepycatIndexedMap.byteArrayToObject(res, 0, res.length) }
				: SleepycatIndexedMap.byteArrayToObject(res, 0, res.length);
	}

	/**
	 * Cast key.
	 * 
	 * @param key
	 *            the key
	 * 
	 * @return the object
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private Object castKey(Object[] key) throws IOException {
		if (key == null)
			return null;

		for (int i = 0; i < this.mKeyLength; i++) {
			key[i] = this.castKeyElement(key[i], this.mKeyTypes[i]);
		}

		return key;
	}

	/**
	 * Cast key element.
	 * 
	 * @param obj
	 *            the obj
	 * @param cl
	 *            the cl
	 * 
	 * @return the object
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private Object castKeyElement(Object obj, Class cl) throws IOException {

		if (obj == null)
			return null;

		Class clFrom = obj.getClass();

		if (cl == clFrom) {
			return obj;
		}

		if (cl.isArray()) {
			throw new IOException("Key cannot be cast to an Array, incoming class " + clFrom.getCanonicalName());
		}

		if (clFrom == Double.class || clFrom == Long.class || clFrom == Integer.class || clFrom == Short.class
				|| clFrom == Float.class || clFrom == java.math.BigDecimal.class || obj instanceof java.lang.Number) {
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
				return obj;
			}

			if (cl == String.class) {
				return obj;
			}

			if (cl == java.sql.Timestamp.class) {
				return obj;
			}

			if (cl == java.math.BigDecimal.class) {
				return obj;
			}

			if (cl == java.sql.Date.class) {
				return obj;
			}

			if (cl == java.sql.Time.class) {
				return obj;
			}

			if (cl == Short.class) {
				return obj;
			}

			if (cl == Long.class) {
				return obj;
			}

			if (cl == Float.class) {
				return obj;
			}

			if (cl == Double.class) {
				return obj;
			}
		} catch (ClassCastException e) {
			throw new IOException("Cache '" + this.getName() + "' error: Class of incoming key "
					+ clFrom.getCanonicalName() + " cannot be cast to expected to data type " + cl.getCanonicalName()
					+ ", " + e.getMessage());
		}

		throw new IOException("Cache '" + this.getName() + "' error: Class of incoming key "
				+ clFrom.getCanonicalName() + " cannot be cast to expected to data type " + cl.getCanonicalName());

	}

	/**
	 * Gets the key.
	 * 
	 * @param val
	 *            the val
	 * 
	 * @return the key
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private DatabaseEntry getKey(Object val) throws IOException {

		val = this.castKey((Object[]) val);

		if (this.mKeyIsArray == false)
			return new DatabaseEntry(SleepycatIndexedMap.objToByteArray(val == null ? null : ((Object[]) val)[0]));

		// if (this.mKeyBinding == null)
		return new DatabaseEntry(SleepycatIndexedMap.objToByteArray((val)));

		// DatabaseEntry entry = new DatabaseEntry();
		// this.mKeyBinding.objectToEntry((val == null ? null : (Object[])
		// val)[0], entry);
		// return entry;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getKeyTypes()
	 */
	public Class[] getKeyTypes() {
		return this.mKeyTypes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getName()
	 */
	public String getName() {
		return this.mName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getStorageClass()
	 */
	public Class getStorageClass() {
		return this.getClass();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getValueFields()
	 */
	public String[] getValueFields() {
		return this.mValueFields;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#getValueTypes()
	 */
	public Class[] getValueTypes() {
		return this.mValueTypes;
	}

	/**
	 * Init.
	 * 
	 * @param purge
	 *            the purge
	 */
	void init(boolean purge) {

		SleepycatIndexedMap.initEnvironment(this.getCacheDirectory());

		SleepycatIndexedMap.mValidCacheList.add(this.dataDb);
		SleepycatIndexedMap.mValidCacheList.add(this.classDb);

		if (purge)
			this.deleteCache();

		if (this.myDbConfig == null) {
			this.myDbConfig = new DatabaseConfig();
			this.myDbConfig.setAllowCreate(true);
			this.myDbConfig.setDeferredWrite(true);
		}

		try {
			synchronized (SleepycatIndexedMap.lock) {
				this.myDatabase = SleepycatIndexedMap.myEnvironment.openDatabase(null, this.dataDb, this.myDbConfig);
				this.myClassDb = SleepycatIndexedMap.myEnvironment.openDatabase(null, this.classDb, this.myDbConfig);
				List ls = SleepycatIndexedMap.myEnvironment.getDatabaseNames();
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

	/**
	 * Inits the environment.
	 * 
	 * @param cacheDir
	 *            the cache dir
	 */
	private static synchronized void initEnvironment(String cacheDir) {
		synchronized (SleepycatIndexedMap.lock) {
			if (SleepycatIndexedMap.myEnvironment == null) {
				SleepycatIndexedMap.mValidCacheList = java.util.Collections.synchronizedList(new ArrayList());
				try {
					SleepycatIndexedMap.configureEnvironment(cacheDir);
				} catch (DatabaseException dbe) {
					throw new KETLError(dbe);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#isEmpty()
	 */
	public boolean isEmpty() {
		return this.size() == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#keySet()
	 */
	public Set keySet() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#put(java.lang.Object,
	 *      java.lang.Object)
	 */
	public Object put(Object pkey, Object pValue) {

		try {

			// Create the DatabaseEntry for the key
			DatabaseEntry theKey = this.getKey(pkey);

			// Create the DatabaseEntry for the data. Use the EntryBinding
			// object
			// that was just created to populate the DatabaseEntry
			DatabaseEntry theData = new DatabaseEntry(SleepycatIndexedMap.objToByteArray(this.mValuesIsArray ? pValue
					: ((Object[]) pValue)[0]));

			// Put it as normal
			if (this.myDatabase.putNoOverwrite(null, theKey, theData) == OperationStatus.SUCCESS)
				this.commitCount++;

			if (this.commitCount >= 200000) {
				this.commit(false);
			}

		} catch (Exception e) {
			throw new KETLError(e);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	public void putAll(Map t) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#remove(java.lang.Object)
	 */
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
			this.myDatabase.delete(null, theKey);
		} catch (DatabaseException e) {
			throw new KETLError(e);
		}

		return null;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#size()
	 */
	public int size() {
		try {
			this.myDatabase.sync();
			return (int) this.myDatabase.count();
		} catch (DatabaseException e) {
			e.printStackTrace();
			throw new KETLError(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#switchToReadOnlyMode()
	 */
	public void switchToReadOnlyMode() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String exampleValue = "N/A", exampleKey = "N/A";
		String strSize;
		try {
			synchronized (SleepycatIndexedMap.lock) {
				strSize = NumberFormatter.format(SleepycatIndexedMap.myEnvironment == null ? 0
						: SleepycatIndexedMap.myEnvironment.getStats(null).getCacheTotalBytes());
			}
		} catch (DatabaseException e) {
			strSize = "N/A";
		}

		return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
				+ "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
				+ java.util.Arrays.toString(this.mValueTypes) + "\n\tExample: Key->" + exampleKey + " Value->"
				+ exampleValue + "\n\tConsolidated Cache Size: " + strSize;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#values()
	 */
	public Collection values() {
		return null;
	}

	/** The lock. */
	private static Object lock = new Object();

	/**
	 * Close env.
	 */
	private static void closeEnv() {
		synchronized (SleepycatIndexedMap.lock) {
			try {
				List dbs = SleepycatIndexedMap.myEnvironment.getDatabaseNames();

				dbs.removeAll(SleepycatIndexedMap.mValidCacheList);
				for (Object o : dbs) {
					try {
						ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
								"Removing unreferenced cache " + o);
						SleepycatIndexedMap.myEnvironment.removeDatabase(null, (String) o);
					} catch (Throwable e) {
						e.printStackTrace();
					}
				}
				SleepycatIndexedMap.myEnvironment.sync();
				SleepycatIndexedMap.myEnvironment.close();
				SleepycatIndexedMap.myEnvironment = null;
			} catch (DatabaseException e) {
				ResourcePool.LogException(e, Thread.currentThread());
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.lookup.PersistentMap#closeCacheEnvironment()
	 */
	public void closeCacheEnvironment() {
		SleepycatIndexedMap.closeEnv();
	}

}
