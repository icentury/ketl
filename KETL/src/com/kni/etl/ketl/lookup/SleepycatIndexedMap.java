/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Sep 25, 2006
 * 
 */
package com.kni.etl.ketl.lookup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.bind.tuple.*;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
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
 * @author nwakefield To change the template for this generated type comment go
 *         to Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and
 *         Comments
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

		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
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

	private boolean mValuesIsArray;

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

	private int mSize;

	private boolean mKeyIsArray;

	private static Environment myEnvironment = null;

	public SleepycatIndexedMap(String pName, int pSize, Integer pPersistanceID,
			String pCacheDir, Class[] pKeyTypes, Class[] pValueTypes,
			String[] pValueFields, boolean pPurgeCache) {
		super();

		mSize = pSize;
		mPersistanceID = pPersistanceID;
		mName = pName;
		this.mCacheDir = pCacheDir;
		this.mKeyTypes = pKeyTypes;
		this.mValueTypes = pValueTypes;
		this.mKeyIsArray = pKeyTypes.length == 1 ? false : true;

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

		mValuesIsArray = pValueTypes.length == 1 ? false : true;
		this.mValueFields = pValueFields;
		for (int i = 0; i < pValueFields.length; i++) {
			fieldIndex.put(pValueFields[i], i);
		}

		// initialize the cache
		init();

	}

	private boolean dbClearPending = false;

	public void clear() {
		// reinitialize the cache
		this.init();
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
					ResourcePool.LogMessage(Thread.currentThread(),
							ResourcePool.INFO_MESSAGE, "Lookup '" + mName
									+ ", Approximate Count: "
									+ this.myDatabase.count());

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
				this.myDatabase.sync();
				this.myClassDb.sync();
				String nm = myDatabase.getDatabaseName();
				myDatabase.close();
				myEnvironment.removeDatabase(null, nm);

				nm = this.myClassDb.getDatabaseName();
				myClassDb.close();
				myEnvironment.removeDatabase(null, nm);
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
		try {
			res = this.getItem(pkey);
		} catch (Exception e) {
			throw new KETLError(e);
		}
		if (res == null)
			return null;

		if (this.mValuesIsArray == false)
			return res;

		Object idx = this.fieldIndex.get(pField);

		if (idx == null)
			throw new KETLError("Key " + pField + " does not exist in lookup");

		return ((Object[]) res)[(Integer) idx];

	}

	private String getCacheDirectory() {
		File fDir;
		if (this.mPersistanceID == null)
			fDir = new File(mCacheDir + File.separator + "KETL." + mName
					+ "cache");
		else
			fDir = new File(mCacheDir + File.separator + "KETL." + mName + "."
					+ this.mPersistanceID + ".cache");

		if (fDir.exists() && fDir.isDirectory())
			return fDir.getAbsolutePath();
		else if (fDir.exists() == false) {
			fDir.mkdir();
			return fDir.getAbsolutePath();
		} else
			throw new KETLError("Cache directory is already in use");

	}

	private Database myClassDb, myDatabase;

	private DatabaseConfig myDbConfig;

	void init() {

		this.deleteCache();

		if (myEnvironment == null) {
			try {
				configureEnvironment();
			} catch (DatabaseException dbe) {
				throw new KETLError(dbe);
			}
		}

		if (this.myDbConfig == null) {
			myDbConfig = new DatabaseConfig();
			myDbConfig.setAllowCreate(true);
			myDbConfig.setDeferredWrite(true);
		}

		try {
			myDatabase = myEnvironment.openDatabase(null, mName, myDbConfig);
			myClassDb = myEnvironment.openDatabase(null, mName + "classDb",
					myDbConfig);
			List ls = myEnvironment.getDatabaseNames();
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.DEBUG_MESSAGE, "Cache db's: " + ls.toString());
			this.myDatabase.sync();
			this.myClassDb.sync();

		} catch (DatabaseException dbe) {
			throw new KETLError(dbe);
		}

		// Instantiate the class catalog
		// StoredClassCatalog classCatalog = new StoredClassCatalog(myClassDb);

		if (dbClearPending) {
			this.clear();
			dbClearPending = false;
		}

		ResourcePool.LogMessage(Thread.currentThread(),
				ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
						+ this.mName + ", Key Type(s):"
						+ java.util.Arrays.toString(this.mKeyTypes)
						+ ", Key Field(s):"
						+ java.util.Arrays.toString(this.mValueFields)
						+ ", Result Type(s):"
						+ java.util.Arrays.toString(this.mValueTypes));
	}

	private void configureEnvironment() throws DatabaseException {

		File dir = new File(this.getCacheDirectory());

		if (dir.exists())
			deleteDir(dir);

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		myEnvironment = new Environment(new File(this.getCacheDirectory()),
				envConfig);

		EnvironmentMutableConfig envMutableConfig = new EnvironmentMutableConfig();
		int pct = (int) (EngineConstants.getCacheMemoryRatio() * 100);

		envMutableConfig.setCachePercent(pct);
		envMutableConfig.setTxnWriteNoSync(true);
		myEnvironment.setMutableConfig(envMutableConfig);

		ResourcePool.LogMessage(Thread.currentThread(),
				ResourcePool.INFO_MESSAGE, "Setting cache size to "
						+ NumberFormatter.format((long) (Runtime.getRuntime()
								.maxMemory() * EngineConstants
								.getCacheMemoryRatio())));
	}

	public boolean isEmpty() {
		return this.size() == 0;
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
			DatabaseEntry theData = new DatabaseEntry(objToByteArray(pValue));

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
			// TODO Auto-generated catch block
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
			strSize = NumberFormatter.format(myEnvironment == null ? 0
					: myEnvironment.getStats(null).getCacheTotalBytes());
		} catch (DatabaseException e) {
			strSize = "N/A";
		}
		// TODO Auto-generated method stub
		return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):"
				+ java.util.Arrays.toString(this.mKeyTypes)
				+ "\n\tKey Field(s):"
				+ java.util.Arrays.toString(this.mValueFields)
				+ "\n\tResult Type(s):"
				+ java.util.Arrays.toString(this.mValueTypes)
				+ "\n\tExample: Key->" + exampleKey + " Value->" + exampleValue
				+ "\n\tCount: " + this.count + "\n\tConsolidated Cache Size: "
				+ strSize;

	}

	public Collection values() {
		return null;
	}

	public Object getItem(Object pkey) throws DatabaseException, IOException,
			ClassNotFoundException {
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
		return byteArrayToObject(res, 0, res.length);
	}

	private TupleBinding mKeyBinding;

	private DatabaseEntry getKey(Object val) throws IOException {
		if (this.mKeyBinding == null)
			return new DatabaseEntry(objToByteArray((val)));

		DatabaseEntry entry = new DatabaseEntry();
		this.mKeyBinding.objectToEntry(
				(val == null ? null : (Object[]) val)[0], entry);
		return entry;

	}

	final static byte SHORT = 0;

	final static byte INT = 1;

	final static byte LONG = 2;

	final static byte FLOAT = 3;

	final static byte DOUBLE = 4;

	final static byte STRING = 5;

	final static byte OBJECT = 6;

	final static byte NULL = 7;

	final static byte ARRAY = 8;

	final static byte DATE = 9;

	final static byte TIME = 10;

	final static byte SQLDATE = 11;

	final static byte TIMESTAMP = 12;

	final static byte BIGDECIMAL = 13;

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
		} else if (cl == String.class) {
			byte[] tmp = ((String) obj).getBytes();
			buf = new byte[tmp.length + 1];
			buf[0] = STRING;
			System.arraycopy(tmp, 0, buf, 1, tmp.length);
		} else if (cl == java.sql.Timestamp.class) {
			java.sql.Timestamp tmp = (java.sql.Timestamp) obj;

			buf = new byte[13];
			buf[0] = TIMESTAMP;
			Bytes.pack8(buf, 1, tmp.getTime());
			Bytes.pack4(buf, 9, tmp.getNanos());
		} else if (cl == java.math.BigDecimal.class) {
			BigDecimal bd = (BigDecimal) obj;
			byte[] tmp = bd.unscaledValue().toByteArray();
			buf = new byte[tmp.length + 5];
			buf[0] = BIGDECIMAL;
			int scale = bd.scale();
			Bytes.pack4(buf, 1, scale);
			System.arraycopy(tmp, 0, buf, 5, tmp.length);
		} else if (cl == java.sql.Date.class) {
			buf = new byte[9];
			buf[0] = SQLDATE;
			Bytes.pack8(buf, 1, ((java.sql.Date) obj).getTime());
		} else if (cl == java.sql.Time.class) {
			buf = new byte[9];
			buf[0] = TIME;
			Bytes.pack8(buf, 1, ((java.sql.Time) obj).getTime());
		} else if (cl == Short.class) {
			buf = new byte[3];
			buf[0] = SHORT;
			Bytes.pack2(buf, 1, ((Short) obj));
		} else if (cl == Long.class) {
			buf = new byte[9];
			buf[0] = LONG;
			Bytes.pack8(buf, 1, ((Long) obj));
		} else if (cl == Float.class) {
			buf = new byte[5];
			buf[0] = FLOAT;
			Bytes.packF4(buf, 1, ((Float) obj));
		} else if (cl == Double.class) {
			buf = new byte[9];
			buf[0] = DOUBLE;
			Bytes.packF8(buf, 1, ((Double) obj));
		} else {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			ObjectOutputStream objStream = new ObjectOutputStream(outStream);
			objStream.write(OBJECT);
			objStream.writeObject(obj);
			objStream.flush();
			outStream.flush();
			buf = outStream.toByteArray();
			objStream.close();
			outStream.close();
		}
		return buf;
	}

	public static Object byteArrayToObject(byte[] buf, int off, int length)
			throws IOException, ClassNotFoundException {

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
			ByteArrayInputStream outStream = new ByteArrayInputStream(buf, off,
					length - 1);
			ObjectInputStream objStream = new ObjectInputStream(outStream);
			Object obj = objStream.readObject();
			objStream.close();
			outStream.close();
			return obj;
		}
		throw new IOException("Invalid type found in data stream");
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
