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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.lookup.LookupUtil.ByteArrayWrapper;
import com.kni.etl.util.LRUCache;

// TODO: Auto-generated Javadoc
/**
 * The Class CachedIndexedMap.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
final public class CachedIndexedMap implements PersistentMap {

    /**
     * The Class LRU.
     */
    final class LRU extends LRUCache {

        /* (non-Javadoc)
         * @see com.kni.etl.util.LRUCache#removingEntry(java.util.Map.Entry)
         */
        @Override
        protected void removingEntry(Entry eldest)  {
            Object pValue = eldest.getValue();
            ByteArrayWrapper buf = (ByteArrayWrapper) eldest.getKey();
            try {
    			Object pkey = LookupUtil.byteArrayToObject(buf);
            	CachedIndexedMap.this.mParentCache.put( pkey, pValue);
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}

            super.removingEntry(eldest);

        }

        /**
         * Instantiates a new LRU.
         * 
         * @param cacheSize the cache size
         */
        public LRU(int cacheSize) {
            super(2);
            // TODO Auto-generated constructor stub
        }

        /**
         * Size memory only.
         * 
         * @return the int
         */
        public synchronized int sizeMemoryOnly() {
            int cnt = 0;
            for (Object o : this.getBackingMap().keySet()) {
                if (CachedIndexedMap.this.mKeyIsArray) {
                        cnt++;
                }
            }

            return cnt;
        }

        /**
         * Flush.
         * @throws ClassNotFoundException 
         * @throws IOException 
         */
        public void flush() throws IOException, ClassNotFoundException {
            Object[] vals =  this.getBackingMap().keySet().toArray();
            for (Object b : vals) {
            	Object pKey = LookupUtil.byteArrayToObject((ByteArrayWrapper)b);
                CachedIndexedMap.this.mParentCache.put(pKey,  this.get(b));
            }
        }

    }


    /**
     * Gets the approximate class size.
     * 
     * @param cl the cl
     * 
     * @return the approximate class size
     * 
     * @throws Error the error
     */
    static private int getApproximateClassSize(Class cl) throws Error {

        if (cl == Double.class)
            return Double.SIZE;
        if (cl == Integer.class)
            return Integer.SIZE;
        if (cl == String.class)
            return 256;
        if (cl == Long.class)
            return Long.SIZE;
        if (cl == Short.class)
            return Short.SIZE;
        if (cl == Float.class)
            return Float.SIZE;
        if (cl == java.util.Date.class)
            return Long.SIZE + 4;

        if (cl == java.sql.Date.class)
            return Long.SIZE + 4 + Integer.SIZE;
        if (cl == java.sql.Time.class)
            return Long.SIZE + 4 + Integer.SIZE;
        if (cl == java.sql.Timestamp.class)
            return Long.SIZE + 4 + Integer.SIZE;
        if (cl == byte[].class)
            return 256;
        if (cl == BigDecimal.class) {
            return Long.SIZE * 2;
        }

        return 64;
    }

    /** The cache size. */
    private int mCacheSize;

    /** The field index. */
    private HashMap mFieldIndex = new HashMap();

    
    /** The key is array. */
    private boolean mKeyIsArray;

    /** The LRU. */
    private LRU mLRU;

    /** The parent cache. */
    private PersistentMap mParentCache;

    /** The value is single. */
    private boolean mValueIsSingle;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#get(java.lang.Object)
     */

    /**
     * Instantiates a new cached indexed map.
     * 
     * @param pParentCache the parent cache
     */
    public CachedIndexedMap(PersistentMap pParentCache) {
        super();

        this.mParentCache = pParentCache;
        this.mKeyIsArray = true;// pKeyTypes.length == 1 ? false : true;
        this.mValueIsSingle = pParentCache.getValueTypes().length == 1;

        int elementSize = 4; // add node footprint
        for (int i = 0; i < pParentCache.getValueFields().length; i++) {
            elementSize += CachedIndexedMap.getApproximateClassSize(pParentCache.getValueTypes()[i]);
            this.mFieldIndex.put(pParentCache.getValueFields()[i], i);
        }
        for (int i = 0; i < pParentCache.getKeyTypes().length; i++) {
            elementSize += CachedIndexedMap.getApproximateClassSize(pParentCache.getKeyTypes()[i]);
        }

        this.mCacheSize = pParentCache.getCacheSize() / elementSize;

        this.mLRU = new LRU(this.mCacheSize);

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Level 1 Cache '"
                + pParentCache.getName() + "' will contain " + this.mCacheSize + " element(s)");

    }

    /* (non-Javadoc)
     * @see java.util.Map#clear()
     */
    public void clear() {
        this.mLRU.clear();
        this.mParentCache.clear();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#commit(boolean)
     */
    public synchronized void commit(boolean force) throws IOException, ClassNotFoundException {
        this.mLRU.flush();
        this.mParentCache.commit(false);
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        return this.get(key) != null || this.mParentCache.get(key) != null;
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#delete()
     */
    public synchronized void delete() {
        this.deleteCache();
        this.mParentCache.delete();
    }

    /**
     * Delete cache.
     */
    public synchronized void deleteCache() {
        this.mLRU = new LRU(this.mCacheSize);
    }

    /* (non-Javadoc)
     * @see java.util.Map#entrySet()
     */
    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see java.util.Map#get(java.lang.Object)
     */
    public Object get(Object key) throws Error {
        try {
            return this.get(key, null);
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#get(java.lang.Object, java.lang.String)
     */
    public Object get(Object pkey, String pField) throws IOException {

        Object res = this.getItem(pkey);

        if (res == null) {
            try {
                res = this.mParentCache.getItem(pkey);
            } catch (Exception e) {
                throw new KETLError(e);
            }

            // if found then add to LRU
            if (res == null)
                return null;
            else
                this.put(pkey, res, false);

        }

        Integer idx;
        if (this.mValueIsSingle)
            idx = 0;
        else
            idx = (Integer) this.mFieldIndex.get(pField);

        if (idx == null)
            throw new KETLError("Key " + pField + " does not exist in lookup");

        return ((Object[]) res)[idx];

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getItem(java.lang.Object)
     */
    public Object getItem(Object pkey) throws IOException {
        return this.mLRU.get(LookupUtil.objToWrappedByteArray(pkey));
    }

    /* (non-Javadoc)
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return this.size() == 0;
    }

    /* (non-Javadoc)
     * @see java.util.Map#keySet()
     */
    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object pkey, Object pValue) {

        try {
			return this.put(pkey, pValue, true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

    /**
     * Put.
     * 
     * @param pkey the pkey
     * @param pValue the value
     * @param dirty the dirty
     * 
     * @return the object
     * @throws IOException 
     */
    final private Object put(Object pkey, Object pValue, boolean dirty) throws IOException {

        // local hashed cached
        this.mLRU.put(LookupUtil.objToWrappedByteArray(pkey), pValue);
        // this.mParentCache.put(pkey, pValue);

        return null;
    }

    /* (non-Javadoc)
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map t) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see java.util.Map#remove(java.lang.Object)
     */
    public Object remove(Object pkey) {
        // local hashed cached
        this.mLRU.remove(pkey);
        this.mParentCache.remove(pkey);
        return null;
    }

    /* (non-Javadoc)
     * @see java.util.Map#size()
     */
    public int size() {
        return this.mParentCache.size();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#switchToReadOnlyMode()
     */
    public void switchToReadOnlyMode() {
        this.mParentCache.switchToReadOnlyMode();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        try {
			this.mLRU.flush();
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
        return this.mParentCache.toString();
    }

    /* (non-Javadoc)
     * @see java.util.Map#values()
     */
    public Collection values() {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getCacheSize()
     */
    public int getCacheSize() {
        return this.mParentCache.getCacheSize();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getKeyTypes()
     */
    public Class[] getKeyTypes() {
        return this.mParentCache.getKeyTypes();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getName()
     */
    public String getName() {
        return this.mParentCache.getName();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getValueFields()
     */
    public String[] getValueFields() {
        return this.mParentCache.getValueFields();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getValueTypes()
     */
    public Class[] getValueTypes() {
        return this.mParentCache.getValueTypes();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getStorageClass()
     */
    public Class getStorageClass() {
        return this.mParentCache.getStorageClass();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#close()
     */
    public void close() {
        this.mParentCache.close();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#closeCacheEnvironment()
     */
    public void closeCacheEnvironment() {
        this.mParentCache.closeCacheEnvironment();
    }

}
