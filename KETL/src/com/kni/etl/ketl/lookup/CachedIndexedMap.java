/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Sep 25, 2006
 * 
 */
package com.kni.etl.ketl.lookup;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.util.LRUCache;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
final public class CachedIndexedMap implements PersistentMap {

    final class LRU extends LRUCache {

        @Override
        protected void removingEntry(Entry eldest) {
            Object pValue = eldest.getValue();
            Object pkey = eldest.getKey();

            if (mKeyIsArray) {
                if (((HashWrapper) pkey).dirty) {
                    mParentCache.put(((HashWrapper) pkey).data, pValue);
                    ((HashWrapper) pkey).dirty = false;
                }
            }
            else
                mParentCache.put(pkey, pValue);

            super.removingEntry(eldest);

        }

        public LRU(int cacheSize) {
            super(2);
            // TODO Auto-generated constructor stub
        }

        public synchronized int sizeMemoryOnly() {
            int cnt = 0;
            for (Object o : this.getBackingMap().keySet()) {
                if (mKeyIsArray) {
                    if (((HashWrapper) o).dirty) {
                        cnt++;
                    }
                }
            }

            return cnt;
        }

        public void flush() {
            Object[] vals = this.getBackingMap().keySet().toArray();
            for (Object pKey : vals) {
                Object pValue = this.get(pKey);
                if (mKeyIsArray) {
                    if (((HashWrapper) pKey).dirty) {
                        mParentCache.put(((HashWrapper) pKey).data, pValue);
                        ((HashWrapper) pKey).dirty = false;
                    }
                }
                else
                    mParentCache.put(pKey, pValue);
            }
        }

    }

    final class HashWrapper {

        Object[] data;
        boolean dirty;

        public HashWrapper(Object[] data, boolean dirty2) {
            super();
            this.data = data;
            this.dirty = dirty2;
        }

        @Override
        public boolean equals(Object obj) {
            return java.util.Arrays.equals(data, ((HashWrapper) obj).data);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(data);
        }

    }

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

    private int mCacheSize;

    private HashMap mFieldIndex = new HashMap();

    private HashWrapper mHWR = new HashWrapper(null, true);
    private boolean mKeyIsArray;

    private LRU mLRU;

    private PersistentMap mParentCache;

    private boolean mValueIsSingle;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#get(java.lang.Object)
     */

    public CachedIndexedMap(PersistentMap pParentCache) {
        super();

        this.mParentCache = pParentCache;
        this.mKeyIsArray = true;// pKeyTypes.length == 1 ? false : true;
        this.mValueIsSingle = pParentCache.getValueTypes().length == 1;

        int elementSize = 4; // add node footprint
        for (int i = 0; i < pParentCache.getValueFields().length; i++) {
            elementSize += getApproximateClassSize(pParentCache.getValueTypes()[i]);
            mFieldIndex.put(pParentCache.getValueFields()[i], i);
        }
        for (int i = 0; i < pParentCache.getKeyTypes().length; i++) {
            elementSize += getApproximateClassSize(pParentCache.getKeyTypes()[i]);
        }

        this.mCacheSize = pParentCache.getCacheSize() / elementSize;

        this.mLRU = new LRU(this.mCacheSize);

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Level 1 Cache '"
                + pParentCache.getName() + "' will contain " + mCacheSize + " element(s)");

    }

    public void clear() {
        this.mLRU.clear();
        this.mParentCache.clear();
    }

    public synchronized void commit(boolean force) {
        this.mLRU.flush();
        this.mParentCache.commit(false);
    }

    public boolean containsKey(Object key) {
        return this.get(key) != null || this.mParentCache.get(key) != null;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public synchronized void delete() {
        this.deleteCache();
        this.mParentCache.delete();
    }

    public synchronized void deleteCache() {
        this.mLRU = new LRU(mCacheSize);
    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    public Object get(Object key) throws Error {
        try {
            return this.get(key, null);
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

    public Object get(Object pkey, String pField) {

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

    public Object getItem(Object pkey) {
        if (this.mKeyIsArray) {
            mHWR.data = (Object[]) pkey;
            return this.mLRU.get(mHWR);
        }

        return this.mLRU.get(pkey);

    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object pkey, Object pValue) {

        return this.put(pkey, pValue, true);
    }

    final private Object put(Object pkey, Object pValue, boolean dirty) {

        // local hashed cached
        this.mLRU.put(this.mKeyIsArray ? new HashWrapper((Object[]) pkey, dirty) : pkey, pValue);
        // this.mParentCache.put(pkey, pValue);

        return null;
    }

    public void putAll(Map t) {
        throw new UnsupportedOperationException();
    }

    public Object remove(Object pkey) {
        // local hashed cached
        this.mLRU.remove(pkey);
        this.mParentCache.remove(pkey);
        return null;
    }

    public int size() {
        return this.mParentCache.size();
    }

    public void switchToReadOnlyMode() {
        this.mParentCache.switchToReadOnlyMode();
    }

    @Override
    public String toString() {
        this.mLRU.flush();
        return this.mParentCache.toString();
    }

    public Collection values() {
        throw new UnsupportedOperationException();
    }

    public int getCacheSize() {
        return this.mParentCache.getCacheSize();
    }

    public Class[] getKeyTypes() {
        return this.mParentCache.getKeyTypes();
    }

    public String getName() {
        return this.mParentCache.getName();
    }

    public String[] getValueFields() {
        return this.mParentCache.getValueFields();
    }

    public Class[] getValueTypes() {
        return this.mParentCache.getValueTypes();
    }

    public Class getStorageClass() {
        return this.mParentCache.getStorageClass();
    }

    public void close() {
        this.mParentCache.close();
    }

    public void closeCacheEnvironment() {
        this.mParentCache.closeCacheEnvironment();
    }

}
