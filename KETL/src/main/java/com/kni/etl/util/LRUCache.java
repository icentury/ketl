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
package com.kni.etl.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

// TODO: Auto-generated Javadoc
/**
 * An LRU cache, based on <code>LinkedHashMap</code>.<br>
 * This cache has a fixed maximum number of elements (<code>cacheSize</code>). If the cache is full and another
 * entry is added, the LRU (least recently used) entry is dropped.
 * <p>
 */
public class LRUCache<K, V> {

    /** The hash table load factor. */
    private final float hashTableLoadFactor = 0.75f;

    /** The map. */
    private LinkedHashMap<K, V> map;
    
    /** The cache size. */
    private int cacheSize;

    /**
     * Creates a new LRU cache.
     * 
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        int hashTableCapacity = (int) Math.ceil(cacheSize / this.hashTableLoadFactor) + 1;
        this.map = new LinkedHashMap<K, V>(hashTableCapacity, this.hashTableLoadFactor, true) {

            // (an anonymous inner class)
            private static final long serialVersionUID = 1;

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if (this.size() > LRUCache.this.cacheSize) {
                    LRUCache.this.removingEntry(eldest);
                    return true;
                }

                return false;
            }
        };
    }

    /**
     * Removing entry.
     * 
     * @param eldest the eldest
     * @throws IOException 
     */
    protected void removingEntry(Map.Entry<K, V> eldest)  {

    }

    /**
     * Gets the backing map.
     * 
     * @return the backing map
     */
    protected Map getBackingMap() {
        return this.map;
    }

    /**
     * Retrieves an entry from the cache.<br>
     * The retrieved entry becomes the MRU (most recently used) entry.
     * 
     * @param key the key whose associated value is to be returned.
     * 
     * @return the value associated to this key, or null if no value with this key exists in the cache.
     */
    public synchronized V get(K key) {
        return this.map.get(key);
    }

    /**
     * Adds an entry to this cache. If the cache is full, the LRU (least recently used) entry is dropped.
     * 
     * @param key the key with which the specified value is to be associated.
     * @param value a value to be associated with the specified key.
     */
    public synchronized void put(K key, V value) {
        this.map.put(key, value);
    }

    /**
     * Clears the cache.
     */
    public synchronized void clear() {
        this.map.clear();
    }

    /**
     * Returns the number of used entries in the cache.
     * 
     * @return the number of entries currently in the cache.
     */
    public synchronized int usedEntries() {
        return this.map.size();
    }

    /**
     * Returns a <code>Collection</code> that contains a copy of all cache entries.
     * 
     * @return a <code>Collection</code> with a copy of the cache content.
     */
    public synchronized Collection<Map.Entry<K, V>> getAll() {
        return new ArrayList<Map.Entry<K, V>>(this.map.entrySet());
    }

    /**
     * Remove.
     * 
     * @param key the key
     */
    public synchronized void remove(K key) {
        this.map.remove(key);
    }

} // end class LRUCache

