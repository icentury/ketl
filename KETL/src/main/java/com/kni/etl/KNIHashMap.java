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
package com.kni.etl;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (4/19/2002 5:47:07 PM)
 * 
 * @author: Administrator
 */
public class KNIHashMap extends AbstractMap implements Map, Cloneable, java.io.Serializable {

    /** The hash table data. */
    private transient Entry[] table;

    /** The total number of mappings in the hash table. */
    private transient int count;

    /** The table is rehashed when its size exceeds this threshold. (The value of this field is (int)(capacity * loadFactor).) */
    private int threshold;

    /** The load factor for the hashtable. */
    private float loadFactor;

    /** The number of times this HashMap has been structurally modified Structural modifications are those that change the number of mappings in the HashMap or otherwise modify its internal structure (e.g., rehash). This field is used to make iterators on Collection-views of the HashMap fail-fast. (See ConcurrentModificationException). */
    private transient int modCount = 0;

    // Views
    /** The key set. */
    private transient Set keySet = null;
    
    /** The entry set. */
    private transient Set entrySet = null;
    
    /** The values. */
    private transient Collection values = null;

    /**
     * HashMap collision list entry.
     */
    private static class Entry implements Map.Entry {

        /** The hash. */
        int hash;
        
        /** The key. */
        Object key;
        
        /** The value. */
        Object value;
        
        /** The next. */
        Entry next;

        /**
         * Instantiates a new entry.
         * 
         * @param hash the hash
         * @param key the key
         * @param value the value
         * @param next the next
         */
        Entry(int hash, Object key, Object value, Entry next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#clone()
         */
        @Override
        protected Object clone() {
            return new Entry(this.hash, this.key, this.value, ((this.next == null) ? null : (Entry) this.next.clone()));
        }

        // Map.Entry Ops
        /* (non-Javadoc)
         * @see java.util.Map$Entry#getKey()
         */
        public Object getKey() {
            return this.key;
        }

        /* (non-Javadoc)
         * @see java.util.Map$Entry#getValue()
         */
        public Object getValue() {
            return this.value;
        }

        /* (non-Javadoc)
         * @see java.util.Map$Entry#setValue(java.lang.Object)
         */
        public Object setValue(Object value) {
            Object oldValue = this.value;
            this.value = value;

            return oldValue;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            Map.Entry e = (Map.Entry) o;

            return ((this.key == null) ? (e.getKey() == null) : this.key.equals(e.getKey()))
                    && ((this.value == null) ? (e.getValue() == null) : this.value.equals(e.getValue()));
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return this.hash ^ ((this.value == null) ? 0 : this.value.hashCode());
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return this.key + "=" + this.value;
        }
    }

    // Types of Iterators
    /** The Constant KEYS. */
    private static final int KEYS = 0;
    
    /** The Constant VALUES. */
    private static final int VALUES = 1;
    
    /** The Constant ENTRIES. */
    private static final int ENTRIES = 2;

    /**
     * The Class HashIterator.
     */
    private class HashIterator implements Iterator {

        /** The table. */
        Entry[] table = KNIHashMap.this.table;
        
        /** The index. */
        int index = this.table.length;
        
        /** The entry. */
        Entry entry = null;
        
        /** The last returned. */
        Entry lastReturned = null;
        
        /** The type. */
        int type;

        /** The modCount value that the iterator believes that the backing List should have. If this expectation is violated, the iterator has detected concurrent modification. */
        private int expectedModCount = KNIHashMap.this.modCount;

        /**
         * Instantiates a new hash iterator.
         * 
         * @param type the type
         */
        HashIterator(int type) {
            this.type = type;
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#hasNext()
         */
        public boolean hasNext() {
            while ((this.entry == null) && (this.index > 0))
                this.entry = this.table[--this.index];

            return this.entry != null;
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#next()
         */
        public Object next() {
            if (KNIHashMap.this.modCount != this.expectedModCount) {
                throw new ConcurrentModificationException();
            }

            while ((this.entry == null) && (this.index > 0))
                this.entry = this.table[--this.index];

            if (this.entry != null) {
                Entry e = this.lastReturned = this.entry;
                this.entry = e.next;

                return (this.type == KNIHashMap.KEYS) ? e.key : ((this.type == KNIHashMap.VALUES) ? e.value : e);
            }

            throw new NoSuchElementException();
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#remove()
         */
        public void remove() {
            if (this.lastReturned == null) {
                throw new IllegalStateException();
            }

            if (KNIHashMap.this.modCount != this.expectedModCount) {
                throw new ConcurrentModificationException();
            }

            Entry[] tab = KNIHashMap.this.table;
            int index = (this.lastReturned.hash & 0x7FFFFFFF) % tab.length;

            for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
                if (e == this.lastReturned) {
                    KNIHashMap.this.modCount++;
                    this.expectedModCount++;

                    if (prev == null) {
                        tab[index] = e.next;
                    }
                    else {
                        prev.next = e.next;
                    }

                    KNIHashMap.this.count--;
                    this.lastReturned = null;

                    return;
                }
            }

            throw new ConcurrentModificationException();
        }
    }

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 362498820763181265L;

    /**
     * Constructs a new, empty map with a default capacity and load factor, which is <tt>0.75</tt>.
     */
    public KNIHashMap() {
        this(101, 0.75f);
    }

    /**
     * Constructs a new, empty map with the specified initial capacity and default load factor, which is <tt>0.75</tt>.
     * 
     * @param initialCapacity the initial capacity of the KNIHashMap.
     * 
     * @throws IllegalArgumentException if the initial capacity is less than zero.
     */
    public KNIHashMap(int initialCapacity) {
        this(initialCapacity, 0.75f);
    }

    /**
     * Constructs a new, empty map with the specified initial capacity and the specified load factor.
     * 
     * @param initialCapacity the initial capacity of the KNIHashMap.
     * @param loadFactor the load factor of the KNIHashMap
     * 
     * @throws IllegalArgumentException if the initial capacity is less than zero, or if the load factor is nonpositive.
     */
    public KNIHashMap(int initialCapacity, float loadFactor) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Initial Capacity: " + initialCapacity);
        }

        if (loadFactor <= 0) {
            throw new IllegalArgumentException("Illegal Load factor: " + loadFactor);
        }

        if (initialCapacity == 0) {
            initialCapacity = 1;
        }

        this.loadFactor = loadFactor;
        this.table = new Entry[initialCapacity];
        this.threshold = (int) (initialCapacity * loadFactor);
    }

    /**
     * Constructs a new map with the same mappings as the given map. The map is created with a capacity of twice the
     * number of mappings in the given map or 11 (whichever is greater), and a default load factor, which is
     * <tt>0.75</tt>.
     * 
     * @param t the t
     */
    public KNIHashMap(Map t) {
        this(Math.max(2 * t.size(), 11), 0.75f);
        this.putAll(t);
    }

    /**
     * Capacity.
     * 
     * @return the int
     */
    int capacity() {
        return this.table.length;
    }

    /**
     * Removes all mappings from this map.
     */
    @Override
    public void clear() {
        Entry[] tab = this.table;
        this.modCount++;

        for (int index = tab.length; --index >= 0;)
            tab[index] = null;

        this.count = 0;
    }

    /**
     * Returns a shallow copy of this <tt>KNIHashMap</tt> instance: the keys and values themselves are not cloned.
     * 
     * @return a shallow copy of this map.
     */
    @Override
    public Object clone() {
        try {
            KNIHashMap t = (KNIHashMap) super.clone();
            t.table = new Entry[this.table.length];

            for (int i = this.table.length; i-- > 0;) {
                t.table[i] = (this.table[i] != null) ? (Entry) this.table[i].clone() : null;
            }

            t.keySet = null;
            t.entrySet = null;
            t.values = null;
            t.modCount = 0;

            return t;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified key.
     * 
     * @param key key whose presence in this Map is to be tested.
     * 
     * @return <tt>true</tt> if this map contains a mapping for the specified key.
     */
    @Override
    public boolean containsKey(Object key) {
        Entry[] tab = this.table;

        if (key != null) {
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % tab.length;

            for (Entry e = tab[index]; e != null; e = e.next)
                if ((e.hash == hash) && key.equals(e.key)) {
                    return true;
                }
        }
        else {
            for (Entry e = tab[0]; e != null; e = e.next)
                if (e.key == null) {
                    return true;
                }
        }

        return false;
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the specified value.
     * 
     * @param value value whose presence in this map is to be tested.
     * 
     * @return <tt>true</tt> if this map maps one or more keys to the specified value.
     */
    @Override
    public boolean containsValue(Object value) {
        Entry[] tab = this.table;

        if (value == null) {
            for (int i = tab.length; i-- > 0;)
                for (Entry e = tab[i]; e != null; e = e.next)
                    if (e.value == null) {
                        return true;
                    }
        }
        else {
            for (int i = tab.length; i-- > 0;)
                for (Entry e = tab[i]; e != null; e = e.next)
                    if (value.equals(e.value)) {
                        return true;
                    }
        }

        return false;
    }

    /**
     * Returns a collection view of the mappings contained in this map. Each element in the returned collection is a
     * <tt>Map.Entry</tt>. The collection is backed by the map, so changes to the map are reflected in the
     * collection, and vice-versa. The collection supports element removal, which removes the corresponding mapping from
     * the map, via the <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt>, and <tt>clear</tt> operations. It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     * 
     * @return a collection view of the mappings contained in this map.
     * 
     * @see Map.Entry
     */
    @Override
    public Set entrySet() {
        if (this.entrySet == null) {
            this.entrySet = new AbstractSet() {

                @Override
                public Iterator iterator() {
                    return new HashIterator(KNIHashMap.ENTRIES);
                }

                @Override
                public boolean contains(Object o) {
                    if (!(o instanceof Map.Entry)) {
                        return false;
                    }

                    Map.Entry entry = (Map.Entry) o;
                    Object key = entry.getKey();
                    Entry[] tab = KNIHashMap.this.table;
                    int hash = ((key == null) ? 0 : key.hashCode());
                    int index = (hash & 0x7FFFFFFF) % tab.length;

                    for (Entry e = tab[index]; e != null; e = e.next)
                        if ((e.hash == hash) && e.equals(entry)) {
                            return true;
                        }

                    return false;
                }

                @Override
                public boolean remove(Object o) {
                    if (!(o instanceof Map.Entry)) {
                        return false;
                    }

                    Map.Entry entry = (Map.Entry) o;
                    Object key = entry.getKey();
                    Entry[] tab = KNIHashMap.this.table;
                    int hash = ((key == null) ? 0 : key.hashCode());
                    int index = (hash & 0x7FFFFFFF) % tab.length;

                    for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
                        if ((e.hash == hash) && e.equals(entry)) {
                            KNIHashMap.this.modCount++;

                            if (prev != null) {
                                prev.next = e.next;
                            }
                            else {
                                tab[index] = e.next;
                            }

                            KNIHashMap.this.count--;
                            e.value = null;

                            return true;
                        }
                    }

                    return false;
                }

                @Override
                public int size() {
                    return KNIHashMap.this.count;
                }

                @Override
                public void clear() {
                    KNIHashMap.this.clear();
                }
            };
        }

        return this.entrySet;
    }

    /**
     * Returns the value to which this map maps the specified key. Returns <tt>null</tt> if the map contains no
     * mapping for this key. A return value of <tt>null</tt> does not <i>necessarily</i> indicate that the map
     * contains no mapping for the key; it's also possible that the map explicitly maps the key to <tt>null</tt>. The
     * <tt>containsKey</tt> operation may be used to distinguish these two cases.
     * 
     * @param key key whose associated value is to be returned.
     * 
     * @return the value to which this map maps the specified key.
     */
    @Override
    public Object get(Object key) {
        Entry[] tab = this.table;

        if (key != null) {
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % tab.length;

            for (Entry e = tab[index]; e != null; e = e.next)
                if ((e.hash == hash) && key.equals(e.key)) {
                    return e.value;
                }
        }
        else {
            for (Entry e = tab[0]; e != null; e = e.next)
                if (e.key == null) {
                    return e.value;
                }
        }

        return null;
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     * 
     * @return <tt>true</tt> if this map contains no key-value mappings.
     */
    @Override
    public boolean isEmpty() {
        return this.count == 0;
    }

    /**
     * Returns a set view of the keys contained in this map. The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa. The set supports element removal, which removes the corresponding mapping
     * from this map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt>, and <tt>clear</tt> operations. It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     * 
     * @return a set view of the keys contained in this map.
     */
    @Override
    public Set keySet() {
        if (this.keySet == null) {
            this.keySet = new AbstractSet() {

                @Override
                public Iterator iterator() {
                    return new HashIterator(KNIHashMap.KEYS);
                }

                @Override
                public int size() {
                    return KNIHashMap.this.count;
                }

                @Override
                public boolean contains(Object o) {
                    return KNIHashMap.this.containsKey(o);
                }

                @Override
                public boolean remove(Object o) {
                    return KNIHashMap.this.remove(o) != null;
                }

                @Override
                public void clear() {
                    KNIHashMap.this.clear();
                }
            };
        }

        return this.keySet;
    }

    /**
     * Load factor.
     * 
     * @return the float
     */
    float loadFactor() {
        return this.loadFactor;
    }

    /**
     * Associates the specified value with the specified key in this map. If the map previously contained a mapping for
     * this key, the old value is replaced.
     * 
     * @param key key with which the specified value is to be associated.
     * @param value value to be associated with the specified key.
     * 
     * @return previous value associated with specified key, or <tt>null</tt> if there was no mapping for key. A
     * <tt>null</tt> return can also indicate that the KNIHashMap previously associated <tt>null</tt> with
     * the specified key.
     */
    @Override
    public Object put(Object key, Object value) {
        // Makes sure the key is not already in the KNIHashMap.
        Entry[] tab = this.table;
        int hash = 0;
        int index = 0;

        if (key != null) {
            hash = key.hashCode();
            index = (hash & 0x7FFFFFFF) % tab.length;

            for (Entry e = tab[index]; e != null; e = e.next) {
                if ((e.hash == hash) && key.equals(e.key)) {
                    Object old = e.value;
                    e.value = value;

                    return old;
                }
            }
        }
        else {
            for (Entry e = tab[0]; e != null; e = e.next) {
                if (e.key == null) {
                    Object old = e.value;
                    e.value = value;

                    return old;
                }
            }
        }

        this.modCount++;

        if (this.count >= this.threshold) {
            // Rehash the table if the threshold is exceeded
            this.rehash();

            tab = this.table;
            index = (hash & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        Entry e = new Entry(hash, key, value, tab[index]);
        tab[index] = e;
        this.count++;

        return null;
    }

    /**
     * Copies all of the mappings from the specified map to this one. These mappings replace any mappings that this map
     * had for any of the keys currently in the specified Map.
     * 
     * @param t Mappings to be stored in this map.
     */
    @Override
    public void putAll(Map t) {
        Iterator i = t.entrySet().iterator();

        while (i.hasNext()) {
            Map.Entry e = (Map.Entry) i.next();
            this.put(e.getKey(), e.getValue());
        }
    }

    /**
     * Reconstitute the <tt>KNIHashMap</tt> instance from a stream (i.e., deserialize it).
     * 
     * @param s the s
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        // Read in the threshold, loadfactor, and any hidden stuff
        s.defaultReadObject();

        // Read in number of buckets and allocate the bucket array;
        int numBuckets = s.readInt();
        this.table = new Entry[numBuckets];

        // Read in size (number of Mappings)
        int size = s.readInt();

        // Read the keys and values, and put the mappings in the KNIHashMap
        for (int i = 0; i < size; i++) {
            Object key = s.readObject();
            Object value = s.readObject();
            this.put(key, value);
        }
    }

    /**
     * Rehashes the contents of this map into a new <tt>KNIHashMap</tt> instance with a larger capacity. This method
     * is called automatically when the number of keys in this map exceeds its capacity and load factor.
     */
    private void rehash() {
        int oldCapacity = this.table.length;
        Entry[] oldMap = this.table;

        int newCapacity = (oldCapacity * 2) + 1;
        Entry[] newMap = new Entry[newCapacity];

        this.modCount++;
        this.threshold = (int) (newCapacity * this.loadFactor);
        this.table = newMap;

        for (int i = oldCapacity; i-- > 0;) {
            for (Entry old = oldMap[i]; old != null;) {
                Entry e = old;
                old = old.next;

                int index = (e.hash & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    /**
     * Removes the mapping for this key from this map if present.
     * 
     * @param key key whose mapping is to be removed from the map.
     * 
     * @return previous value associated with specified key, or <tt>null</tt> if there was no mapping for key. A
     * <tt>null</tt> return can also indicate that the map previously associated <tt>null</tt> with the
     * specified key.
     */
    @Override
    public Object remove(Object key) {
        Entry[] tab = this.table;

        if (key != null) {
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % tab.length;

            for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
                if ((e.hash == hash) && key.equals(e.key)) {
                    this.modCount++;

                    if (prev != null) {
                        prev.next = e.next;
                    }
                    else {
                        tab[index] = e.next;
                    }

                    this.count--;

                    Object oldValue = e.value;
                    e.value = null;

                    return oldValue;
                }
            }
        }
        else {
            for (Entry e = tab[0], prev = null; e != null; prev = e, e = e.next) {
                if (e.key == null) {
                    this.modCount++;

                    if (prev != null) {
                        prev.next = e.next;
                    }
                    else {
                        tab[0] = e.next;
                    }

                    this.count--;

                    Object oldValue = e.value;
                    e.value = null;

                    return oldValue;
                }
            }
        }

        return null;
    }

    /**
     * Returns the number of key-value mappings in this map.
     * 
     * @return the number of key-value mappings in this map.
     */
    @Override
    public int size() {
        return this.count;
    }

    /**
     * Returns a collection view of the values contained in this map. The collection is backed by the map, so changes to
     * the map are reflected in the collection, and vice-versa. The collection supports element removal, which removes
     * the corresponding mapping from this map, via the <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt> operations. It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     * 
     * @return a collection view of the values contained in this map.
     */
    @Override
    public Collection values() {
        if (this.values == null) {
            this.values = new AbstractCollection() {

                @Override
                public Iterator iterator() {
                    return new HashIterator(KNIHashMap.VALUES);
                }

                @Override
                public int size() {
                    return KNIHashMap.this.count;
                }

                @Override
                public boolean contains(Object o) {
                    return KNIHashMap.this.containsValue(o);
                }

                @Override
                public void clear() {
                    KNIHashMap.this.clear();
                }
            };
        }

        return this.values;
    }

    /**
     * Save the state of the <tt>KNIHashMap</tt> instance to a stream (i.e., serialize it).
     * 
     * @param s the s
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * 
     * @serialData The <i>capacity</i> of the KNIHashMap (the length of the bucket array) is emitted (int), followed by
     * the <i>size</i> of the KNIHashMap (the number of key-value mappings), followed by the key (Object)
     * and value (Object) for each key-value mapping represented by the KNIHashMap The key-value mappings
     * are emitted in no particular order.
     */
    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        // Write out the threshold, loadfactor, and any hidden stuff
        s.defaultWriteObject();

        // Write out number of buckets
        s.writeInt(this.table.length);

        // Write out size (number of Mappings)
        s.writeInt(this.count);

        // Write out keys and values (alternating)
        for (int index = this.table.length - 1; index >= 0; index--) {
            Entry entry = this.table[index];

            while (entry != null) {
                s.writeObject(entry.key);
                s.writeObject(entry.value);
                entry = entry.next;
            }
        }
    }
}
