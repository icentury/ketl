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

import java.util.Map;

// TODO: Auto-generated Javadoc
/**
 * The Interface PersistentMap.
 */
public interface PersistentMap extends Map {

	/** The value fields. */
	String[] mValueFields = null;

	/**
	 * Get.
	 * 
	 * @param key
	 *            the key
	 * @param pField
	 *            the field
	 * 
	 * @return the object
	 */
	public abstract Object get(Object key, String pField);

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	public abstract Object put(Object key, Object value);

	/**
	 * Gets the storage class.
	 * 
	 * @return the storage class
	 */
	public abstract Class getStorageClass();

	/**
	 * Delete.
	 */
	public abstract void delete();

	/**
	 * Switch to read only mode.
	 */
	public abstract void switchToReadOnlyMode();

	/**
	 * Gets the item.
	 * 
	 * @param pkey
	 *            the pkey
	 * 
	 * @return the item
	 * 
	 * @throws Exception
	 *             the exception
	 */
	public abstract Object getItem(Object pkey) throws Exception;

	/**
	 * Commit.
	 * 
	 * @param force
	 *            the force
	 */
	public abstract void commit(boolean force);

	/**
	 * Gets the value fields.
	 * 
	 * @return the value fields
	 */
	public abstract String[] getValueFields();

	/**
	 * Gets the value types.
	 * 
	 * @return the value types
	 */
	public abstract Class[] getValueTypes();

	/**
	 * Gets the key types.
	 * 
	 * @return the key types
	 */
	public abstract Class[] getKeyTypes();

	/**
	 * Gets the cache size.
	 * 
	 * @return the cache size
	 */
	public abstract int getCacheSize();

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 */
	public abstract String getName();

	/**
	 * Close.
	 */
	public abstract void close();

	/**
	 * Close cache environment.
	 */
	public abstract void closeCacheEnvironment();

}