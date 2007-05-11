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

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.dbutils.hsqldb.HSQLDBItemHelper;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.util.Bytes;

// TODO: Auto-generated Javadoc
/**
 * The Class HSQLDBIndexedMap.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
final public class HSQLDBIndexedMap implements PersistentMap {

    /**
     * The Class HashWrapper.
     */
    final class HashWrapper {

        /** The data. */
        Object[] data;

        /**
         * Instantiates a new hash wrapper.
         * 
         * @param data the data
         */
        public HashWrapper(Object[] data) {
            super();
            this.data = data;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            return java.util.Arrays.equals(this.data, ((HashWrapper) obj).data);
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(this.data);
        }

    }

    /** The commit count. */
    private int commitCount = 0;
    
    /** The count. */
    private int count = 0;
    
    /** The field index. */
    private HashMap fieldIndex = new HashMap();
    
    /** The data objs. */
    private String keyFlds = "", keyFldsSel = "", keyParms = "", keyObjs = "", dataFlds = "", dataParms = "",
            dataFldsSel = "", dataObjs = "";

    /** The m cache dir. */
    private String mCacheDir = null;
    
    /** The m values is array. */
    private boolean mKeyIsArray, mValuesIsArray;
    
    /** The m key types. */
    private Class[] mKeyTypes;
    
    /** The m name. */
    private String mName;
    
    /** The m persistance ID. */
    private Integer mPersistanceID;
    
    /** The m remove. */
    private PreparedStatement mSelect, mInsert, mRemove;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#get(java.lang.Object)
     */

    /** The m value fields. */
    private String[] mValueFields;

    /** The m value types. */
    private Class[] mValueTypes;

    /** The stmt. */
    private Statement stmt;

    /** The storage. */
    private Connection storage = null;
    
    /** The m size. */
    private int mSize;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getKeyTypes()
     */
    public Class[] getKeyTypes() {
        return this.mKeyTypes;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getValueFields()
     */
    public String[] getValueFields() {
        return this.mValueFields;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getValueTypes()
     */
    public Class[] getValueTypes() {
        return this.mValueTypes;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getCacheSize()
     */
    public int getCacheSize() {
        return this.mSize;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getName()
     */
    public String getName() {
        return this.mName;
    }

    /**
     * Instantiates a new HSQLDB indexed map.
     * 
     * @param pName the name
     * @param pSize the size
     * @param pPersistanceID the persistance ID
     * @param pCacheDir the cache dir
     * @param pKeyTypes the key types
     * @param pValueTypes the value types
     * @param pValueFields the value fields
     * @param pPurgeCache the purge cache
     */
    public HSQLDBIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes,
            Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
        super();

        this.mPersistanceID = pPersistanceID;
        this.mName = pName;
        this.mSize = pSize;
        this.mCacheDir = pCacheDir;
        this.mKeyTypes = pKeyTypes;
        this.mValueTypes = pValueTypes;
        this.mKeyIsArray = true;// pKeyTypes.length == 1 ? false : true;
        this.mValuesIsArray = true; // pValueTypes.length == 1 ? false : true;
        this.mValueFields = pValueFields;
        for (int i = 0; i < pValueFields.length; i++) {
            this.fieldIndex.put(pValueFields[i], i);
        }
        this.mJDBCItemHelper = new HSQLDBItemHelper();
        try {
            this.init();
        } catch (Exception e) {
            throw new KETLError(e);
        }

    }

    /** The m JDBC item helper. */
    private HSQLDBItemHelper mJDBCItemHelper;

    /* (non-Javadoc)
     * @see java.util.Map#clear()
     */
    public void clear() {

        {
            try {
                this.mSelect.close();
                this.mRemove.close();
                this.mInsert.close();
                this.stmt.executeUpdate("drop table " + this.mName);
                this.createCacheTable();
                this.prepareStatements();

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#commit(boolean)
     */
    public synchronized void commit(boolean force) {
        if (this.commitCount > 0) {
            this.commitCount = 0;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + this.mName + "' Size: "
                    + NumberFormatter.format(new File(this.getCacheFileName()).length()) + ", Approximate Count: "
                    + this.count);
            try {
                this.storage.commit();
            } catch (SQLException e) {
                throw new KETLError(e);

            }
        }
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates the cache table.
     * 
     * @throws SQLException the SQL exception
     */
    private void createCacheTable() throws SQLException {
        try {
            this.stmt.execute("drop table " + this.mName);
        } catch (Exception e) {
        }
        this.stmt.execute("create cached table " + this.mName + "(" + this.keyObjs + "," + this.dataObjs + ")");
        this.stmt.execute("create unique index idx" + this.mName + " on " + this.mName + "(" + this.keyFlds + ")");
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#delete()
     */
    public synchronized void delete() {
        this.deleteCache();
    }

    /**
     * Delete cache.
     */
    public synchronized void deleteCache() {

        if (this.storage != null) {
            try {
                try {
                    Statement stmt = this.storage.createStatement();
                    stmt.executeQuery("drop table " + this.mName);
                } catch (Exception e) {
                }

                this.storage.close();
            } catch (SQLException e) {
                throw new KETLError(e);

            }

            this.storage = null;
        }
        File fl = new File(this.getCacheFileName());
        if (fl.exists())
            fl.delete();

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
    public Object get(Object key) {
        return this.get(key, null);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#get(java.lang.Object, java.lang.String)
     */
    public Object get(Object pkey, String pField) {

        Object res = null;

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
        }

        if (this.mValuesIsArray == false)
            return res;

        Object idx = this.fieldIndex.get(pField);

        if (idx == null)
            throw new KETLError("Key " + pField + " does not exist in lookup");

        return ((Object[]) res)[(Integer) idx];

    }

    /**
     * Gets the as key.
     * 
     * @param obj the obj
     * @param cl the cl
     * 
     * @return the as key
     * 
     * @throws Error the error
     */
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

    /**
     * Gets the cache file name.
     * 
     * @return the cache file name
     */
    private String getCacheFileName() {
        if (this.mPersistanceID == null)
            return new File(this.mCacheDir + File.separator + "KETL." + this.mName + "cache").getAbsolutePath();
        else
            return new File(this.mCacheDir + File.separator + "KETL." + this.mName + "." + this.mPersistanceID + ".cache")
                    .getAbsolutePath();

    }

    /**
     * Init.
     * 
     * @throws ClassNotFoundException the class not found exception
     * @throws SQLException the SQL exception
     */
    void init() throws ClassNotFoundException, SQLException {

        this.deleteCache();

        for (int i = 0; i < this.mKeyTypes.length; i++) {
            if (i > 0) {
                this.keyFlds += ",";
                this.keyObjs += ",";
                this.keyFldsSel += " AND ";
                this.keyParms += ",";
            }
            this.keyFldsSel += " keyFld" + i + " = ?";
            this.keyFlds += "keyFld" + i;
            this.keyObjs += "keyFld" + i + " VARBINARY";
            this.keyParms += "?";
        }
        for (int i = 0; i < this.mValueTypes.length; i++) {
            if (i > 0) {
                this.dataFlds += ",";
                this.dataFldsSel += " AND ";
                this.dataParms += ",";
                this.dataObjs += ",";
            }
            this.dataFldsSel += " dataFld" + i + " = ?";
            this.dataFlds += "dataFld" + i;
            this.dataParms += "?";
            this.dataObjs += "dataFld" + i + " " + HSQLDBItemHelper.getType(this.mValueTypes[i], false);
        }

        if (this.storage == null) {
            Class.forName("org.hsqldb.jdbcDriver");
            this.storage = DriverManager.getConnection("jdbc:hsqldb:file:" + this.getCacheFileName(), "sa", "");
        }

        this.stmt = this.storage.createStatement();
        boolean exists = false;
        try {
            ResultSet rs = this.stmt.executeQuery("select " + this.keyFlds + "," + this.dataFlds + " from " + this.mName);
            while (rs.next() && exists == false) {

                int pos = 1;
                for (Class element : this.mKeyTypes) {
                    this.mJDBCItemHelper.getObjectFromResultSet(rs, pos++, element, -1);
                }
                for (Class element : this.mValueTypes) {
                    this.mJDBCItemHelper.getObjectFromResultSet(rs, pos++, element, -1);
                }

                exists = true;

            }

            rs.close();
        } catch (Exception e) {

        }

        if (exists == false) {
            this.createCacheTable();
        }
        this.stmt.execute("SET WRITE_DELAY TRUE");

        this.prepareStatements();

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
                + this.mName + ", Key Type(s):" + java.util.Arrays.toString(this.mKeyTypes) + ", Key Field(s):"
                + java.util.Arrays.toString(this.mValueFields) + ", Result Type(s):"
                + java.util.Arrays.toString(this.mValueTypes));
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

    /**
     * Prepare statements.
     * 
     * @throws SQLException the SQL exception
     */
    private void prepareStatements() throws SQLException {

        this.mSelect = this.storage.prepareStatement("select " + this.dataFlds + " from " + this.mName + " where " + this.keyFldsSel);
        this.mRemove = this.storage.prepareStatement("delete from " + this.mName + " where " + this.keyFldsSel);
        this.mInsert = this.storage.prepareStatement("insert into " + this.mName + "(" + this.keyFlds + "," + this.dataFlds + ") values("
                + this.keyParms + "," + this.dataParms + ")");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object pkey, Object pValue) {

        // local hashed cached

        // HSQLDB based cache
        try {
            int pos = 1;
            if (this.mKeyIsArray) {
                Object[] tmp = (Object[]) pkey;

                for (int i = 0; i < tmp.length; i++) {
                    this.mInsert.setBytes(pos++, this.getAsKey(tmp[i], this.mKeyTypes[i]));
                }
            }
            else {
                this.mInsert.setBytes(pos++, this.getAsKey(pkey, pkey == null ? null : pkey.getClass()));
            }

            if (this.mValuesIsArray) {
                Object[] tmp = (Object[]) pValue;

                for (int i = 0; i < tmp.length; i++) {
                    this.mJDBCItemHelper.setParameterFromClass(this.mInsert, pos++, this.mValueTypes[i], tmp[i], -1,
                            null);
                }
            }
            else {
                this.mJDBCItemHelper.setParameterFromClass(this.mInsert, pos++, pValue.getClass(), pValue, -1, null);
            }

            this.mInsert.executeUpdate();
            this.count++;
            if (this.commitCount++ > 50000) {
                this.commit(false);
            }

        } catch (SQLException e) {
            int code = e.getErrorCode();
            if (!(code == -9 && e.getSQLState().equals("23000"))) {
                throw new KETLError(e);
            }
        }

        return null;
    }

    /* (non-Javadoc)
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map t) {

    }

    /* (non-Javadoc)
     * @see java.util.Map#remove(java.lang.Object)
     */
    public Object remove(Object pkey) {
        // local hashed cached

        try {
            int pos = 1;
            if (this.mKeyIsArray) {
                Object[] tmp = (Object[]) pkey;

                for (int i = 0; i < tmp.length; i++) {
                    this.mRemove.setBytes(pos++, this.getAsKey(tmp[i], this.mKeyTypes[i]));
                }
            }
            else {
                this.mRemove.setBytes(pos++, this.getAsKey(pkey, pkey == null ? null : pkey.getClass()));
            }

            this.mRemove.executeUpdate();
        } catch (SQLException e) {
            throw new KETLError(e);

        }
        return null;
    }

    /* (non-Javadoc)
     * @see java.util.Map#size()
     */
    public int size() {
        try {
            ResultSet rs = this.stmt.executeQuery("select count(*) from " + this.mName);

            int size = -1;
            while (rs.next()) {
                size = rs.getInt(1);
            }
            return size;
        } catch (SQLException e) {
            throw new KETLError(e);

        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#switchToReadOnlyMode()
     */
    public void switchToReadOnlyMode() {

    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String exampleValue = "N/A", exampleKey = "N/A";
        if (this.storage != null && this.count > 0) {
        }
        // TODO Auto-generated method stub
        return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
                + "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
                + java.util.Arrays.toString(this.mValueTypes) + "\n\tExample: Key->" + exampleKey + " Value->"
                + exampleValue + "\n\tCount: " + this.count + "\n\tConsolidated Cache Size: "
                + NumberFormatter.format(new File(this.getCacheFileName()).length());

    }

    /* (non-Javadoc)
     * @see java.util.Map#values()
     */
    public Collection values() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getItem(java.lang.Object)
     */
    public Object getItem(Object pkey) throws Exception {
        int pos = 1;
        if (this.mKeyIsArray) {
            Object[] tmp = (Object[]) pkey;

            for (int i = 0; i < tmp.length; i++) {
                this.mSelect.setBytes(pos++, this.getAsKey(tmp[i], this.mKeyTypes[i]));
            }
        }
        else {
            this.mSelect.setBytes(pos++, this.getAsKey(pkey, pkey == null ? null : pkey.getClass()));
        }

        Object res = null;
        ResultSet rs = this.mSelect.executeQuery();
        while (res == null && rs.next()) {
            int i = 0;

            if (this.mValuesIsArray) {
                Object[] ar = new Object[this.mValueTypes.length];
                for (i = 0; i < this.mValueTypes.length; i++) {
                    ar[i] = this.mJDBCItemHelper.getObjectFromResultSet(rs, i + 1, this.mValueTypes[i], -1);
                }
                res = ar;
            }
            else
                res = rs.getObject(1);
        }
        rs.close();

        return res;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#getStorageClass()
     */
    public Class getStorageClass() {
        return this.getClass();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#close()
     */
    public void close() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.PersistentMap#closeCacheEnvironment()
     */
    public void closeCacheEnvironment() {
        // TODO Auto-generated method stub
        
    }

}
