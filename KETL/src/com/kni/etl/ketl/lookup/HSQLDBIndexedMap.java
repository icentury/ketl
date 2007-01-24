/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Sep 25, 2006
 * 
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

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
final public class HSQLDBIndexedMap implements PersistentMap {

    final class HashWrapper {

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

    }

    private int commitCount = 0;
    private int count = 0;
    private HashMap fieldIndex = new HashMap();
    private String keyFlds = "", keyFldsSel = "", keyParms = "", keyObjs = "", dataFlds = "", dataParms = "",
            dataFldsSel = "", dataObjs = "";

    private String mCacheDir = null;
    private boolean mKeyIsArray, mValuesIsArray;
    private Class[] mKeyTypes;
    private String mName;
    private Integer mPersistanceID;
    private PreparedStatement mSelect, mInsert, mRemove;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#get(java.lang.Object)
     */

    private String[] mValueFields;

    private Class[] mValueTypes;

    private Statement stmt;

    private Connection storage = null;
    private int mSize;

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

    public HSQLDBIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes,
            Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
        super();

        mPersistanceID = pPersistanceID;
        mName = pName;
        mSize = pSize;
        this.mCacheDir = pCacheDir;
        this.mKeyTypes = pKeyTypes;
        this.mValueTypes = pValueTypes;
        this.mKeyIsArray = true;// pKeyTypes.length == 1 ? false : true;
        this.mValuesIsArray = true; // pValueTypes.length == 1 ? false : true;
        this.mValueFields = pValueFields;
        for (int i = 0; i < pValueFields.length; i++) {
            fieldIndex.put(pValueFields[i], i);
        }
        this.mJDBCItemHelper = new HSQLDBItemHelper();
        try {
            init();
        } catch (Exception e) {
            throw new KETLError(e);
        }

    }

    private HSQLDBItemHelper mJDBCItemHelper;

    public void clear() {

        {
            try {
                mSelect.close();
                mRemove.close();
                mInsert.close();
                stmt.executeUpdate("drop table " + mName);
                createCacheTable();
                prepareStatements();

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public synchronized void commit(boolean force) {
        if (commitCount > 0) {
            commitCount = 0;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + mName + "' Size: "
                    + NumberFormatter.format(new File(this.getCacheFileName()).length()) + ", Approximate Count: "
                    + this.count);
            try {
                storage.commit();
            } catch (SQLException e) {
                throw new KETLError(e);

            }
        }
    }

    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    private void createCacheTable() throws SQLException {
        try {
            stmt.execute("drop table " + mName);
        } catch (Exception e) {
        }
        stmt.execute("create cached table " + mName + "(" + keyObjs + "," + dataObjs + ")");
        stmt.execute("create unique index idx" + mName + " on " + mName + "(" + keyFlds + ")");
    }

    public synchronized void delete() {
        this.deleteCache();
    }

    public synchronized void deleteCache() {

        if (storage != null) {
            try {
                try {
                    Statement stmt = storage.createStatement();
                    stmt.executeQuery("drop table " + mName);
                } catch (Exception e) {
                }

                storage.close();
            } catch (SQLException e) {
                throw new KETLError(e);

            }

            storage = null;
        }
        File fl = new File(this.getCacheFileName());
        if (fl.exists())
            fl.delete();

    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    public Object get(Object key) {
        return this.get(key, null);
    }

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

    private String getCacheFileName() {
        if (this.mPersistanceID == null)
            return new File(mCacheDir + File.separator + "KETL." + mName + "cache").getAbsolutePath();
        else
            return new File(mCacheDir + File.separator + "KETL." + mName + "." + this.mPersistanceID + ".cache")
                    .getAbsolutePath();

    }

    void init() throws ClassNotFoundException, SQLException {

        this.deleteCache();

        for (int i = 0; i < this.mKeyTypes.length; i++) {
            if (i > 0) {
                keyFlds += ",";
                keyObjs += ",";
                keyFldsSel += " AND ";
                keyParms += ",";
            }
            keyFldsSel += " keyFld" + i + " = ?";
            keyFlds += "keyFld" + i;
            keyObjs += "keyFld" + i + " VARBINARY";
            keyParms += "?";
        }
        for (int i = 0; i < this.mValueTypes.length; i++) {
            if (i > 0) {
                dataFlds += ",";
                dataFldsSel += " AND ";
                dataParms += ",";
                dataObjs += ",";
            }
            dataFldsSel += " dataFld" + i + " = ?";
            dataFlds += "dataFld" + i;
            dataParms += "?";
            dataObjs += "dataFld" + i + " " + HSQLDBItemHelper.getType(this.mValueTypes[i], false);
        }

        if (storage == null) {
            Class.forName("org.hsqldb.jdbcDriver");
            storage = DriverManager.getConnection("jdbc:hsqldb:file:" + this.getCacheFileName(), "sa", "");
        }

        stmt = storage.createStatement();
        boolean exists = false;
        try {
            ResultSet rs = stmt.executeQuery("select " + keyFlds + "," + dataFlds + " from " + mName);
            while (rs.next() && exists == false) {

                int pos = 1;
                for (int i = 0; i <= this.mKeyTypes.length; i++) {
                    this.mJDBCItemHelper.getObjectFromResultSet(rs, pos++, this.mKeyTypes[i], -1);
                }
                for (int i = 0; i <= this.mValueTypes.length; i++) {
                    this.mJDBCItemHelper.getObjectFromResultSet(rs, pos++, this.mValueTypes[i], -1);
                }

                exists = true;

            }

            rs.close();
        } catch (Exception e) {

        }

        if (exists == false) {
            createCacheTable();
        }
        stmt.execute("SET WRITE_DELAY TRUE");

        prepareStatements();

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
                + this.mName + ", Key Type(s):" + java.util.Arrays.toString(this.mKeyTypes) + ", Key Field(s):"
                + java.util.Arrays.toString(this.mValueFields) + ", Result Type(s):"
                + java.util.Arrays.toString(this.mValueTypes));
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    private void prepareStatements() throws SQLException {

        mSelect = storage.prepareStatement("select " + dataFlds + " from " + mName + " where " + keyFldsSel);
        mRemove = storage.prepareStatement("delete from " + mName + " where " + keyFldsSel);
        mInsert = storage.prepareStatement("insert into " + mName + "(" + keyFlds + "," + dataFlds + ") values("
                + keyParms + "," + dataParms + ")");
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
                    this.mInsert.setBytes(pos++, getAsKey(tmp[i], this.mKeyTypes[i]));
                }
            }
            else {
                this.mInsert.setBytes(pos++, getAsKey(pkey, pkey == null ? null : pkey.getClass()));
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

            mInsert.executeUpdate();
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

    public void putAll(Map t) {

    }

    public Object remove(Object pkey) {
        // local hashed cached

        try {
            int pos = 1;
            if (this.mKeyIsArray) {
                Object[] tmp = (Object[]) pkey;

                for (int i = 0; i < tmp.length; i++) {
                    this.mRemove.setBytes(pos++, getAsKey(tmp[i], this.mKeyTypes[i]));
                }
            }
            else {
                this.mRemove.setBytes(pos++, getAsKey(pkey, pkey == null ? null : pkey.getClass()));
            }

            mRemove.executeUpdate();
        } catch (SQLException e) {
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
        if (this.storage != null && this.count > 0) {
        }
        // TODO Auto-generated method stub
        return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
                + "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
                + java.util.Arrays.toString(this.mValueTypes) + "\n\tExample: Key->" + exampleKey + " Value->"
                + exampleValue + "\n\tCount: " + this.count + "\n\tConsolidated Cache Size: "
                + NumberFormatter.format(new File(this.getCacheFileName()).length());

    }

    public Collection values() {
        return null;
    }

    public Object getItem(Object pkey) throws Exception {
        int pos = 1;
        if (this.mKeyIsArray) {
            Object[] tmp = (Object[]) pkey;

            for (int i = 0; i < tmp.length; i++) {
                this.mSelect.setBytes(pos++, getAsKey(tmp[i], this.mKeyTypes[i]));
            }
        }
        else {
            this.mSelect.setBytes(pos++, getAsKey(pkey, pkey == null ? null : pkey.getClass()));
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

    public Class getStorageClass() {
        return this.getClass();
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public void closeCacheEnvironment() {
        // TODO Auto-generated method stub
        
    }

}
