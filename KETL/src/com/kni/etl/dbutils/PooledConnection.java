/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on May 5, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.dbutils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PooledConnection {

    private static long TIMEOUT = 60 * 1000; // 60 seconds
    Connection mConnection;
    private boolean mInUse = false;
    boolean mAllowReuse = true;
    String mDriverClass;
    String mURL;
    String mUserName;
    String mPassword;
    String mPreSQL;
    int mUsed = 0;
    java.util.Date mLastActivity = null;

    /**
     * @throws ClassNotFoundException 
     * @throws SQLException 
     *
     */
    public PooledConnection(String pDriverClass, String pURL, String pUserName, String pPassword, String pPreSQL,
            boolean pAllowReuse) throws ClassNotFoundException, SQLException {
        super();

        mAllowReuse = pAllowReuse;

        mDriverClass = pDriverClass;
        mURL = pURL;
        mUserName = pUserName;
        mPassword = pPassword;

        if (pPreSQL == null) {
            pPreSQL = NULL;
        }

        mPreSQL = pPreSQL;

        // create new connection and mark element as in use.
        Class.forName(pDriverClass);
        mConnection = DriverManager.getConnection(pURL, pUserName, pPassword);

        if (pPreSQL != NULL) {
            Statement stmt = mConnection.createStatement();
            try {
            stmt.execute(pPreSQL);
            } catch(SQLException e) {
                ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE,"Pre SQL setting for connection failed");
                stmt.close();
                mConnection.close();
                throw e;
            }
        }
                
        this.setInUse(false);
    }

    public boolean timeout(java.util.Date pCurrentDate) throws Exception {
        if ((this.mLastActivity == null) || ((pCurrentDate.getTime() - this.mLastActivity.getTime()) > TIMEOUT)) {
            if (this.mConnection != null) {
                this.mConnection.close();
            }

            return true;
        }

        return false;
    }

    private Thread owner=null;
    
    public void setInUse(boolean pInUse) throws SQLException {
        if (pInUse) {
            mLastActivity = new java.util.Date();
            mConnection.setAutoCommit(false);
            owner = Thread.currentThread();
        }
        else {
            mConnection.setAutoCommit(true);
            owner = null;
        }

        mInUse = pInUse;
    }

    public boolean inUse() {
        if(owner!=null &&owner.isAlive()==false){
            ResourcePool.LogMessage(this,ResourcePool.WARNING_MESSAGE,"Connection is in use by a thread that is not alive, please report to technical support - Thread: " + owner.getName());
            owner = null;
        }
        
        
        return this.mInUse;
    }

    private static final String NULL = "";

    public boolean match(String pDriverClass, String pURL, String pUserName, String pPassword, String pPreSQL,
            boolean pAllowReuse) {
        if (pPreSQL == null) {
            pPreSQL = NULL;
        }

        if (mAllowReuse == false) {
            return false;
        }

        if (pDriverClass.equals(mDriverClass) && pURL.equals(mURL) && pUserName.equals(mUserName)
                && pPassword.equals(mPassword) && pPreSQL.equals(mPreSQL)) {
            return true;
        }

        return false;
    }
}
