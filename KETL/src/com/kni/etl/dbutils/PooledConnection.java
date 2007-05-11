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

// TODO: Auto-generated Javadoc
/**
 * The Class PooledConnection.
 */
public class PooledConnection {

    /** The TIMEOUT. */
    private static long TIMEOUT = 60 * 1000; // 60 seconds
    
    /** The connection. */
    Connection mConnection;
    
    /** The in use. */
    private boolean mInUse = false;
    
    /** The allow reuse. */
    boolean mAllowReuse = true;
    
    /** The driver class. */
    String mDriverClass;
    
    /** The URL. */
    String mURL;
    
    /** The user name. */
    String mUserName;
    
    /** The password. */
    String mPassword;
    
    /** The pre SQL. */
    String mPreSQL;
    
    /** The used. */
    int mUsed = 1;
    
    /** The last activity. */
    java.util.Date mLastActivity = null;

    /**
     * The Constructor.
     * 
     * @param pDriverClass the driver class
     * @param pURL the URL
     * @param pUserName the user name
     * @param pPassword the password
     * @param pPreSQL the pre SQL
     * @param pAllowReuse the allow reuse
     * 
     * @throws ClassNotFoundException the class not found exception
     * @throws SQLException the SQL exception
     */
    public PooledConnection(String pDriverClass, String pURL, String pUserName, String pPassword, String pPreSQL,
            boolean pAllowReuse) throws ClassNotFoundException, SQLException {
        super();

        this.mAllowReuse = pAllowReuse;

        this.mDriverClass = pDriverClass;
        this.mURL = pURL;
        this.mUserName = pUserName;
        this.mPassword = pPassword;

        if (pPreSQL == null) {
            pPreSQL = PooledConnection.NULL;
        }

        this.mPreSQL = pPreSQL;

        // create new connection and mark element as in use.
        Class.forName(pDriverClass);
        this.mConnection = DriverManager.getConnection(pURL, pUserName, pPassword);

        if (pPreSQL != PooledConnection.NULL) {
            Statement stmt = this.mConnection.createStatement();
            try {
            stmt.execute(pPreSQL);
            } catch(SQLException e) {
                ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE,"Pre SQL setting for connection failed");
                stmt.close();
                this.mConnection.close();
                throw e;
            }
        }
             
        try {
            this.setInUse(false);
        } catch (SQLException e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error getting connection");
            this.mConnection.close();
            throw e;
        }
    }

    /**
     * Timeout.
     * 
     * @param pCurrentDate the current date
     * 
     * @return true, if successful
     * 
     * @throws Exception the exception
     */
    public boolean timeout(java.util.Date pCurrentDate) throws Exception {
        if ((this.mLastActivity == null) || ((pCurrentDate.getTime() - this.mLastActivity.getTime()) > PooledConnection.TIMEOUT)) {
            if (this.mConnection != null) {
                this.mConnection.close();
            }

            return true;
        }

        return false;
    }

    /** The owner. */
    private Thread owner=null;
    
    /**
     * Sets the in use.
     * 
     * @param pInUse the new in use
     * 
     * @throws SQLException the SQL exception
     */
    public void setInUse(boolean pInUse) throws SQLException {
        if (pInUse) {
            this.mLastActivity = new java.util.Date();
            this.mConnection.setAutoCommit(false);
            this.owner = Thread.currentThread();
        }
        else {
            this.mConnection.setAutoCommit(true);
            this.owner = null;
        }

        this.mInUse = pInUse;
    }

    /**
     * In use.
     * 
     * @return true, if successful
     */
    public boolean inUse() {
        if(this.owner!=null &&this.owner.isAlive()==false){
            ResourcePool.LogMessage(this,ResourcePool.WARNING_MESSAGE,"Connection is in use by a thread that is not alive, please report to technical support - Thread: " + this.owner.getName());
            this.owner = null;
        }
        
        
        return this.mInUse;
    }

    /** The Constant NULL. */
    private static final String NULL = "";

    /**
     * Match.
     * 
     * @param pDriverClass the driver class
     * @param pURL the URL
     * @param pUserName the user name
     * @param pPassword the password
     * @param pPreSQL the pre SQL
     * @param pAllowReuse the allow reuse
     * 
     * @return true, if successful
     */
    public boolean match(String pDriverClass, String pURL, String pUserName, String pPassword, String pPreSQL,
            boolean pAllowReuse) {
        if (pPreSQL == null) {
            pPreSQL = PooledConnection.NULL;
        }

        if (this.mAllowReuse == false) {
            return false;
        }

        if (pDriverClass.equals(this.mDriverClass) && pURL.equals(this.mURL) && pUserName.equals(this.mUserName)
                && pPassword.equals(this.mPassword) && pPreSQL.equals(this.mPreSQL)) {
            return true;
        }

        return false;
    }
}
