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
 * Created on Apr 6, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import com.kni.etl.dbutils.ResourcePool;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLCluster.
 */
public class KETLCluster {

    /** The un assigned exec. */
    HashMap unAssignedExec = new HashMap();

    /**
     * The Class Server.
     */
    class Server {

        /** The name. */
        public String mName;
        
        /** The started. */
        public Timestamp mStarted;
        
        /** The system time. */
        public Timestamp mSystemTime;
        
        /** The last ping. */
        public Timestamp mLastPing;
        
        /** The status. */
        public String mStatus;
        
        /** The executors. */
        public HashMap mExecutors = new HashMap();
        
        /** The server ID. */
        public int mServerID;

        /**
         * The Constructor.
         * 
         * @param mName The name
         * @param mStarted The started
         * @param mLastPing The last ping
         * @param mStatus The status
         * @param mServerID The server ID
         * @param mSystemTime The system time
         */
        public Server(int mServerID, String mName, Timestamp mStarted, Timestamp mLastPing, String mStatus,
                Timestamp mSystemTime) {
            super();
            this.mServerID = mServerID;
            this.mName = mName;
            this.mStarted = mStarted;
            this.mLastPing = mLastPing;
            this.mStatus = mStatus;
            this.mSystemTime = mSystemTime;
        }

        /**
         * Adds the executor.
         * 
         * @param mName The name
         * @param mObject The object
         */
        public void addExecutor(String mName, Object mObject) {
            this.mExecutors.put(mName, mObject);
        }

        /**
         * Gets the executor.
         * 
         * @param mName The name
         * 
         * @return the executor
         */
        public Object getExecutor(String mName) {
            return this.mExecutors.get(mName);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();

            if (this.mStatus.equalsIgnoreCase("Active")
                    && ((this.mSystemTime.getTime() - this.mLastPing.getTime()) > (120 * 1000))) {
                sb.append("\n  WARNING   : System reports active but has not pinged in the last 120 seconds!");
            }

            sb.append("\n  Server    : " + this.mName);
            sb.append("\n  Status    : " + this.mStatus);
            sb.append("\n  Start Time: " + this.mStarted);
            sb.append("\n  Last Ping : " + this.mLastPing);
            sb.append("\n  Executors (Stats)\n");

            Set set = this.mExecutors.keySet();

            Iterator iter = set.iterator();

            while (iter.hasNext()) {
                Object o = this.mExecutors.get(iter.next());
                sb.append(o);
            }

            return sb.toString();
        }
    }

    /**
     * Gets the server list.
     * 
     * @return the server list
     */
    public String[] getServerList() {
        ArrayList ls = new ArrayList();
        Set set = this.mServers.keySet();
        Iterator iter = set.iterator();

        while (iter.hasNext()) {
            Server s = (Server) this.mServers.get(iter.next());
            ls.add("ID: " + s.mServerID + ", Name: " + s.mName + ", Status: " + s.mStatus + ", Last Ping: "
                    + s.mLastPing);
        }

        String[] res = new String[ls.size()];
        ls.toArray(res);
        return res;
    }

    /**
     * The Class Executor.
     */
    class Executor {

        /** The name. */
        String mName;
        
        /** The states. */
        ArrayList mStates = new ArrayList();
        
        /** The count. */
        int mCount;

        /**
         * Instantiates a new executor.
         * 
         * @param mName The name
         * @param mCount The count
         */
        public Executor(String mName, int mCount) {
            super();
            this.mName = mName;
            this.mCount = mCount;
        }

        /**
         * Adds the executor state.
         * 
         * @param mState The state
         */
        public void addExecutorState(Object mState) {
            this.mStates.add(mState);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("\t" + this.mName + ": ");

            if (this.mCount != -1) {
                sb.append("(Total: " + this.mCount + ") ");
            }

            for (int i = 0; i < this.mStates.size(); i++) {
                Object o = this.mStates.get(i);
                sb.append(o);
            }

            sb.append("\n");

            return sb.toString();
        }
    }

    /**
     * The Class ExecutorState.
     */
    class ExecutorState {

        /** The status. */
        String mStatus;
        
        /** The count. */
        int mCount;

        /**
         * The Constructor.
         * 
         * @param mStatus The status
         * @param mCount The count
         */
        public ExecutorState(String mStatus, int mCount) {
            super();
            this.mStatus = mStatus;
            this.mCount = mCount;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "(" + this.mStatus + ": " + this.mCount + ") ";
        }
    }

    /** The servers. */
    HashMap mServers = new HashMap();

    /**
     * Adds the server.
     * 
     * @param mServerID The server ID
     * @param mName The name
     * @param mStarted The started
     * @param mLastPing The last ping
     * @param mStatus The status
     * @param mSystemTime The system time
     */
    public void addServer(int mServerID, String mName, Timestamp mStarted, Timestamp mLastPing, String mStatus,
            Timestamp mSystemTime) {
        if (this.mServers.get(new Integer(mServerID)) == null) {
            Server s = new Server(mServerID, mName, mStarted, mLastPing, mStatus, mSystemTime);
            this.mServers.put(new Integer(mServerID), s);
        }
    }

    /**
     * Adds the executor.
     * 
     * @param mServerID The server ID
     * @param mName The name
     * @param mCount The count
     */
    public void addExecutor(int mServerID, String mName, int mCount) {
        Server s = (Server) this.mServers.get(new Integer(mServerID));
        Executor e = new Executor(mName, mCount);
        s.addExecutor(mName, e);
    }

    /**
     * Adds the executor state.
     * 
     * @param mServerID The server ID
     * @param mName The name
     * @param mState The state
     * @param mCount The count
     */
    public void addExecutorState(int mServerID, String mName, String mState, int mCount) {
        Executor e;

        if (mServerID == -1) {
            e = (Executor) this.unAssignedExec.get(mName);

            if (e == null) {
                e = new Executor(mName, -1);
            }

            this.unAssignedExec.put(mName, e);
        }
        else {
            Server s = (Server) this.mServers.get(new Integer(mServerID));
            e = (Executor) s.getExecutor(mName);

            if (e == null) {
                e = new Executor(mName, -1);
                s.addExecutor(mName, e);
            }
        }

        e.addExecutorState(new ExecutorState(mState, mCount));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        // Show how many servers in cluster, how many running
        // Cluster
        // Alive Servers: 4
        // Total Executors (Running:Available:Finished:Failed:Waiting)
        // KETL Job : 2:4
        // SQL Job : 0:0
        // OS Job : 1:1
        // Sessionizer : 1:1
        //
        // Server : devmart01
        // Status : Alive
        // Start Time : 454543
        // Executors (Running:Available)
        // KETL Job : 2:4
        // SQL Job : 0:0
        // OS Job : 1:1
        // Sessionizer : 1:1
        StringBuffer sb = new StringBuffer();

        sb.append("KETL Cluster Status");
        sb.append("\n  Registered Servers: " + this.mServers.size() + "");
        sb.append("\n  Alive Servers     : " + this.getAliveServers() + "");
        sb.append("\n  Pending Jobs\n");

        Set set = this.unAssignedExec.keySet();

        Iterator iter = set.iterator();

        while (iter.hasNext()) {
            Object o = this.unAssignedExec.get(iter.next());
            sb.append(o);
        }

        sb.append(this.getServerDetails());

        return sb.toString();
    }

    /**
     * Gets the server details.
     * 
     * @return the server details
     */
    private String getServerDetails() {
        Set set = this.mServers.keySet();
        Iterator iter = set.iterator();

        StringBuffer sb = new StringBuffer();

        while (iter.hasNext()) {
            Server s = (Server) this.mServers.get(iter.next());
            sb.append(s);
        }

        return sb.toString();
    }

    /**
     * Gets the alive servers.
     * 
     * @return the alive servers
     */
    private int getAliveServers() {
        Set set = this.mServers.keySet();
        Iterator iter = set.iterator();
        int i = 0;
        Date currDate = new Date();

        while (iter.hasNext()) {
            Server s = (Server) this.mServers.get(iter.next());
            long diff = (currDate.getTime() - s.mLastPing.getTime()) / 1000;

            if (s.mStatus.equalsIgnoreCase("Active") && (diff <= 120)) {
                i++;
            }
        }

        return i;
    }

    /**
     * Checks if is server alive.
     * 
     * @param mServerName The server name
     * 
     * @return true, if is server alive
     */
    public boolean isServerAlive(String mServerName) {
        Server s = null;

        Set set = this.mServers.keySet();

        Iterator iter = set.iterator();

        while (iter.hasNext()) {
            Server o = (Server) this.mServers.get(iter.next());

            if (o.mName.equalsIgnoreCase(mServerName)) {
                s = o;
            }
        }

        if (s != null) {
            if (s.mStatus.equalsIgnoreCase("Active") == false) {
                return false;
            }
        }
        else {
            ResourcePool.LogMessage("ERROR: Server not registered");
        }

        return true;
    }

    /**
     * Checks if is server alive.
     * 
     * @param mServerID The server ID
     * 
     * @return true, if is server alive
     */
    public boolean isServerAlive(int mServerID) {
        Server s = null;

        Set set = this.mServers.keySet();

        Iterator iter = set.iterator();

        while (iter.hasNext()) {
            Server o = (Server) this.mServers.get(iter.next());

            if (o.mServerID == mServerID) {
                s = o;
            }
        }

        if (s != null) {
            if (s.mStatus.equalsIgnoreCase("Active") == false) {
                return false;
            }
        }
        else {
            ResourcePool.LogMessage("ERROR: Server not registered");
        }

        return true;
    }
    
    /**
     * Gets the as XML.
     * 
     * @return the as XML
     */
    public String getAsXML() {
        return "";
    }
}
