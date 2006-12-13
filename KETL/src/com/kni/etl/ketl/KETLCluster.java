/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

public class KETLCluster {

    HashMap unAssignedExec = new HashMap();

    class Server {

        public String mName;
        public Timestamp mStarted;
        public Timestamp mSystemTime;
        public Timestamp mLastPing;
        public String mStatus;
        public HashMap mExecutors = new HashMap();
        public int mServerID;

        /**
         * @param mName
         * @param mStarted
         * @param mLastPing
         * @param mStatus
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

        public void addExecutor(String mName, Object mObject) {
            this.mExecutors.put(mName, mObject);
        }

        public Object getExecutor(String mName) {
            return this.mExecutors.get(mName);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        public String toString() {
            StringBuffer sb = new StringBuffer();

            if (mStatus.equalsIgnoreCase("Active")
                    && ((this.mSystemTime.getTime() - this.mLastPing.getTime()) > (120 * 1000))) {
                sb.append("\n  WARNING   : System reports active but has not pinged in the last 120 seconds!");
            }

            sb.append("\n  Server    : " + mName);
            sb.append("\n  Status    : " + mStatus);
            sb.append("\n  Start Time: " + mStarted);
            sb.append("\n  Last Ping : " + mLastPing);
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

    class Executor {

        String mName;
        ArrayList mStates = new ArrayList();
        int mCount;

        public Executor(String mName, int mCount) {
            super();
            this.mName = mName;
            this.mCount = mCount;
        }

        public void addExecutorState(Object mState) {
            mStates.add(mState);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
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

    class ExecutorState {

        String mStatus;
        int mCount;

        /**
         * @param mStatus
         * @param mCount
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
        public String toString() {
            return "(" + mStatus + ": " + mCount + ") ";
        }
    }

    HashMap mServers = new HashMap();

    public void addServer(int mServerID, String mName, Timestamp mStarted, Timestamp mLastPing, String mStatus,
            Timestamp mSystemTime) {
        if (this.mServers.get(new Integer(mServerID)) == null) {
            Server s = new Server(mServerID, mName, mStarted, mLastPing, mStatus, mSystemTime);
            this.mServers.put(new Integer(mServerID), s);
        }
    }

    public void addExecutor(int mServerID, String mName, int mCount) {
        Server s = (Server) this.mServers.get(new Integer(mServerID));
        Executor e = new Executor(mName, mCount);
        s.addExecutor(mName, e);
    }

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
    
    public String getAsXML() {
        return "";
    }
}
