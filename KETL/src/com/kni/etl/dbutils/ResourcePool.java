/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.dbutils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.kni.etl.ETLJob;
import com.kni.etl.EngineConstants;
import com.kni.etl.Metadata;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.ketl.RegisteredLookup;
import com.kni.etl.ketl.smp.ETLCore;
import com.kni.etl.ketl.writer.ForcedException;

/**
 * @author Owner Resource pool, this currently is only a passthrough but will eventually allow for resource pooling
 */
public class ResourcePool {

    private static int CONNECTIONS = 0;
    private static int LOOKUP_CACHES = 1;
    private static int MAX_RESOURCE_TYPES = 2;
    private static Object[] mResources = new Object[ResourcePool.MAX_RESOURCE_TYPES];
    private final static int INUSE_ELEMENT = 0;
    private final static int CONNECTION_ELEMENT = 1;
    private final static int ALLOW_REUSE = 2;
    private final static int MAX_CONNECTION_USE = 1;
    private final static int MAX_CONNECTION_ARRAY_ELEMENTS = 3;
    private static Metadata metadata = null;
    private static Logger logger = null;

    /**
     *
     */
    public ResourcePool() {
        super();
    }

    /*
     * Handle to catch messages rather than using system.out eventually these will log to the metadata
     */
    public static synchronized void LogMessage(String strMessage) {
        ResourcePool.LogMessage(Thread.currentThread().getName(), strMessage);
    }

    private final static int UNKNOWN_MESSAGE_TYPE = -1;
    private final static int UNKNOWN_MESSAGE_LEVEL = -1;
    public final static int FATAL_MESSAGE = 1;
    public final static int DEBUG_MESSAGE = 4;
    public final static int INFO_MESSAGE = 0;
    public final static int ERROR_MESSAGE = 2;
    public final static int WARNING_MESSAGE = 3;
    public final static int EVENT_MESSAGE_TYPE = 0;

    /*
     * Handle to catch messages rather than using system.out eventually these will log to the metadata
     */
    public static synchronized void LogMessage(Object oSource, String strMessage) {
        ResourcePool.LogMessage(oSource, UNKNOWN_MESSAGE_TYPE, UNKNOWN_MESSAGE_LEVEL, strMessage, null, false);
    }

    /*
     * Handle to catch messages rather than using system.out eventually these will log to the metadata
     */
    public static synchronized void LogMessage(Object oSource, int iLevel, String strMessage) {
        ResourcePool.LogMessage(oSource, UNKNOWN_MESSAGE_TYPE, iLevel, strMessage, null, false);
    }

    /*
     * Handle to catch messages rather than using system.out eventually these will log to the metadata and
     */
    public static synchronized void LogMessage(Object oSource, int iType, int iLevel, String strMessage,
            String strExtendedDetails, boolean bToDB) {

        ETLStep eStep = null;
        ETLJob eJob = null;

        // some messages contain an extra carriage return, strip this off.
        if (strMessage != null && strMessage.charAt(strMessage.length() - 1) == '\n')
            strMessage = strMessage.substring(0, strMessage.length() - 1);

        if (oSource instanceof ETLCore) {
            oSource = ((ETLCore) oSource).getOwner();
        }
        if (oSource instanceof ETLPort) {
            oSource = ((ETLPort) oSource).mesStep;
        }
        if (oSource instanceof ETLStep) {
            eStep = (ETLStep) oSource;
            if (eStep.getJobExecutor() != null)
                eJob = eStep.getJobExecutor().getCurrentETLJob();
        }
        else if (oSource instanceof KETLJobExecutor) {
            eJob = ((KETLJobExecutor) oSource).getCurrentETLJob();
        }
        else if (oSource instanceof ETLJob) {
            eJob = (ETLJob) oSource;
        }

        if (eStep != null && eStep.debug() == false && iLevel == ResourcePool.DEBUG_MESSAGE) {
            return;
        }

        if (bToDB) {
            if (ResourcePool.getMetadata() != null) {
                ResourcePool.getMetadata().recordJobMessage(eJob, eStep, iType, iLevel, strMessage, strExtendedDetails,
                        true);
            }
        }
        else {
            StringBuilder sourceDesc = new StringBuilder();
            if (eJob != null)
                sourceDesc.append(eJob.getJobID() + "(" + eJob.getJobExecutionID() + ")-");
            if (eStep != null)
                sourceDesc.append(eStep.toString());
            else
                sourceDesc.append(oSource.toString());

            String lvl;
            switch (iLevel) {
            case DEBUG_MESSAGE:
                lvl = "DEBUG";

                break;
            case FATAL_MESSAGE:
                lvl = "FATAL";

                break;

            case ERROR_MESSAGE:
                lvl = "ERROR";

                break;

            case WARNING_MESSAGE:
                lvl = "WARNING";

                break;

            case INFO_MESSAGE:
                lvl = "INFO";
                break;
            default:
                lvl = "UNKNOWN";
            }

            if ((logger == null) || (System.getProperty("log4j.configuration") == null)) {
                logger = null;

                System.out.println("[" + lvl + "]" + new java.util.Date().toString() + " - [" + sourceDesc.toString()
                        + "] " + strMessage);
            }
            else {
                if (strMessage.endsWith("\n")) {
                    strMessage = strMessage.substring(0, strMessage.length() - 2);
                }

                switch (iLevel) {
                case DEBUG_MESSAGE:
                    logger.debug("[" + sourceDesc.toString() + "] " + strMessage);

                    break;
                case FATAL_MESSAGE:
                    logger.fatal("[" + sourceDesc.toString() + "] " + strMessage);

                    break;

                case ERROR_MESSAGE:
                    logger.error("[" + sourceDesc.toString() + "] " + strMessage);

                    break;

                case WARNING_MESSAGE:
                    logger.warn("[" + sourceDesc.toString() + "] " + strMessage);

                    break;

                case INFO_MESSAGE:
                case UNKNOWN_MESSAGE_LEVEL:
                    logger.info("[" + sourceDesc.toString() + "] " + strMessage);
                }
            }

            // record in source, if source can record
            if (eStep != null || eJob != null || oSource != null) {
                if (eStep != null) {
                    eStep.recordToLog("[" + lvl + "]" + " - [" + sourceDesc.toString() + "] " + strMessage,
                            iLevel == INFO_MESSAGE || iLevel == UNKNOWN_MESSAGE_LEVEL);
                }
                else if (eJob != null && !(iLevel == INFO_MESSAGE || iLevel == UNKNOWN_MESSAGE_LEVEL)) {
                    eJob.logJobMessage("[" + lvl + "]" + new java.util.Date().toString() + " - ["
                            + sourceDesc.toString() + "] " + strMessage);
                }
            }
        }
    }

    /*
     * Handle to catch exceptions rather than using printstacktrace eventually these will log to the metadata
     */
    public static synchronized void LogException(Throwable pException, Object pSource) {
        // temp measure
        ETLStep es = null;

        if (pSource != null) {
            ETLJob eJob = null;
            if (pSource instanceof ETLCore) {
                pSource = ((ETLCore) pSource).getOwner();
            }
            if (pSource instanceof ETLPort) {
                pSource = ((ETLPort) pSource).mesStep;
            }
            if (pSource instanceof ETLStep) {
                es = (ETLStep) pSource;
                if (es.getJobExecutor() != null)
                    eJob = es.getJobExecutor().getCurrentETLJob();
            }
            else if (pSource instanceof KETLJobExecutor) {
                eJob = ((KETLJobExecutor) pSource).getCurrentETLJob();
            }
            else if (pSource instanceof ETLJob) {
                eJob = (ETLJob) pSource;
            }

            if (es != null)
                es.recordToLog(pException, false);
            else if (eJob != null)
                eJob.logJobMessage(pException);
        }

        if ((es != null && es.showException()) || es == null) {
            if (pException instanceof ForcedException)
                System.err.println(pException.getMessage());
            else
                pException.printStackTrace();
        }
    }

    public static synchronized Connection getConnection(String strDriverClass, String strURL, String strUserName,
            String strPassword, String strPrepSQL, boolean bAllowPooling) throws SQLException, ClassNotFoundException {
        // Get hashtable of lookup caches
        Hashtable aConnections = (Hashtable) ResourcePool.mResources[ResourcePool.CONNECTIONS];

        if (aConnections == null) {
            aConnections = new Hashtable();
            ResourcePool.mResources[ResourcePool.CONNECTIONS] = aConnections;
        }

        // get first free connection
        for (Iterator i = aConnections.entrySet().iterator(); i.hasNext();) {
            Map.Entry e = (Map.Entry) i.next();
            PooledConnection connection = (PooledConnection) e.getValue();

            if ((connection.inUse() == false)
                    && connection.match(strDriverClass, strURL, strUserName, strPassword, strPrepSQL, bAllowPooling)) {
                try {
                    connection.setInUse(true);
                    return connection.mConnection;
                } catch (Exception e1) {
                    ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.ERROR_MESSAGE,"Connection has entered an unknown state and will be release");
                    e1.printStackTrace();
                    try {
                        aConnections.remove(connection.mConnection);
                        connection.mConnection.close();
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }

                }
                
            }
        }

        PooledConnection connection = null;

        connection = new PooledConnection(strDriverClass, strURL, strUserName, strPassword, strPrepSQL, bAllowPooling);

        connection.setInUse(true);
        aConnections.put(connection.mConnection, connection);

        return connection.mConnection;
    }

    public static synchronized boolean testConnection(Connection cConnection) {
        try {
            // Test the connection first to make sure it's still alive...
            try {
                // REVIEW: Using get schemas as this doesn't require use of sql to test if connection is alive
                // this may be cached with some drivers, not sure.
                java.sql.ResultSet rs = cConnection.getMetaData().getSchemas();

                int iObjects = 0;

                while (rs.next() && (iObjects == 0)) {
                    String strSchemaName = rs.getString(1);
                    strSchemaName.trim();
                    iObjects++;
                }

                rs.close();

                if(iObjects == 0){
                     rs = cConnection.getMetaData().getTables(null,null, null,null);

                    while (rs.next() && (iObjects == 0)) {
                        String strSchemaName = rs.getString(1);
                        strSchemaName.trim();
                        iObjects++;
                    }

                    rs.close();

                }
                // if no schemas found then connection is invalid, as a schema is required
                // to perform any sql, a database must have one schema at least.
                if (iObjects == 0) {
                    cConnection.close();
                    cConnection = null;

                    return false;
                }
            } catch (Exception e) {
                // If can't read metadata, the connection must be dead. Remove it from our pool...
                cConnection.close();
                cConnection = null;

                return false;
            }
        } catch (Exception e) {
            // If can't create a statement, the connection must really be dead. Remove it from our pool...
            return false;
        }

        return true;
    }

    public static int getConnectionPoolSize(String pConnectionKey) {
        return 100;
    }

    public static synchronized void releaseTimedOutConnections() {
        // Get hashtable of lookup caches
        Hashtable aConnections = (Hashtable) ResourcePool.mResources[ResourcePool.CONNECTIONS];
        java.util.Date dt = new java.util.Date();

        if (aConnections == null) {
            aConnections = new Hashtable();
            ResourcePool.mResources[ResourcePool.CONNECTIONS] = aConnections;
        }

        // get first free connection
        for (Iterator i = aConnections.entrySet().iterator(); i.hasNext();) {
            Map.Entry e = (Map.Entry) i.next();
            PooledConnection connection = (PooledConnection) e.getValue();

            try {
                if ((connection.inUse() == false) && connection.timeout(dt)) {
                    i.remove();
                }
            } catch (Exception e1) {
                ResourcePool.LogException(e1, null);
                aConnections.remove(connection.mConnection);
            }
        }
    }

    public static synchronized boolean releaseConnection(Connection pConnection) {
    	
//    	 Get hashtable of lookup caches
        Hashtable aConnections = (Hashtable) ResourcePool.mResources[ResourcePool.CONNECTIONS];

        if (aConnections == null) {
            aConnections = new Hashtable();
            ResourcePool.mResources[ResourcePool.CONNECTIONS] = aConnections;
        }

        PooledConnection connection = (PooledConnection) aConnections.get(pConnection);

        
        try {
            if (pConnection.isClosed() == false && pConnection.getAutoCommit() == false)
                pConnection.commit();
        } catch (SQLException e1) {
            ResourcePool
                    .LogMessage("WARNING: Could not commit, when returning connection, cursors and locks might not be released, connection will be dropped from resource pool");
            ResourcePool.LogException(e1, null);

            try {
				pConnection.close();
			} catch (Exception e) {
			} finally {
				aConnections.remove(pConnection);
			}
            
            return false;
        }

        
        try {
            if (connection == null) {
                pConnection.close();
            }
            else {
                if (connection.mConnection.isClosed() == false && connection.mAllowReuse
                        && (connection.mUsed < MAX_CONNECTION_USE)) {
                    connection.setInUse(false);
                    connection.mUsed++;
                }
                else {
                    aConnections.remove(pConnection);
                    if (connection.mConnection.isClosed() == false)
                        connection.mConnection.close();
                }
            }
        } catch (Exception e) {
            ResourcePool.LogException(e, null);

            return false;
        }

        return true;
    }

    public static synchronized boolean setMetadata(Metadata pMetadata) {
        ResourcePool.metadata = pMetadata;

        File f = new File("log");
        if (f.exists() == false) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Creating log directory "
                    + f.getAbsolutePath());
            f.mkdir();
        }
        else if (f.exists() && f.isDirectory() == false) {
            System.err.println("Cannot initialize as logging directory is currently a file and not a directory: "
                    + f.getAbsolutePath());
            System.err.println("Please move this file or rename it: " + f.getAbsolutePath());
        }

        try {
            Class.forName("org.apache.log4j.Logger");
            logger = Logger.getLogger("KETL");
        } catch (Exception e) {
            ResourcePool.LogMessage("WARNING log4j not found defaulting to alternative logging mechanism");
        }

        return true;
    }

    public static synchronized Metadata getMetadata() {
        return ResourcePool.metadata;
    }

    
    private static HashMap mLookups;

    public  static synchronized HashMap _getLookup() {
    	if(mLookups == null)
    		mLookups = loadLookups();
    	
    	return mLookups;
    }
    
    private static String mCacheIndexPrefix = null;
     
    private static synchronized HashMap loadLookups() {

        
        try {
            // setup a stream to a physical file on the filesystem
            
            getCacheIndexPrefix();
            
            File fl = new File(EngineConstants.CACHE_PATH + File.separator + "KETL."+mCacheIndexPrefix+".Lookup.index");

            if (fl.exists()) {

                FileInputStream outStream = new FileInputStream(EngineConstants.CACHE_PATH + File.separator
                        + "KETL."+mCacheIndexPrefix+".Lookup.index");

                // attach a stream capable of writing objects to the stream that is
                // connected to the file
                ObjectInputStream objStream = new ObjectInputStream(outStream);

                HashMap hm = (HashMap) objStream.readObject();
                objStream.close();
                outStream.close();

                StringBuffer sb = new StringBuffer();
                Collection tmp = hm.values();
                HashMap goodLookups = new HashMap();
                int i = 0;
                for (Object o : tmp) {
                    RegisteredLookup lk = (RegisteredLookup) o;

                    if(lk.corrupt()) {
                        ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.ERROR_MESSAGE, "Lookup " + lk.getName() + " could not be restored, it will be removed, please rerun the job that creates it");
                        lk.delete();
                    } else{
                        sb.append("" + (++i) + ".\t" + lk.toString() + "\n");
                        goodLookups.put(lk.getName(),lk);
                    }

                }

                if(sb.length()>0) ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, sb.toString());

                return goodLookups;
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return new HashMap();
    }

    public static String getCacheIndexPrefix() {
        if(mCacheIndexPrefix==null){
            mCacheIndexPrefix = Thread.currentThread().getName().contains("Executor")?"Daemon":"Console";
            System.err.println("Defaulting cache prefix to " + mCacheIndexPrefix);
        }
        
        return mCacheIndexPrefix;
    } 
    
    public static void setCacheIndexPrefix(String arg0) {
            mCacheIndexPrefix = arg0;
    }

    private static synchronized void syncLookupsToDisc() {

        try {
            getCacheIndexPrefix();
            // setup a stream to a physical file on the filesystem
            File fl = new File(EngineConstants.CACHE_PATH + File.separator + "KETL."+mCacheIndexPrefix+".Lookup.index");

            if (fl.exists())
                fl.delete();

            FileOutputStream outStream = new FileOutputStream(EngineConstants.CACHE_PATH + File.separator
                    + "KETL."+mCacheIndexPrefix+".Lookup.index");

            // attach a stream capable of writing objects to the stream that is
            // connected to the file
            ObjectOutputStream objStream = new ObjectOutputStream(outStream);

            objStream.writeObject(_getLookup());
            objStream.flush();
            outStream.flush();
            objStream.close();
            outStream.close();

        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

    public static RegisteredLookup getLookup(String lookupName) {
        return (RegisteredLookup) _getLookup().get(lookupName);
    }

    public static synchronized void registerLookup(RegisteredLookup res) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Registering persistent lookup "
                + res.getName());
        _getLookup().put(res.getName(), res);

        syncLookupsToDisc();
    }

    public static synchronized void releaseLoadLookups(int loadId) {
        Collection tmp = _getLookup().values();

        ArrayList res = new ArrayList();
        for (Object o : tmp) {
            RegisteredLookup lk = (RegisteredLookup) o;
            if (lk.getAssociatedLoadID() == loadId) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Deleting load " + loadId
                        + " lookup " + lk.getName());
                lk.delete();
            }
            res.add(lk.getName());
        }

        for (Object o : res)
        	_getLookup().remove(o);
        
        syncLookupsToDisc();
    }

    public static synchronized void releaseAllLookups() {
        Collection tmp = _getLookup().values();

        RegisteredLookup lkLast = null;
        // check cache for random lookups
        for (Object o : tmp) {
            RegisteredLookup lk = (RegisteredLookup) o;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Writing lookup "
                    + lk.getName());
            lk.flush();    		
        }
        
        syncLookupsToDisc();
        
        for (Object o : tmp) {
            RegisteredLookup lk = (RegisteredLookup) o;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Writing lookup "
                    + lk.getName());
            lk.close();
            lkLast = lk;
        }
        
        if(lkLast != null) lkLast.closeCaches();
        
        
    }

    public static synchronized boolean releaseLookup(String lookupName) {
        RegisteredLookup lk = (RegisteredLookup) _getLookup().get(lookupName);

        if (lk == null)
            return false;

        lk.delete();

        _getLookup().remove(lookupName);

        syncLookupsToDisc();
        
        return true;
    }

    public static ArrayList getLookups(int loadId) {
        Collection tmp = _getLookup().values();
        ArrayList res = new ArrayList();
        for (Object o : tmp) {
            RegisteredLookup lk = (RegisteredLookup) o;
            if (lk.getAssociatedLoadID() == loadId) {

                res.add(lk.toString());
            }
        }

        return res;
    }
}