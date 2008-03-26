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
package com.kni.etl.dbutils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

// TODO: Auto-generated Javadoc
/**
 * The Class ResourcePool.
 * 
 * @author Owner Resource pool, this currently is only a passthrough but will
 *         eventually allow for resource pooling
 */
public class ResourcePool {

	/** The CONNECTIONS. */
	private static int CONNECTIONS = 0;

	/** The MAX RESOURCE TYPES. */
	private static int MAX_RESOURCE_TYPES = 2;

	/** The resources. */
	private static Object[] mResources = new Object[ResourcePool.MAX_RESOURCE_TYPES];

	/** The Constant MAX_CONNECTION_USE. */
	private final static int MAX_CONNECTION_USE = 1;

	/** The metadata. */
	private static Metadata metadata = null;

	/** The logger. */
	private static Logger logger = null;

	/**
	 * Instantiates a new resource pool.
	 */
	public ResourcePool() {
		super();
	}

	/*
	 * Handle to catch messages rather than using system.out eventually these
	 * will log to the metadata
	 */
	/**
	 * Log message.
	 * 
	 * @param strMessage
	 *            the str message
	 */
	public static synchronized void LogMessage(String strMessage) {
		ResourcePool.LogMessage(Thread.currentThread().getName(),ResourcePool.INFO_MESSAGE, strMessage);
	}

	public static synchronized void LogMessage(Exception e) {
		ResourcePool.LogException(e, Thread.currentThread());
	}

	public static synchronized void logMessage(String strMessage) {
		ResourcePool.LogMessage(Thread.currentThread().getName(),ResourcePool.INFO_MESSAGE, strMessage);
	}

	public static synchronized void logMessage(Exception e) {
		ResourcePool.LogException(e, Thread.currentThread());
	}

	/** The Constant DEFAULT_ERROR_CODE. */
	private final static int DEFAULT_ERROR_CODE = -1;

	/** The Constant UNKNOWN_MESSAGE_LEVEL. */
	private final static int UNKNOWN_MESSAGE_LEVEL = -1;

	/** The Constant FATAL_MESSAGE. */
	public final static int FATAL_MESSAGE = 1;

	/** The Constant DEBUG_MESSAGE. */
	public final static int DEBUG_MESSAGE = 4;

	/** The Constant INFO_MESSAGE. */
	public final static int INFO_MESSAGE = 0;

	/** The Constant ERROR_MESSAGE. */
	public final static int ERROR_MESSAGE = 2;

	/** The Constant WARNING_MESSAGE. */
	public final static int WARNING_MESSAGE = 3;

	/*
	 * Handle to catch messages rather than using system.out eventually these
	 * will log to the metadata
	 */
	/**
	 * Log message.
	 * 
	 * @param oSource
	 *            the o source
	 * @param strMessage
	 *            the str message
	 */
	public static synchronized void LogMessage(Object oSource, String strMessage) {
		ResourcePool.LogMessage(oSource, ResourcePool.DEFAULT_ERROR_CODE, ResourcePool.UNKNOWN_MESSAGE_LEVEL,
				strMessage, null, false);
	}

	/*
	 * Handle to catch messages rather than using system.out eventually these
	 * will log to the metadata
	 */
	/**
	 * Log message.
	 * 
	 * @param oSource
	 *            the o source
	 * @param iLevel
	 *            The level
	 * @param strMessage
	 *            the str message
	 */
	public static synchronized void LogMessage(Object oSource, int iLevel, String strMessage) {
		ResourcePool.LogMessage(oSource, ResourcePool.DEFAULT_ERROR_CODE, iLevel, strMessage, null, false);
	}

	/*
	 * Handle to catch messages rather than using system.out eventually these
	 * will log to the metadata and
	 */
	/**
	 * Log message.
	 * 
	 * @param oSource
	 *            the o source
	 * @param iErrorCode
	 *            The error code
	 * @param iLevel
	 *            The level
	 * @param strMessage
	 *            the str message
	 * @param strExtendedDetails
	 *            the str extended details
	 * @param bToDB
	 *            the b to DB
	 */
	public static synchronized void LogMessage(Object oSource, int iErrorCode, int iLevel, String strMessage,
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
		} else if (oSource instanceof KETLJobExecutor) {
			eJob = ((KETLJobExecutor) oSource).getCurrentETLJob();
		} else if (oSource instanceof ETLJob) {
			eJob = (ETLJob) oSource;
		}

		if (eStep != null && eStep.debug() == false && iLevel == ResourcePool.DEBUG_MESSAGE) {
			return;
		}

		if (bToDB) {
			if (ResourcePool.getMetadata() != null) {
				ResourcePool.getMetadata().recordJobMessage(eJob, eStep, iErrorCode, iLevel, strMessage,
						strExtendedDetails, true);
			}
		} else {
			StringBuilder sourceDesc = new StringBuilder();
			if (eJob != null)
				sourceDesc.append(eJob.getJobID() + "(" + eJob.getJobExecutionID() + ")-");
			if (eStep != null)
				sourceDesc.append(eStep.toString());
			else
				sourceDesc.append(oSource.toString());

			String lvl = ResourcePool.getAlertLevelName(iLevel);

			if ((ResourcePool.logger == null) || (System.getProperty("log4j.configuration") == null)) {
				ResourcePool.logger = null;

				System.out.println("[" + lvl + "]" + new java.util.Date().toString() + " - [" + sourceDesc.toString()
						+ "] " + strMessage);
			} else {
				if (strMessage.endsWith("\n")) {
					strMessage = strMessage.substring(0, strMessage.length() - 2);
				}

				switch (iLevel) {
				case DEBUG_MESSAGE:
					ResourcePool.logger.debug("[" + sourceDesc.toString() + "] " + strMessage);

					break;
				case FATAL_MESSAGE:
					ResourcePool.logger.fatal("[" + sourceDesc.toString() + "] " + strMessage);

					break;

				case ERROR_MESSAGE:
					ResourcePool.logger.error("[" + sourceDesc.toString() + "] " + strMessage);

					break;

				case WARNING_MESSAGE:
					ResourcePool.logger.warn("[" + sourceDesc.toString() + "] " + strMessage);

					break;

				case INFO_MESSAGE:
				case UNKNOWN_MESSAGE_LEVEL:
					ResourcePool.logger.info("[" + sourceDesc.toString() + "] " + strMessage);
				}
			}

			// record in source, if source can record
			if (eStep != null || eJob != null || oSource != null) {
				if (eStep != null) {
					eStep.recordToLog("[" + lvl + "]" + " - [" + sourceDesc.toString() + "] " + strMessage,
							iLevel == ResourcePool.INFO_MESSAGE || iLevel == ResourcePool.UNKNOWN_MESSAGE_LEVEL);
				} else if (eJob != null
						&& !(iLevel == ResourcePool.INFO_MESSAGE || iLevel == ResourcePool.UNKNOWN_MESSAGE_LEVEL)) {
					eJob.logJobMessage("[" + lvl + "]" + new java.util.Date().toString() + " - ["
							+ sourceDesc.toString() + "] " + strMessage);
				}
			}
		}
	}

	/**
	 * Gets the alert level name.
	 * 
	 * @param iLevel
	 *            The level
	 * @return the alert level name
	 */
	public static String getAlertLevelName(int iLevel) {
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
		return lvl;
	}

	/*
	 * Handle to catch exceptions rather than using printstacktrace eventually
	 * these will log to the metadata
	 */
	/**
	 * Log exception.
	 * 
	 * @param pException
	 *            the exception
	 * @param pSource
	 *            the source
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
			} else if (pSource instanceof KETLJobExecutor) {
				eJob = ((KETLJobExecutor) pSource).getCurrentETLJob();
			} else if (pSource instanceof ETLJob) {
				eJob = (ETLJob) pSource;
			}

			if (es != null)
				es.recordToLog(pException, false);
			else if (eJob != null)
				eJob.logJobMessage(pException);
		}

		if ((es != null && es.showException()) || es == null) {
			if (pException instanceof ForcedException)
				System.err.println(new java.util.Date() + pException.getMessage());
			else
				pException.printStackTrace();
		}
	}

	/**
	 * Gets the connection.
	 * 
	 * @param strDriverClass
	 *            the str driver class
	 * @param strURL
	 *            the str URL
	 * @param strUserName
	 *            the str user name
	 * @param strPassword
	 *            the str password
	 * @param strPrepSQL
	 *            the str prep SQL
	 * @param bAllowPooling
	 *            the b allow pooling
	 * @return the connection
	 * @throws SQLException
	 *             the SQL exception
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	public static synchronized Connection getConnection(String strDriverClass, String strURL, String strUserName,
			String strPassword, String strPrepSQL, boolean bAllowPooling) throws SQLException, ClassNotFoundException {
		// Get hashtable of lookup caches
		Hashtable<Connection, PooledConnection> aConnections = (Hashtable<Connection, PooledConnection>) ResourcePool.mResources[ResourcePool.CONNECTIONS];

		if (aConnections == null) {
			aConnections = new Hashtable<Connection, PooledConnection>();
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
					ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
							"Connection has entered an unknown state and will be release");
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

		DatabaseMetaData mdDB = connection.mConnection.getMetaData();
		if (mdDB != null)
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Connection established to a "
					+ mdDB.getDatabaseProductName() + " Version: " + mdDB.getDatabaseMajorVersion() + "."
					+ mdDB.getDatabaseMinorVersion() + " Database, using Driver Version: "
					+ mdDB.getDriverMajorVersion() + "." + mdDB.getDriverMinorVersion() + " as '" + mdDB.getUserName()
					+ "'.");

		return connection.mConnection;
	}

	/**
	 * Test connection.
	 * 
	 * @param cConnection
	 *            the c connection
	 * @return true, if successful
	 */
	public static synchronized boolean testConnection(Connection cConnection) {
		try {
			// Test the connection first to make sure it's still alive...
			try {
				// REVIEW: Using get schemas as this doesn't require use of sql
				// to test if connection is alive
				// this may be cached with some drivers, not sure.
				java.sql.ResultSet rs = cConnection.getMetaData().getSchemas();

				int iObjects = 0;

				while (rs.next() && (iObjects == 0)) {
					String strSchemaName = rs.getString(1);
					strSchemaName.trim();
					iObjects++;
				}

				rs.close();

				if (iObjects == 0) {
					rs = cConnection.getMetaData().getTables(null, null, null, null);

					while (rs.next() && (iObjects == 0)) {
						String strSchemaName = rs.getString(1);
						strSchemaName.trim();
						iObjects++;
					}

					rs.close();

				}
				// if no schemas found then connection is invalid, as a schema
				// is required
				// to perform any sql, a database must have one schema at least.
				if (iObjects == 0) {
					cConnection.close();
					cConnection = null;

					return false;
				}
			} catch (Exception e) {
				// If can't read metadata, the connection must be dead. Remove
				// it from our pool...
				cConnection.close();
				cConnection = null;

				return false;
			}
		} catch (Exception e) {
			// If can't create a statement, the connection must really be dead.
			// Remove it from our pool...
			return false;
		}

		return true;
	}

	/**
	 * Gets the connection pool size.
	 * 
	 * @param pConnectionKey
	 *            the connection key
	 * @return the connection pool size
	 */
	public static int getConnectionPoolSize(String pConnectionKey) {
		return 100;
	}

	/**
	 * Release timed out connections.
	 */
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

	/**
	 * Release connection.
	 * 
	 * @param pConnection
	 *            the connection
	 * @return true, if successful
	 */
	public static synchronized boolean releaseConnection(Connection pConnection) {

		// Get hashtable of lookup caches
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
			} else {
				if (connection.mConnection.isClosed() == false && connection.mAllowReuse
						&& (connection.mUsed < ResourcePool.MAX_CONNECTION_USE)) {
					connection.setInUse(false);
					connection.mUsed++;
				} else {
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

	/**
	 * Sets the metadata.
	 * 
	 * @param pMetadata
	 *            the metadata
	 * @return true, if successful
	 */
	public static synchronized boolean setMetadata(Metadata pMetadata) {
		ResourcePool.metadata = pMetadata;

		File f = new File("log");
		if (f.exists() == false) {
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Creating log directory "
					+ f.getAbsolutePath());
			f.mkdir();
		} else if (f.exists() && f.isDirectory() == false) {
			System.err.println("[" + new java.util.Date()
					+ "] Cannot initialize as logging directory is currently a file and not a directory: "
					+ f.getAbsolutePath());
			System.err.println("[" + new java.util.Date() + "] Please move this file or rename it: "
					+ f.getAbsolutePath());
		}

		try {
			Class.forName("org.apache.log4j.Logger");
			ResourcePool.logger = Logger.getLogger("KETL");
		} catch (Exception e) {
			ResourcePool.LogMessage("WARNING log4j not found defaulting to alternative logging mechanism");
		}

		return true;
	}

	/**
	 * Gets the metadata.
	 * 
	 * @return the metadata
	 */
	public static synchronized Metadata getMetadata() {
		return ResourcePool.metadata;
	}

	/** The lookups. */
	private static HashMap<String, RegisteredLookup> mLookups;

	/**
	 * _get lookup.
	 * 
	 * @return the hash map
	 */
	public static synchronized HashMap<String, RegisteredLookup> _getLookup() {
		if (ResourcePool.mLookups == null)
			ResourcePool.mLookups = ResourcePool.loadLookups();

		return ResourcePool.mLookups;
	}

	/** The cache index prefix. */
	private static String mCacheIndexPrefix = null;

	/**
	 * Load lookups.
	 * 
	 * @return the hash map
	 */
	private static synchronized HashMap<String, RegisteredLookup> loadLookups() {

		try {
			// setup a stream to a physical file on the filesystem

			ResourcePool.getCacheIndexPrefix();

			File fl = new File(EngineConstants.CACHE_PATH + File.separator + "KETL." + ResourcePool.mCacheIndexPrefix
					+ ".Lookup.index");

			if (fl.exists()) {

				FileInputStream outStream = new FileInputStream(EngineConstants.CACHE_PATH + File.separator + "KETL."
						+ ResourcePool.mCacheIndexPrefix + ".Lookup.index");

				// attach a stream capable of writing objects to the stream that
				// is
				// connected to the file
				ObjectInputStream objStream = new ObjectInputStream(outStream);

				HashMap hm = (HashMap) objStream.readObject();
				objStream.close();
				outStream.close();

				StringBuffer sb = new StringBuffer();
				Collection tmp = hm.values();
				HashMap<String, RegisteredLookup> goodLookups = new HashMap<String, RegisteredLookup>();
				int i = 0;
				for (Object o : tmp) {
					RegisteredLookup lk = (RegisteredLookup) o;

					if (lk.corrupt()) {
						ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Lookup "
								+ lk.getName()
								+ " could not be restored, it will be removed, please rerun the job that creates it");
						lk.delete();
					} else {
						sb.append("" + (++i) + ".\t" + lk.toString() + "\n");
						goodLookups.put(lk.getName(), lk);
					}

				}

				if (sb.length() > 0)
					ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, sb.toString());

				return goodLookups;
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return new HashMap<String, RegisteredLookup>();
	}

	/**
	 * Gets the cache index prefix.
	 * 
	 * @return the cache index prefix
	 */
	public static String getCacheIndexPrefix() {
		if (ResourcePool.mCacheIndexPrefix == null) {
			ResourcePool.mCacheIndexPrefix = Thread.currentThread().getName().contains("Executor") ? "Daemon"
					: "Console";
			System.err.println("[" + new java.util.Date() + "] Defaulting cache prefix to "
					+ ResourcePool.mCacheIndexPrefix);
		}

		return ResourcePool.mCacheIndexPrefix;
	}

	/**
	 * Sets the cache index prefix.
	 * 
	 * @param arg0
	 *            the new cache index prefix
	 */
	public static void setCacheIndexPrefix(String arg0) {
		ResourcePool.mCacheIndexPrefix = arg0;
	}

	/**
	 * Sync lookups to disc.
	 */
	private static synchronized void syncLookupsToDisc() {

		try {
			ResourcePool.getCacheIndexPrefix();
			// setup a stream to a physical file on the filesystem
			File fl = new File(EngineConstants.CACHE_PATH + File.separator + "KETL." + ResourcePool.mCacheIndexPrefix
					+ ".Lookup.index");

			if (fl.exists())
				fl.delete();

			FileOutputStream outStream = new FileOutputStream(EngineConstants.CACHE_PATH + File.separator + "KETL."
					+ ResourcePool.mCacheIndexPrefix + ".Lookup.index");

			// attach a stream capable of writing objects to the stream that is
			// connected to the file
			ObjectOutputStream objStream = new ObjectOutputStream(outStream);

			objStream.writeObject(ResourcePool._getLookup());
			objStream.flush();
			outStream.flush();
			objStream.close();
			outStream.close();

		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

	/**
	 * Gets the lookup.
	 * 
	 * @param lookupName
	 *            the lookup name
	 * @return the lookup
	 */
	public static RegisteredLookup getLookup(String lookupName) {
		return ResourcePool._getLookup().get(lookupName);
	}

	/**
	 * Register lookup.
	 * 
	 * @param res
	 *            the res
	 */
	public static synchronized void registerLookup(RegisteredLookup res) {
		ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Registering persistent lookup "
				+ res.getName());
		ResourcePool._getLookup().put(res.getName(), res);

		ResourcePool.syncLookupsToDisc();
	}

	/**
	 * Release load lookups.
	 * 
	 * @param loadId
	 *            the load id
	 */
	public static synchronized void releaseLoadLookups(int loadId) {
		Collection<RegisteredLookup> tmp = ResourcePool._getLookup().values();

		ArrayList<String> res = new ArrayList<String>();
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
			ResourcePool._getLookup().remove(o);

		ResourcePool.syncLookupsToDisc();
	}

	/**
	 * Release all lookups.
	 */
	public static synchronized void releaseAllLookups() {
		Collection<RegisteredLookup> tmp = ResourcePool._getLookup().values();

		RegisteredLookup lkLast = null;
		// check cache for random lookups
		for (Object o : tmp) {
			RegisteredLookup lk = (RegisteredLookup) o;
			ResourcePool
					.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Writing lookup " + lk.getName());
			lk.flush();
		}

		ResourcePool.syncLookupsToDisc();

		for (Object o : tmp) {
			RegisteredLookup lk = (RegisteredLookup) o;
			ResourcePool
					.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Writing lookup " + lk.getName());
			lk.close();
			lkLast = lk;
		}

		if (lkLast != null)
			lkLast.closeCaches();

	}

	/**
	 * Release lookup.
	 * 
	 * @param lookupName
	 *            the lookup name
	 * @return true, if successful
	 */
	public static synchronized boolean releaseLookup(String lookupName) {
		RegisteredLookup lk = ResourcePool._getLookup().get(lookupName);

		if (lk == null)
			return false;

		lk.delete();

		ResourcePool._getLookup().remove(lookupName);

		ResourcePool.syncLookupsToDisc();

		return true;
	}

	/**
	 * Gets the lookups.
	 * 
	 * @param loadId
	 *            the load id
	 * @return the lookups
	 */
	public static ArrayList<String> getLookups(int loadId) {
		Collection<RegisteredLookup> tmp = ResourcePool._getLookup().values();
		ArrayList<String> res = new ArrayList<String>();
		for (Object o : tmp) {
			RegisteredLookup lk = (RegisteredLookup) o;
			res.add("Load ID:" + lk.getAssociatedLoadID()
					+ (lk.getAssociatedLoadID() != loadId ? " - (Not in current load " + loadId + ")\n\t" : "\n\t")
					+ lk.toString());
		}

		return res;
	}

	public static void logException(Exception e) {
		if (ResourcePool.logger != null)
			ResourcePool.logger.error(e.getMessage(), e);
		else
			e.printStackTrace();
	}

	public static void logError(String msg) {
		if (ResourcePool.logger != null)
			ResourcePool.logger.error(msg);
		else
			System.err.println(msg);
	}
}