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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import sun.security.provider.MD2;

import com.kni.etl.ETLJob;
import com.kni.etl.EngineConstants;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLCluster;
import com.kni.etl.ketl.KETLCluster.Server;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ExternalJarLoader;

// TODO: Auto-generated Javadoc
/**
 * The Class ExecuteJob.
 */
class ETLDaemonMonitor {

	/**
	 * ExecuteJob constructor comment.
	 */
	public ETLDaemonMonitor() {
		super();
	}

	/**
	 * Extract arguments.
	 * 
	 * @param pArg
	 *            the arg
	 * @param pVarName
	 *            the var name
	 * 
	 * @return the string
	 */
	public static String extractArguments(String pArg, String pVarName) {
		String result = null;
		int argPos = -1;

		argPos = pArg.indexOf(pVarName);

		if (argPos != -1) {
			String fields = pArg.substring(pVarName.length());

			if (fields.length() > 0) {
				result = fields;
			}
		}

		return (result);
	}

	/**
	 * Extract multiple arguments.
	 * 
	 * @param pArg
	 *            the arg
	 * @param pVarName
	 *            the var name
	 * 
	 * @return the string[]
	 */
	public static String[] extractMultipleArguments(String pArg, String pVarName) {
		String[] result = null;
		int argPos = -1;

		argPos = pArg.indexOf(pVarName);

		if (argPos != -1) {
			String fields = pArg.substring(pVarName.length(), pArg.indexOf(")", pVarName.length()));

			if (fields.indexOf(',') != -1) {
				// string contains multiple files
				StringTokenizer st = new StringTokenizer(fields, ",");

				int nFields = st.countTokens();

				result = new String[nFields];

				int pos = 0;

				while (st.hasMoreTokens()) {
					result[pos] = st.nextToken();
					pos++;
				}
			} else if (fields.length() > 0) {
				result = new String[1];
				result[0] = fields;
			}
		}

		return (result);
	}

	/**
	 * Connect to server.
	 * 
	 * @param xmlConfig
	 *            the xml config
	 * @param pServerName
	 *            the server name
	 * 
	 * @return the metadata
	 * 
	 * @throws Exception
	 *             the exception
	 */
	static private Metadata connectToServer(Document xmlConfig, String pServerName) throws Exception {
		Node nCurrentServer;
		String password;
		String url;
		String driver;
		String mdprefix;
		String username;
		Metadata md = null;
		nCurrentServer = XMLHelper.findElementByName(xmlConfig, "SERVER", "NAME", pServerName);

		if (nCurrentServer == null) {
			throw new Exception("ERROR: Server " + pServerName + " not found!");
		}

		username = XMLHelper.getChildNodeValueAsString(nCurrentServer, "USERNAME", null, null, null);
		password = XMLHelper.getChildNodeValueAsString(nCurrentServer, "PASSWORD", null, null, null);

		url = XMLHelper.getChildNodeValueAsString(nCurrentServer, "URL", null, null, null);
		driver = XMLHelper.getChildNodeValueAsString(nCurrentServer, "DRIVER", null, null, null);
		mdprefix = XMLHelper.getChildNodeValueAsString(nCurrentServer, "MDPREFIX", null, null, null);
		String passphrase = XMLHelper.getChildNodeValueAsString(nCurrentServer, "PASSPHRASE", null, null, null);

		// metadata object isn't set and login information found then connect to
		// metadata

		try {
			Metadata mds = new Metadata(true, passphrase);
			mds.setRepository(username, password, url, driver, mdprefix);
			pServerName = XMLHelper.getAttributeAsString(nCurrentServer.getAttributes(), "NAME", pServerName);
			ResourcePool.setMetadata(mds);
			md = ResourcePool.getMetadata();

		} catch (Exception e1) {
			throw new Exception("ERROR: Connecting to metadata - " + e1.getMessage());
		}

		return md;
	}

	private static enum EXIT_CODES {
		SUCCESS, INVALIDSTATE, INVALIDARGUMENTS, METADATA_ERROR
	};

	private static RandomAccessFile lckFile;

	private static FileChannel channel;

	private static FileLock exLck;

	private static final String MONITOR_LOCK = "ketlMonitor.lck";// lock file

	public static boolean lockMonitorInstance() {

		try {
			if (lckFile == null) {
				lckFile = new RandomAccessFile(new File(MONITOR_LOCK), "rw");
			}

			channel = lckFile.getChannel();

			exLck = channel.tryLock(1, 1, false);
			if (exLck != null) {
				return true;
			}
		} catch (Throwable e) {
			e.printStackTrace();
			return false;
		}

		ResourcePool.logError("A " + MONITOR_LOCK + " file exists! A monitor maybe already running.");
		
		if (exLck != null) {
			try {
				exLck.release();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (channel != null) {
			try {
				channel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (lckFile != null) {
			try {
				lckFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			lckFile = null;
		}
		
		System.exit(EXIT_CODES.INVALIDSTATE.ordinal());
		
		return false;
		
	}

	/**
	 * Starts the application.
	 * 
	 * @param args
	 *            an array of command-line arguments
	 */
	public static void main(java.lang.String[] args) {

		String ketldir = System.getenv("KETLDIR");
		if (ketldir == null) {
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
					"KETLDIR not set, defaulting to working dir");
			ketldir = ".";
		}
		
		if (lockMonitorInstance() == false)
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
					"A monitor lock could not be assigned, duplicate monitors exist, if you want to restart the monitor, identify the process and kill it");


		ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf" + File.separator + "Extra.Libraries"),
				"ketlextralibs", ";");

		// String mdServer = null;
		String jobID = null;

		String server = null;

		// String mdServer = null;
		String projectID = null;

		// String mdServer = null;
		String ignoreDependencies = null;

		// String mdServer = null;
		String allowMultiple = null;

		boolean asynchronous = true, avoidQueue = false;
		Integer pollTime = null, notifyTime = null;

		for (String element : args) {
			if ((server == null) && (element.indexOf("CLUSTER=") != -1)) {
				server = ETLDaemonMonitor.extractArguments(element, "CLUSTER=");
			}

			if ((pollTime == null) && (element.indexOf("POLL=") != -1)) {
				pollTime = Integer.parseInt(ETLDaemonMonitor.extractArguments(element, "POLL="));
			}

			if ((notifyTime == null) && (element.indexOf("NOTIFYTIME=") != -1)) {
				notifyTime = Integer.parseInt(ETLDaemonMonitor.extractArguments(element, "NOTIFYTIME="));
			}
		}

		if (pollTime == null)
			pollTime = 60;
		if (notifyTime == null)
			notifyTime = 300;

		if (server == null) {
			System.out.println("Wrong arguments:  CLUSTER=<KETL_MD_NAME> {POLLTIME=<SECONDS>} {NOTIFYTIME=<SECONDS>}");
			System.out.println("example:  CLUSTER=TEST");

			System.exit(EXIT_CODES.INVALIDARGUMENTS.ordinal());
		}
		Metadata md = null;

		
		InetAddress thisIp;

		
		try {
			md = ETLDaemonMonitor.connectToServer(Metadata.LoadConfigFile(null, Metadata.CONFIG_FILE), server);
		} catch (Exception e1) {
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Connecting to metadata - "
					+ e1.getMessage());
			System.exit(EXIT_CODES.METADATA_ERROR.ordinal());
		}

		try {
			String monitorName;
			
			thisIp = InetAddress.getLocalHost();
			monitorName = thisIp.getHostName();						
	
			Set<Integer> warningSent = new HashSet<Integer>();
			while (true) {
				for (Server s : md.getAliveServers()) {

					long diff = (s.mSystemTime.getTime() - s.mLastPing.getTime()) / 1000;

					if (diff >= notifyTime) {
						if (warningSent.contains(s.mServerID) == false) {
							md
									.sendEmailToAll("KETL Server " + s.mName + " Offline","Monitor " + monitorName + " has detected that KETL server " + s.mName
											+ " appears to be offline and has not pinged the metadata in " + diff
											+ " seconds.");
							warningSent.add(s.mServerID);
						}
					} else {
						if (warningSent.remove(s.mServerID)) {
							md.sendEmailToAll("KETL Server " + s.mName + " Recovered", "Monitor " + monitorName + " has detected that KETL server " + s.mName
									+ " appears to be online again and pinged the metadata in last " + diff
									+ " seconds.");
						}
					}
					File f = new File(EngineConstants.MONITORPATH + File.separator + s.mName + ".monitor");
					FileWriter fw = new FileWriter(f);
					fw.append(Long.toString(diff));
					fw.close();
				}
				Thread.sleep(pollTime * 1000);
			}

		} catch (SQLException e) {
			ResourcePool.logMessage(e);

		} catch (Exception e) {
			ResourcePool.logMessage(e);
		}

		ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Invalid state reached.");

		exit(md, EXIT_CODES.INVALIDSTATE.ordinal());
	}

	private static void exit(Metadata md, int exitCode) {
		if (md != null)
			md.closeMetadata();

		System.exit(exitCode);
	}

}
