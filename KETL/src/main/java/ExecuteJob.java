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
import java.sql.SQLException;
import java.util.StringTokenizer;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.kni.etl.ETLJob;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ExternalJarLoader;

// TODO: Auto-generated Javadoc
/**
 * The Class ExecuteJob.
 */
class ExecuteJob {

	/**
	 * ExecuteJob constructor comment.
	 */
	public ExecuteJob() {
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
			String fields = pArg.substring(pVarName.length(), pArg.indexOf(")",
					pVarName.length()));

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
	static private Metadata connectToServer(Document xmlConfig,
			String pServerName) throws Exception {
		Node nCurrentServer;
		String password;
		String url;
		String driver;
		String mdprefix;
		String username;
		Metadata md = null;
		nCurrentServer = XMLHelper.findElementByName(xmlConfig, "SERVER",
				"NAME", pServerName);

		if (nCurrentServer == null) {
			throw new Exception("ERROR: Server " + pServerName + " not found!");
		}

		username = XMLHelper.getChildNodeValueAsString(nCurrentServer,
				"USERNAME", null, null, null);
		password = XMLHelper.getChildNodeValueAsString(nCurrentServer,
				"PASSWORD", null, null, null);

		url = XMLHelper.getChildNodeValueAsString(nCurrentServer, "URL", null,
				null, null);
		driver = XMLHelper.getChildNodeValueAsString(nCurrentServer, "DRIVER",
				null, null, null);
		mdprefix = XMLHelper.getChildNodeValueAsString(nCurrentServer,
				"MDPREFIX", null, null, null);
		String passphrase = XMLHelper.getChildNodeValueAsString(nCurrentServer,
				"PASSPHRASE", null, null, null);

		// metadata object isn't set and login information found then connect to
		// metadata

		try {
			Metadata mds = new Metadata(true, passphrase);
			mds.setRepository(username, password, url, driver, mdprefix);
			pServerName = XMLHelper.getAttributeAsString(nCurrentServer
					.getAttributes(), "NAME", pServerName);
			ResourcePool.setMetadata(mds);
			md = ResourcePool.getMetadata();

		} catch (Exception e1) {
			throw new Exception("ERROR: Connecting to metadata - "
					+ e1.getMessage());
		}

		return md;
	}

	private static enum EXIT_CODES {
		SUCCESS, NOCAPACITY, INVALIDJOB, FAILED, JOBNOTSUBMITTED, INVALIDPROJECT, INVALIDARGUMENTS, METADATA_ERROR, INVALIDSTATE
	};

	/**
	 * Starts the application.
	 * 
	 * @param args
	 *            an array of command-line arguments
	 */
	public static void main(java.lang.String[] args) {

		String ketldir = System.getenv("KETLDIR");
		if (ketldir == null) {
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.WARNING_MESSAGE,
					"KETLDIR not set, defaulting to working dir");
			ketldir = ".";
		}

		ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf"
				+ File.separator + "Extra.Libraries"), "ketlextralibs", ";");

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

		for (String element : args) {
			if ((server == null) && (element.indexOf("SERVER=") != -1)) {
				server = ExecuteJob.extractArguments(element, "SERVER=");
			}

			if ((jobID == null) && (element.indexOf("JOB_ID=") != -1)) {
				jobID = ExecuteJob.extractArguments(element, "JOB_ID=");
			}

			if ((projectID == null) && (element.indexOf("PROJECT_ID=") != -1)) {
				projectID = ExecuteJob.extractArguments(element, "PROJECT_ID=");
			}

			if ((ignoreDependencies == null)
					&& (element.indexOf("IGNORE_DEPENDENCIES=") != -1)) {
				ignoreDependencies = ExecuteJob.extractArguments(element,
						"IGNORE_DEPENDENCIES=");
			}

			if (element.indexOf("ASYNC=") != -1) {
				String tmp = ExecuteJob.extractArguments(element, "ASYNC=");
				if (tmp.equalsIgnoreCase("FALSE")) {
					asynchronous = false;
				}
			}

			if (element.indexOf("AVOIDQUEUE=") != -1) {
				String tmp = ExecuteJob
						.extractArguments(element, "AVOIDQUEUE=");
				if (tmp.equalsIgnoreCase("TRUE")) {
					avoidQueue = true;
				}
			}

			if ((allowMultiple == null)
					&& (element.indexOf("ALLOW_MULTIPLE=") != -1)) {
				allowMultiple = ExecuteJob.extractArguments(element,
						"ALLOW_MULTIPLE=");
			}
		}

		if (allowMultiple == null) {
			allowMultiple = "FALSE";
		}

		if (ignoreDependencies == null) {
			ignoreDependencies = "FALSE";
		}

		if ((server == null) || (ignoreDependencies == null)
				|| (projectID == null) || (jobID == null)) {
			System.out
					.println("Wrong arguments:  SERVER=TEST PROJECT_ID=1 JOB_ID=TEST_SCRIPT IGNORE_DEPENDENCIES=FALSE [ALLOW_MULTIPLE=FALSE]");
			System.out
					.println("example:  SERVER=TEST ASYNC=TRUE AVOIDQUEUE=FALSE PROJECT_ID=1 JOB_ID=TEST_SCRIPT IGNORE_DEPENDENCIES=FALSE ALLOW_MULTIPLE=FALSE");

			System.exit(EXIT_CODES.INVALIDARGUMENTS.ordinal());
		}
		Metadata md = null;

		try {
			md = ExecuteJob.connectToServer(Metadata.LoadConfigFile(null,
					Metadata.CONFIG_FILE), server);
		} catch (Exception e1) {
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.ERROR_MESSAGE, "Connecting to metadata - "
							+ e1.getMessage());
			System.exit(EXIT_CODES.METADATA_ERROR.ordinal());
		}

		boolean ignoreDeps = false;
		boolean allowMult = false;

		if (ignoreDependencies.compareToIgnoreCase("true") == 0) {
			ignoreDeps = true;
		}

		if (allowMultiple.compareToIgnoreCase("true") == 0) {
			allowMult = true;
		}

		int pID = -1;

		try {
			pID = Integer.parseInt(projectID);
		} catch (Exception e) {
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.ERROR_MESSAGE, "Invalid project id");
			System.exit(EXIT_CODES.INVALIDPROJECT.ordinal());
		}

		try {
			com.kni.etl.ETLJob job = md.getJob(jobID);

			if (job == null) {
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.ERROR_MESSAGE, " Job " + jobID
								+ " not found in metadata.");
				System.exit(EXIT_CODES.INVALIDJOB.ordinal());
			}

			if (avoidQueue) {
				boolean capacity = true;
				// check for capacity

				if (md.executorAvailable(job.getJobTypeID(), job.getPool()) == false) {
					ResourcePool.LogMessage(Thread.currentThread(),
							ResourcePool.ERROR_MESSAGE, job.getJobTypeName()
									+ " executor not available for job "
									+ job.getJobID());
					exit(md, EXIT_CODES.NOCAPACITY.ordinal());
				}

			}
			int loadId;
			if ((loadId = md.executeJob(pID, jobID, ignoreDeps, allowMult)) != -1) {
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.ERROR_MESSAGE,
						"Job submitted to server for direct execution, load id = "
								+ loadId + ".");
				if (asynchronous == false) {
					// wait for job to finish
					int waitTime = 1;
					while (md.loadComplete(loadId) == false) {
						Thread.sleep(waitTime * 1000);
						// ramp up wait time to every 10 seconds
						if (waitTime <= 10)
							waitTime++;
						if(waitTime == 10){
							ETLJob[] loadJobs = md.getLoadJobs(null, loadId);
							for (ETLJob completeJob : loadJobs) {
								if(completeJob.isCompleted()==false)
									ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.INFO_MESSAGE, completeJob
												.getStatus()
												.getExtendedMessage());							
							}
						}
					}

					// job finished get end state, return failed exit code and
					// error message if failed
					ETLJob[] loadJobs = md.getLoadJobs(null, loadId);
					for (ETLJob completeJob : loadJobs) {
						if (completeJob.getJobID().equals(jobID)) {
							if (completeJob.isSuccessful()) {
								ResourcePool.LogMessage(Thread.currentThread(),
										ResourcePool.INFO_MESSAGE, completeJob
												.getStatus()
												.getExtendedMessage());
								exit(md, EXIT_CODES.SUCCESS.ordinal());
							} else {
								ResourcePool.LogMessage(Thread.currentThread(),
										ResourcePool.ERROR_MESSAGE, completeJob
												.getStatus().getExtendedMessage());

								exit(md, EXIT_CODES.FAILED.ordinal());
							}

						}
					}
				}
			} else {
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.ERROR_MESSAGE,
						"Warning Job not submitted to server for execution.");

				exit(md, EXIT_CODES.JOBNOTSUBMITTED.ordinal());
			}
		} catch (SQLException e) {
			ResourcePool.logMessage(e);

		} catch (Exception e) {
			ResourcePool.logMessage(e);
		}

		ResourcePool.LogMessage(Thread.currentThread(),
				ResourcePool.ERROR_MESSAGE,
				"Invalid state reached.");

		exit(md, EXIT_CODES.INVALIDSTATE.ordinal());
	}

	private static void exit(Metadata md, int exitCode) {
		if (md != null)
			md.closeMetadata();

		System.exit(exitCode);
	}

}
