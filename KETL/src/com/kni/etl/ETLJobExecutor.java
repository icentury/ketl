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
package com.kni.etl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/3/2002 5:39:29 PM)
 * 
 * @author: Administrator
 */
public abstract class ETLJobExecutor extends Thread {

	/** The b shutdown. */
	protected boolean bShutdown = false;

	/** The ll pending queue. */
	protected Queue llPendingQueue = null;

	/** The sleep period. */
	protected int iSleepPeriod = 100;

	/** The jes status. */
	protected ETLJobExecutorStatus jesStatus = new ETLJobExecutorStatus();

	/** The dmf factory. */
	protected DocumentBuilderFactory dmfFactory;

	/** The aes override parameters. */
	protected ArrayList aesOverrideParameters = null;

	/** The aes ignore Q as. */
	protected String[] aesIgnoreQAs = null;

	/** The ms XML override. */
	protected String msXMLOverride = "";

	/** The mb command line. */
	protected boolean mbCommandLine = true;

	private String msType;

	/**
	 * ETLJobExecutorThread constructor comment.
	 */
	public ETLJobExecutor() {
		super();
		this.dmfFactory = DocumentBuilderFactory.newInstance();
	}

	/**
	 * ETLJobExecutorThread constructor comment.
	 * 
	 * @param target
	 *            java.lang.Runnable
	 */
	public ETLJobExecutor(Runnable target) {
		super(target);
	}

	abstract public ETLJob getCurrentETLJob();

	static protected String getExternalSourceCode(Node n) throws MalformedURLException, IOException {

		String src = XMLHelper.getAttributeAsString(n.getAttributes(), ETLJob.EXTERNAL_SOURCE, null);

		if (src == null)
			return null;

		StringBuffer sb = new StringBuffer(1024);
		URL url = new URL(src);
		InputStream stream = url.openConnection().getInputStream();
		InputStreamReader streamReader = new InputStreamReader(stream);
		BufferedReader reader = new BufferedReader(streamReader);

		char[] chars = new char[512];
		int len;
		while ((len = reader.read(chars)) != -1) {
			sb.append(chars, 0, len);
		}

		streamReader.close();
		reader.close();
		return sb.toString();

	}

	/**
	 * ETLJobExecutorThread constructor comment.
	 * 
	 * @param target
	 *            java.lang.Runnable
	 * @param name
	 *            java.lang.String
	 */
	public ETLJobExecutor(Runnable target, String name) {
		super(target, name);
	}

	/**
	 * ETLJobExecutorThread constructor comment.
	 * 
	 * @param name
	 *            java.lang.String
	 */
	public ETLJobExecutor(String name) {
		super(name);
	}

	/**
	 * ETLJobExecutorThread constructor comment.
	 * 
	 * @param group
	 *            java.lang.ThreadGroup
	 * @param target
	 *            java.lang.Runnable
	 */
	public ETLJobExecutor(ThreadGroup group, Runnable target) {
		super(group, target);
	}

	/**
	 * ETLJobExecutorThread constructor comment.
	 * 
	 * @param group
	 *            java.lang.ThreadGroup
	 * @param target
	 *            java.lang.Runnable
	 * @param name
	 *            java.lang.String
	 */
	public ETLJobExecutor(ThreadGroup group, Runnable target, String name) {
		super(group, target, name);
	}

	/**
	 * ETLJobExecutorThread constructor comment.
	 * 
	 * @param group
	 *            java.lang.ThreadGroup
	 * @param name
	 *            java.lang.String
	 */
	public ETLJobExecutor(ThreadGroup group, String name) {
		super(group, name);
	}

	/**
	 * Insert the method's description here. Creation date: (5/3/2002 6:49:24
	 * PM)
	 * 
	 * @param jCurrentJob
	 *            the j current job
	 * @return boolean
	 */
	protected abstract boolean executeJob(ETLJob jCurrentJob);

	/**
	 * Insert the method's description here. Creation date: (5/4/2002 5:34:09
	 * PM)
	 * 
	 * @return int
	 */
	public int getSleepPeriod() {
		return this.iSleepPeriod;
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 11:23:02
	 * AM)
	 * 
	 * @return com.kni.etl.ETLJobExecutorStatus
	 */
	public ETLJobExecutorStatus getStatus() {
		return this.jesStatus;
	}

	/**
	 * Exit.
	 * 
	 * @param code
	 *            the code
	 * @param e
	 *            the e
	 * @param pExitCleanly
	 *            the exit cleanly
	 * @return the int
	 */
	private static int exit(int code, Throwable e, boolean pExitCleanly) {
		if (pExitCleanly)
			return code;

		if (e == null)
			throw new RuntimeException("Exit code: " + code);
		else if (e instanceof RuntimeException)
			throw (RuntimeException) e;
		else {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute.
	 * 
	 * @param args
	 *            the args
	 * @param pETLJobExecutor
	 *            the ETL job executor
	 * @param pExitCleanly
	 *            the exit cleanly
	 */
	public static void execute(String[] args, ETLJobExecutor pETLJobExecutor, boolean pExitCleanly) {
		int res = ETLJobExecutor._execute(args, pETLJobExecutor, pExitCleanly, 0);
		ResourcePool.releaseLoadLookups(0);

		if (pExitCleanly)
			System.exit(res);
	}

	/**
	 * _execute.
	 * 
	 * @param args
	 *            the args
	 * @param pETLJobExecutor
	 *            the ETL job executor
	 * @param pExitCleanly
	 *            the exit cleanly
	 * @param iLoadID
	 *            The load ID
	 * @return the int
	 */
	public static int _execute(String[] args, ETLJobExecutor pETLJobExecutor, boolean pExitCleanly, int iLoadID) {
		// declare XML filename
		String fileName = null;

		// declare job name override
		String jobName = null, jobID = null;

		ArrayList overrideParameters = new ArrayList();
		String[] ignoreQAs = null;
		String server = null;

		for (String element : args) {
			if ((server == null) && (element.indexOf("SERVER=") != -1)) {
				server = ArgumentParserUtil.extractArguments(element, "SERVER=");
			}

			if ((ignoreQAs == null) && (element.indexOf("IGNOREQA=[") != -1)) {
				ignoreQAs = ArgumentParserUtil.extractMultipleArguments(element, "IGNOREQA=[");
			}

			if ((fileName == null) && (element.indexOf("FILE=") != -1)) {
				fileName = ArgumentParserUtil.extractArguments(element, "FILE=");
			}

			if ((jobName == null) && (element.indexOf("JOB_NAME=") != -1)) {
				jobName = ArgumentParserUtil.extractArguments(element, "JOB_NAME=");
			}
			if ((jobID == null) && (element.indexOf("JOBID=") != -1)) {
				jobID = ArgumentParserUtil.extractArguments(element, "JOBID=");
			}

			if ((element.indexOf("LOADID=") != -1)) {
				iLoadID = Integer.parseInt(ArgumentParserUtil.extractArguments(element, "LOADID="));
			}

			if (element.indexOf("PARAMETER=[") != -1) {
				String[] param = ArgumentParserUtil.extractMultipleArguments(element, "PARAMETER=[");
				overrideParameters.add(param);
			}
		}

		// if filename is null report error
		if (fileName == null) {
			System.out
					.println("Wrong arguments:  FILE=<XML_FILE> (SERVER=localhost) (JOB_NAME=<NAME>) (PARAMETER=[(TestList),PATH,/u01]) (IGNOREQA=[FileTest,SizeTest])");
			System.out.println("example:  FILE=c:\\transform.xml JOB_NAME=Transform SERVER=localhost");

			return ETLJobExecutor.exit(com.kni.etl.EngineConstants.WRONG_ARGUMENT_EXIT_CODE, null, pExitCleanly);
		}

		// Read XML from file and set as action...
		StringBuffer sb = new StringBuffer();

		try {
			FileReader inputFileReader = new FileReader(fileName);
			int c;

			while ((c = inputFileReader.read()) != -1) {
				sb.append((char) c);
			}
		} catch (Exception e) {
			System.out.println("Error reading file '" + args[0] + "': " + e.getMessage());

			return ETLJobExecutor.exit(com.kni.etl.EngineConstants.READXML_ERROR_EXIT_CODE, e, pExitCleanly);
		}

		String strJobXML = sb.toString();

		return internalExecute(pETLJobExecutor, pExitCleanly, iLoadID, jobName, jobID, overrideParameters, ignoreQAs,
				server, strJobXML, null);
	}

	public static int execute(ETLJobExecutor pETLJobExecutor, int iLoadID, String jobName, String jobID, String server,
			String strJobXML, ETLJob eJob) {
		return internalExecute(pETLJobExecutor, true, iLoadID, jobName, jobID, new ArrayList(), null, server,
				strJobXML, eJob);
	}

	private static int internalExecute(ETLJobExecutor pETLJobExecutor, boolean pExitCleanly, int iLoadID,
			String jobName, String jobID, ArrayList overrideParameters, String[] ignoreQAs, String server,
			String strJobXML, ETLJob eJob) {
		// metadata object isn't set and login information found then connect to
		// metadata
		long lStartTime;
		long lEndTime;
		getMetadata(server);

		// Build a DOM out of the XML string...
		try {
			Document xmlDOM = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
					new InputSource(new StringReader(strJobXML)));

			NodeList nl = xmlDOM.getElementsByTagName("JOB");
			NodeList nlp = xmlDOM.getElementsByTagName("PARAMETER_LIST");
			NodeList nlq = xmlDOM.getElementsByTagName("QA");

			if ((nl.getLength() > 1) && (jobName != null)) {
				System.out.println("ERROR: JOB_NAME argument not applicable to XML file with multiple jobs");

				return ETLJobExecutor.exit(com.kni.etl.EngineConstants.MULTIJOB_JOB_OVERRIDE_ERROR_EXIT_CODE, null,
						pExitCleanly);
			}

			for (int i = 0; i < nl.getLength(); i++) {
				Node node = nl.item(i);

				boolean execute = (jobID == null || XMLHelper.getAttributeAsString(node.getAttributes(), "ID", "")
						.equals(jobID));

				if (execute && node.getNodeType() == Node.ELEMENT_NODE) {

					ETLJobExecutor je = pETLJobExecutor;

					if (je.supportsJobType(eJob)) {
						return ETLJobExecutor.exit(com.kni.etl.EngineConstants.BADLY_FORMED_ARGUMENT_EXIT_CODE, null,
								pExitCleanly);
					}
					ETLJob kj = eJob != null ? eJob : je.getNewJob();

					kj.setLoadID(iLoadID);

					kj.setGlobalParameterListName(node, XMLHelper.getAttributeAsString(node.getAttributes(),
							EngineConstants.PARAMETER_LIST, null));

					ArrayList ar = new ArrayList();
					je.aesIgnoreQAs = ignoreQAs;
					je.aesOverrideParameters = overrideParameters;

					// add parameter lists from root
					for (int x = 0; x < nlp.getLength(); x++) {
						Node o = nlp.item(x);

						if ((o != null) && (o.getNodeType() == Node.ELEMENT_NODE)) {
							ar.add(o);
						}
					}

					// add qa'sa from root
					for (int x = 0; x < nlq.getLength(); x++) {
						Node o = nlq.item(x);

						if ((o != null) && (o.getNodeType() == Node.ELEMENT_NODE)) {
							ar.add(o);
						}
					}

					for (int x = 0; x < ar.size(); x++) {
						node.appendChild((Node) ar.get(x));
					}

					// build parameter override tag
					je.msXMLOverride = "<" + XMLHelper.PARAMETER_LIST_TAG + " " + XMLHelper.PARAMETER_OVERRIDE_ATTRIB
							+ "=\"TRUE\">\n";

					// add parameter overrides if any
					for (int x = 0; x < je.aesOverrideParameters.size(); x++) {
						String[] str = (String[]) je.aesOverrideParameters.get(x);

						if (str != null) {
							if (str.length == 2) {
								if ((str[0] == null) || (str[1] == null)) {
									System.out.println("ERROR: Badly formed parameter override ParameterName=" + str[0]
											+ ", ParameterValue=" + str[1]);
									return ETLJobExecutor.exit(
											com.kni.etl.EngineConstants.BADLY_FORMED_ARGUMENT_EXIT_CODE, null,
											pExitCleanly);
								}
							}

							if (str.length == 3) {
								if ((str[0] == null) || (str[1] == null) || (str[2] == null)) {
									System.out.println("ERROR: Badly formed parameter override ParameterListName = "
											+ str[0] + " ParameterName=" + str[1] + ", ParameterValue=" + str[2]);
									return ETLJobExecutor.exit(
											com.kni.etl.EngineConstants.BADLY_FORMED_ARGUMENT_EXIT_CODE, null,
											pExitCleanly);
								}
							}

							if (str.length == 2) {
								je.msXMLOverride = je.msXMLOverride + "\t<" + XMLHelper.PARAMETER_TAG + " "
										+ XMLHelper.NAME_TAG + "=\"" + str[0] + "\">" + str[1] + "</"
										+ XMLHelper.PARAMETER_TAG + ">\n";
							} else if (str.length == 3) {
								je.msXMLOverride = je.msXMLOverride + "\t<" + XMLHelper.PARAMETER_TAG + " "
										+ XMLHelper.PARAMETER_LIST_TAG + "=\"" + str[0] + "\" " + XMLHelper.NAME_TAG
										+ "=\"" + str[1] + "\">" + str[2] + "</" + XMLHelper.PARAMETER_TAG + ">\n";
							}
						}
					}

					// close parameter override tag
					je.msXMLOverride = je.msXMLOverride + "</" + XMLHelper.PARAMETER_LIST_TAG + ">\n";

					// build temp DOM to generate nodes
					Document tmpXMLDOM = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
							new InputSource(new StringReader(je.msXMLOverride)));

					// import nodes into main document
					Node newNode = xmlDOM.importNode(tmpXMLDOM.getFirstChild(), true);

					// append new nodes to the job
					node.appendChild(newNode);

					kj.setAction(XMLHelper.outputXML(node));

					if (jobName == null) {
						kj.setJobID(XMLHelper.getAttributeAsString(node.getAttributes(), "ID", XMLHelper
								.getAttributeAsString(node.getAttributes(), "NAME", null)));
					} else {
						kj.setJobID(jobName);
					}

					lStartTime = System.currentTimeMillis();
					je.executeJob(kj);
					lEndTime = System.currentTimeMillis();

					try {
						kj.cleanup();
					} catch (Exception e) {
					}

					if (kj.getStatus().getErrorCode() != 0) {
						ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Job failed ("
								+ kj.getStatus().getErrorCode() + ") : " + kj.getStatus().getErrorMessage());

						return ETLJobExecutor.exit(kj.getStatus().getErrorCode(), kj.getStatus().getException(),
								pExitCleanly);

					}

					ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Total execution time: "
							+ ((lEndTime - lStartTime) / 1000.0) + " seconds");
					ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Job complete.");
				}
			}

		} catch (org.xml.sax.SAXException e) {
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "ERROR: parsing XML document, "
					+ e.toString());
			return (ETLJobExecutor.exit(EngineConstants.INVALID_XML_EXIT_CODE, e, pExitCleanly));
		} catch (RuntimeException e) {
			Throwable t = e.getCause() == null ? e : (Exception) e.getCause();

			int exitCode = EngineConstants.OTHER_ERROR_EXIT_CODE;
			if (t instanceof KETLQAException)
				exitCode = ((KETLQAException) t).getErrorCode();
			else
				ResourcePool.LogException(e.getCause() == null ? e : (Exception) e.getCause(), null);
			return (ETLJobExecutor.exit(exitCode, e, pExitCleanly));
		} catch (Exception e) {
			ResourcePool.LogException(e, null);
			return (ETLJobExecutor.exit(EngineConstants.OTHER_ERROR_EXIT_CODE, e, pExitCleanly));
		}

		return 0;
	}

	private static void getMetadata(String server) {
		// declare metadata object
		Metadata md = null;

		try {
			Document doc;
			if (ResourcePool.getMetadata() == null) {
				if ((doc = Metadata.LoadConfigFile(null, Metadata.CONFIG_FILE)) != null) {
					md = ETLJobExecutor.connectToServer(doc, server);
					ResourcePool.setMetadata(md);
				}
			}
		} catch (Exception e1) {
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
					"Metadata not available, check KETLServers.xml - " + e1.getMessage() + ", ");
			ResourcePool.setMetadata(null);
		}
	}

	/**
	 * Connect to server.
	 * 
	 * @param xmlConfig
	 *            the xml config
	 * @param pServerName
	 *            the server name
	 * @return the metadata
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

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 11:52:15
	 * AM)
	 * 
	 * @return true, if initialize
	 */
	protected abstract boolean initialize();

	private String getLabel() {
		if (this.llPendingQueue.size() > 0)
			return this.msType + this.llPendingQueue.toString();
		ETLJob job;
		if ((job = this.getCurrentETLJob()) != null)
			return this.msType + "[" + job.toString() + "]";

		return this.msType;
	}

	/**
	 * Loops on the job queue, taking each job and running with it. Creation
	 * date: (5/3/2002 5:43:04 PM)
	 */
	@Override
	public void run() {
		ETLJob jCurrentJob;
		boolean bSuccess;

		this.mbCommandLine = false;

		String orginalName = this.getName();

		this.setName(orginalName + "(" + this.getLabel() + ") - Starting");

		// Run any initialization code that the subclasses will need...
		if (this.initialize() == false) {
			this.jesStatus.setStatusCode(ETLJobExecutorStatus.ERROR);

			return;
		}

		while (this.bShutdown == false) {
			this.setName(orginalName + "(" + this.getLabel() + ") - Ready");
			// We're ready to get the next job, so set our status to READY...
			if (this.jesStatus.getStatusCode() != ETLJobExecutorStatus.READY) {
				this.jesStatus.setStatusCode(ETLJobExecutorStatus.READY);
			}

			jCurrentJob = (ETLJob) this.llPendingQueue.poll();

			// If there was no job for us, sleep for a bit and check again...
			if (jCurrentJob == null) {
				try {
					if (this.getSleepPeriod() < 2000) {
						this.setSleepPeriod(this.getSleepPeriod() + 500);
					}

					Thread.sleep(this.getSleepPeriod());
				} catch (Exception e) {
				}

				continue;
			}

			this.setSleepPeriod(100);

			// We've got a job, so set our executor's status to WORKING...
			this.jesStatus.setStatusCode(ETLJobExecutorStatus.WORKING);
			this.setName(this.getName() + "(" + this.getLabel() + ") - Executing");
			// Make sure that this job wasn't cancelled while it was in the
			// queue...
			if (jCurrentJob.isCancelled()) {
				// Set the status and drop the job...
				jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_CANCELLED);
				this.setName(orginalName + "(" + this.getLabel() + ") - Cancelled");
				continue;
			}

			// Update the status of the job and let's go!
			jCurrentJob.getStatus().setStatusCode(ETLJobStatus.EXECUTING);
			bSuccess = this.executeJob(jCurrentJob);

			ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Job executed: ID = " + jCurrentJob.sJobID
					+ ", STATUS ID = " + jCurrentJob.getStatus().getStatusCode() + ", CANCEL STATUS =  "
					+ jCurrentJob.isCancelSuccessfull());
			// Update the status to done if the subclass forgot to...
			if (jCurrentJob.getStatus().getStatusCode() == ETLJobStatus.EXECUTING
					|| jCurrentJob.getStatus().getStatusCode() == ETLJobStatus.ATTEMPT_CANCEL) {
				if (jCurrentJob.isCancelSuccessfull()) {
					jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_CANCELLED);
				} else if (bSuccess) {
					jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL);
				} else {
					jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_FAILED);
				}
			}
		}

		// Run any initialization code that the subclasses will need...but set
		// the shutdown status regardless...
		this.jesStatus.setStatusCode(ETLJobExecutorStatus.SHUTTING_DOWN);
		this.terminate();
		this.jesStatus.setStatusCode(ETLJobExecutorStatus.TERMINATED);

	}

	/**
	 * Insert the method's description here. Creation date: (5/4/2002 8:01:20
	 * PM)
	 * 
	 * @param queue
	 *            the ll queue
	 */
	public void setPendingQueue(Queue queue) {
		this.llPendingQueue = queue;
	}

	/**
	 * Insert the method's description here. Creation date: (5/4/2002 5:34:09
	 * PM)
	 * 
	 * @param newSleepPeriod
	 *            int
	 */
	public void setSleepPeriod(int newSleepPeriod) {
		this.iSleepPeriod = newSleepPeriod;
	}

	/**
	 * This is the publicly accessible function to set the "shutdown" flag for
	 * the thread. It will no longer process any new jobs, but finish what it's
	 * working on. It will then call the internal terminate() function to close
	 * down anything it needs to. BRIAN: should we make this final? Creation
	 * date: (5/3/2002 6:50:09 PM)
	 */
	public void shutdown() {
		this.bShutdown = true;
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 2:49:24
	 * PM)
	 * 
	 * @param jJob
	 *            the j job
	 * @return boolean
	 */
	public abstract boolean supportsJobType(ETLJob jJob);

	/**
	 * Gets the new job.
	 * 
	 * @return the new job
	 * @throws Exception
	 *             the exception
	 */
	public abstract ETLJob getNewJob() throws Exception;

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 11:52:41
	 * AM)
	 * 
	 * @return true, if terminate
	 */
	protected boolean terminate() {
		return true;
	}

	public void setType(String msType) {
		this.msType = msType;
	}

	protected boolean isValidType(ETLJob job) {
		return this.msType == null || this.msType.equals(job.getJobTypeName());
	}

}
