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
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ETLJobExecutor;
import com.kni.etl.Metadata;
import com.kni.etl.OSJobExecutor;
import com.kni.etl.SQLJobExecutor;
import com.kni.etl.TableauJobExecutor;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;
import com.kni.util.ExternalJarLoader;

// TODO: Auto-generated Javadoc
/**
 * The Class RunJob.
 */
public class RunJob {

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the args
	 * 
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {

		String ketldir = System.getenv("KETLDIR");
		if (ketldir == null) {
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.WARNING_MESSAGE,
					"KETLDIR not set, defaulting to working dir");
			ketldir = ".";
		}

		ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf"
				+ File.separator + "Extra.Libraries"), "ketlextralibs", ";");

		RunJob rj = new RunJob();

		rj.execute(args);

	}

	private String servername;
	private String username;
	private String url;
	private String driver;
	private Metadata md;
	private String password;
	private String mdprefix;
	private Document xmlConfig;
	private Node nCurrentServer;
	private Boolean useMD;

	/**
	 * Execute.
	 * 
	 * @param args
	 *            the args
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private void execute(String[] args) throws Exception {

		try {
			// declare XML filename
			String fileName = null;

			// declare job name override
			String jobID = null;
			String configFile = Metadata.CONFIG_FILE;

			for (String element : args) {

				if ((fileName == null) && (element.indexOf("FILE=") != -1)) {
					fileName = ArgumentParserUtil.extractArguments(element,
							"FILE=");
				}

				if ((configFile == null) && (element.indexOf("CONFIG=") != -1)) {
					configFile = ArgumentParserUtil.extractArguments(element,
							"CONFIG=");
				}
				if ((this.servername == null)
						&& (element.indexOf("SERVER=") != -1)) {
					this.servername = ArgumentParserUtil.extractArguments(
							element, "SERVER=");
				}
				if ((this.useMD == null) && (element.indexOf("USEMD=") != -1)) {
					this.useMD = ArgumentParserUtil.extractArguments(element,
							"USEMD=").equalsIgnoreCase("Y");
				}
			}

			// if filename is null report error
			if (fileName == null) {
				System.out
						.println("Wrong arguments:  FILE=<XML_FILE> (USEMD=<Y|N>)(CONFIG=<KETLSERVER FILE>) (SERVER=<SERVER>) (JOB_NAME=<NAME>) (PARAMETER=[(TestList),PATH,/u01]) (IGNOREQA=[FileTest,SizeTest])");
				System.out
						.println("example:  FILE=c:\\transform.xml JOB_NAME=Transform SERVER=localhost");

				System.exit(-1);
			}

			// EngineConstants.getSystemXML();

			if (this.useMD != null && this.useMD) {
				this.xmlConfig = Metadata.LoadConfigFile(null, configFile);
				this.connectToServer();
			}
			// metadata object isn't set and login information found then
			// connect to metadata

			ETLJobExecutor kJobExec = new KETLJobExecutor();

			ETLJobExecutor osJobExec = new OSJobExecutor();

			ETLJobExecutor sqlJobExec = new SQLJobExecutor();
			
			ETLJobExecutor tableauJobExec = new TableauJobExecutor();

			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.INFO_MESSAGE, "Executing file " + fileName);
			Document doc;
			doc = XMLHelper.readXMLFromFile(fileName);

			if (doc == null)
				throw new RuntimeException("File \'" + fileName
						+ "\' not found");

			NodeList nl = doc.getElementsByTagName("JOB");

			for (int i = 0; i < nl.getLength(); i++) {
				Node nd = nl.item(i);

				jobID = XMLHelper.getAttributeAsString(nd.getAttributes(),
						"ID", null);
				if (jobID == null)
					throw new RuntimeException(
							"Job does not contain an ID attribute, aborting..");

				String type = XMLHelper.getAttributeAsString(
						nd.getAttributes(), "TYPE", null);
				ETLJobExecutor cur = null;
				if (type.startsWith("KETL")) {
					cur = kJobExec;
				} else if (type.equals("SQL")) {
					cur = sqlJobExec;
				} else if (type.equals("OSJOB")) {
					cur = osJobExec;
				} else if (type.equals("TABLEAUJOB")) {
					cur = tableauJobExec;
				} else if (type.equals("EMPTYJOB")) {
					ResourcePool.LogMessage(Thread.currentThread(),
							ResourcePool.INFO_MESSAGE, "Skipping empty job "
									+ jobID);
				}

				if (cur == null)
					ResourcePool.LogMessage(Thread.currentThread(),
							ResourcePool.INFO_MESSAGE,
							"Unknown job type, skipping job " + jobID);
				else {

					String[] vargs = new String[args.length + 1];

					System.arraycopy(args, 0, vargs, 0, args.length);
					vargs[vargs.length - 1] = "JOBID=" + jobID;
					ETLJobExecutor._execute(vargs, cur, false, 0);
				}

			}
		} finally {
			if (this.md != null)
				this.md.closeMetadata();
		}
	}

	private String connectToServer() throws Exception {
		if (this.md != null) {
			this.md.closeMetadata();
			this.md = null;
		}

		// get server to connect to.
		if (this.servername == null) // use default server
		{
			Node n = XMLHelper.findElementByName(this.xmlConfig, "SERVERS",
					null, null);
			if (n == null) {
				ResourcePool
						.LogMessage("KETLServers.xml is missing the root node SERVERS, please review file");
				System.exit(-1);
			}
			this.servername = XMLHelper.getAttributeAsString(n.getAttributes(),
					"DEFAULTSERVER", "");
		}

		if (this.servername.equalsIgnoreCase("LOCALHOST")) {
			try {
				InetAddress thisIp = InetAddress.getLocalHost();
				this.servername = thisIp.getHostName();

				// try for localhost
				this.nCurrentServer = XMLHelper.findElementByName(
						this.xmlConfig, "SERVER", "NAME", "localhost");

				if (this.nCurrentServer == null) {
					this.nCurrentServer = XMLHelper.findElementByName(
							this.xmlConfig, "SERVER", "NAME", "LOCALHOST");
				}

				// try for explicit name
				if (this.nCurrentServer == null) {
					this.nCurrentServer = XMLHelper.findElementByName(
							this.xmlConfig, "SERVER", "NAME", this.servername);
				}
			} catch (UnknownHostException e) {
				return "Connection failure: Could not get system hostname please supply servername";
			}
		} else {
			this.nCurrentServer = XMLHelper.findElementByName(this.xmlConfig,
					"SERVER", "NAME", this.servername);
		}

		if (this.nCurrentServer == null) {
			return "ERROR: Server " + this.servername + " not found!";
		}

		this.username = XMLHelper.getChildNodeValueAsString(
				this.nCurrentServer, "USERNAME", null, null, null);
		this.password = XMLHelper.getChildNodeValueAsString(
				this.nCurrentServer, "PASSWORD", null, null, null);

		this.url = XMLHelper.getChildNodeValueAsString(this.nCurrentServer,
				"URL", null, null, null);
		this.driver = XMLHelper.getChildNodeValueAsString(this.nCurrentServer,
				"DRIVER", null, null, null);
		this.mdprefix = XMLHelper.getChildNodeValueAsString(
				this.nCurrentServer, "MDPREFIX", null, null, null);
		String passphrase = XMLHelper.getChildNodeValueAsString(
				this.nCurrentServer, "PASSPHRASE", null, null, null);

		// metadata object isn't set and login information found then connect to
		// metadata
		if ((this.md == null) && (this.username != null)) {

			try {
				Metadata mds = new Metadata(true, passphrase);
				mds.setRepository(this.username, this.password, this.url,
						this.driver, this.mdprefix);
				this.servername = XMLHelper.getAttributeAsString(
						this.nCurrentServer.getAttributes(), "NAME",
						this.servername);
				ResourcePool.setMetadata(mds);
				this.md = ResourcePool.getMetadata();

			} catch (Exception e1) {
				throw new Exception("Connecting to metadata - "
						+ e1.getMessage());
			}
		}

		return "Connected to " + this.servername;
	}
}
