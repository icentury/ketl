/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Mar 12, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLCluster;
import com.kni.etl.sessionizer.PageParserPageDefinition;
import com.kni.etl.sessionizer.PageParserPageParameter;
import com.kni.etl.sessionizer.SessionDefinition;
import com.kni.etl.sessionizer.SessionIdentifier;
import com.kni.etl.stringtools.DesEncrypter;
import com.kni.etl.util.EncodeBase64;
import com.kni.etl.util.XMLHelper;
import com.kni.util.net.smtp.SMTPClient;
import com.kni.util.net.smtp.SMTPReply;

/**
 * Insert the type's description here. Creation date: (3/5/2002 3:13:11 PM)
 * 
 * @author: Kinetic Networks Inc
 */
public class Metadata {

	// private int ActiveJobs = 0;

	// private SimpleDateFormat DateTimeFormatter;

	private String JDBCDriver;

	private java.lang.String JDBCURL;

	// private Integer MaxActiveJobs;

	protected Connection metadataConnection;

	private String Password;

	// private PreparedStatement testConnectionStmt = null;

	private String Username;

	protected Object oLock;

	// Indexes into the parameter list array that is returned...
	public static final int PARAMETER_NAME = 0;

	public static final int PARAMETER_VALUE = 1;

	public static final int SUB_PARAMETER_LIST_NAME = 2;

	public static final int SUB_PARAMETER_LIST = 3;

	protected String tablePrefix = "";

	private boolean bAnsi92OuterJoin = false;

	protected String currentTimeStampSyntax;

	private String nextLoadIDSyntax;
    private boolean useIdentityColumn;

	private String nextServerIDSyntax;

	private String singleRowPullSyntax;

	private String[] dbTypes = { "PostGreSQL", "Oracle","MySQL" };

    private boolean[] dbUseIdentityColumn = {false, false,true};
    private String[] dbTimeStampTypes = { "CURRENT_TIMESTAMP", "SYSDATE","CURRENT_TIMESTAMP" };

	private String[] dbSequenceSyntax = { "nextval('#')", "#.NEXTVAL","SELECT LAST_INSERT_ID()" };

    private String[] dbIncrementIdentityColumnSyntax = {null,null,"UPDATE mysql_sequence SET id=LAST_INSERT_ID(id+1)"};
	private String[] dbSingleRowPull = { "", " FROM DUAL ","" };

    private String[] mLoadTableName = {null,null,"root_load"};
    
	private String mPassphrase;

	private String mPassphraseFilePath = null;

	static private String mKETLPath = null;

	static public void setKETLPath(String arg0) {
		mKETLPath = arg0 == null ? "." : arg0;
	}

	/**
	 * @param configFile
	 */
	static public Document LoadConfigFile(String ketlpath, String configFile) {
		setKETLPath(ketlpath);

		// Read XML config file
		StringBuffer sb = new StringBuffer();
		Document xmlConfig = null;

		try {
			FileReader inputFileReader = new FileReader(configFile);
			int c;

			while ((c = inputFileReader.read()) != -1) {
				sb.append((char) c);
			}

			// turn file into readable nodes
			DocumentBuilder builder = null;

			// Build a DOM out of the XML string...
			try {
				DocumentBuilderFactory dmf = DocumentBuilderFactory
						.newInstance();
				builder = dmf.newDocumentBuilder();
				xmlConfig = builder.parse(new InputSource(new StringReader(sb
						.toString())));
			} catch (org.xml.sax.SAXException e) {
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.ERROR_MESSAGE, "Parsing XML document, "
								+ e.toString());

				System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
			} catch (Exception e) {
				ResourcePool.LogException(e, Thread.currentThread());

				System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
			}
		} catch (Exception e) {
			ResourcePool
					.LogMessage(Thread.currentThread(),
							ResourcePool.WARNING_MESSAGE,
							"Config file not found or readable, some commands will not be available");
		}

		return xmlConfig;
	}

	final static public String CONFIG_FILE = "xml" + File.separator
			+ "KETLServers.xml";

	final static public String SYSTEM_FILE = "xml" + File.separator
			+ "system.xml";

	private DesEncrypter mEncryptor;

	private boolean mEncryptionEnabled = true;

    private PreparedStatement mIncIdentColStmt = null;

    private String mResolvedLoadTableName;

	public Metadata(boolean pEnableEncryption) throws Exception {
		this(pEnableEncryption, null);
	}

	/**
	 * Metadata constructor comment.
	 * 
	 * @param pEnableEncryption
	 *            TODO
	 * @throws Exception
	 */
	public Metadata(boolean pEnableEncryption, String pPassphrase)
			throws Exception {
		super();

		this.mEncryptionEnabled = pEnableEncryption;

		if (this.mEncryptionEnabled) {
			if (pPassphrase == null) {
				mPassphrase = "default KETL";

				File fs = new File((Metadata.mKETLPath == null ? ""
						: Metadata.mKETLPath + File.separator)
						+ ".ketl_pass");

				if (fs.exists()) {
					FileReader inputFileReader = new FileReader(fs);
					int c;

					StringBuilder sb = new StringBuilder();
					while ((c = inputFileReader.read()) != -1) {
						sb.append((char) c);
					}

					if (sb.length() > 5) {
						mPassphrase = sb.toString();
					} else
						throw new Exception(
								"Pass phrase needs to be more than 5 characters");
				} else {
					FileWriter out = new FileWriter(fs);
					java.util.Date dt = new java.util.Date();
					mPassphrase = new DesEncrypter(mPassphrase).encrypt(dt
							.toString());
					out.append(mPassphrase);
					out.flush();
					out.close();

					ResourcePool
							.LogMessage(
									this,
									ResourcePool.ERROR_MESSAGE,
									"Default pass phrase has been created and placed in the file "
											+ fs.getAbsolutePath()
											+ ", replace this file with the correct pass phrase or make sure to share this file");
				}
				this.mPassphraseFilePath = fs.getAbsolutePath();
			} else {
				this.mPassphraseFilePath = "(N/A)";
				this.mPassphrase = pPassphrase;
			}
			this.mEncryptor = new DesEncrypter(mPassphrase);
		}

		// DateTimeFormatter = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
		// MaxActiveJobs = new Integer(5);

		this.oLock = new Object();
	}

	public void enableEncryption(boolean arg0) {
		this.mEncryptionEnabled = arg0;
	}

	public ETLLoad[] getLoads(java.util.Date pStartDate, int pLoadID)
			throws Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList loads = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT load_id, start_job_id, start_date, project_id, end_date, ignored_parents, failed, 0  FROM  "
							+ tablePrefix
							+ loadTableName() +" A where start_date >= coalesce(?,start_date) and load_id = coalesce(?,load_id) and load_id in (select load_id from "
							+ tablePrefix
							+ "JOB_LOG) union all "
							+ " SELECT load_id, start_job_id, start_date, project_id, end_date, ignored_parents, failed, 1  FROM  "
							+ tablePrefix
							+ loadTableName() +" A where start_date >= coalesce(?,start_date) and load_id = coalesce(?,load_id) and load_id not in (select load_id from "
							+ tablePrefix + "JOB_LOG)");

			if (pStartDate == null) {
				m_stmt.setNull(1, Types.TIMESTAMP);
			} else
				m_stmt.setTimestamp(1, new Timestamp(pStartDate.getTime()));

			if (pLoadID < 0) {
				m_stmt.setNull(2, Types.INTEGER);
			} else
				m_stmt.setInt(2, pLoadID);

			if (pStartDate == null) {
				m_stmt.setNull(3, Types.TIMESTAMP);
			} else
				m_stmt.setTimestamp(3, new Timestamp(pStartDate.getTime()));

			if (pLoadID < 0) {
				m_stmt.setNull(4, Types.INTEGER);
			} else
				m_stmt.setInt(4, pLoadID);

			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				ETLLoad load = new ETLLoad();

				load.LoadID = m_rs.getInt(1);
				load.start_job_id = m_rs.getString(2);
				load.start_date = m_rs.getTimestamp(3);
				load.project_id = m_rs.getInt(4);
				load.end_date = m_rs.getTimestamp(5);
				load.ignored_parents = m_rs.getString(6) == null ? false : true;
				load.failed = m_rs.getString(7) == null ? false : true;
				load.running = m_rs.getInt(8) == 0 ? true : false;
				loads.add(load);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		ETLLoad[] tmp = new ETLLoad[loads.size()];
		loads.toArray(tmp);
		return tmp;
	}

	/**
	 * @author dnguyen 2006-07-27
	 * @return Returns a list of all loads that contain this job.
	 * @param pStartDate
	 *            Date - return all rows updated since this date
	 * @param pJobName
	 *            String - the job in question
	 */
	public ETLLoad[] getJobLoads(java.util.Date pStartDate, String pJobName)
			throws Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList loads = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			// the job_id's are in the JOB_LOG and JOB_LOG_HIST tables; join to
			// LOAD table to get load info
			m_stmt = metadataConnection
					.prepareStatement("SELECT a.load_id, a.start_job_id, a.start_date as load_start_date, a.project_id, "
							+ "a.end_date as load_end_date, a.ignored_parents, a.failed, 0 as is_running, b.dm_load_id FROM  "
							+ tablePrefix
							+ loadTableName() + " a, "
							+ tablePrefix
							+ "JOB_LOG b "
							+ "where a.load_id=b.load_id and b.start_date >= coalesce(?,b.start_date) and job_id = coalesce(?,job_id) "
							+ " union all "
							+ "SELECT a.load_id, a.start_job_id, a.start_date as load_start_date, a.project_id, "
							+ "a.end_date as load_end_date, a.ignored_parents, a.failed, 1 as is_running, b.dm_load_id FROM  "
							+ tablePrefix
							+ loadTableName() + " a, "
							+ tablePrefix
							+ "JOB_LOG_HIST b "
							+ "where a.load_id=b.load_id and b.start_date >= coalesce(?,b.start_date) and job_id = coalesce(?,job_id)");

			if (pStartDate == null)
				m_stmt.setNull(1, Types.TIMESTAMP);
			else
				m_stmt.setTimestamp(1, new Timestamp(pStartDate.getTime()));

			m_stmt.setString(2, pJobName);

			if (pStartDate == null)
				m_stmt.setNull(3, Types.TIMESTAMP);
			else
				m_stmt.setTimestamp(3, new Timestamp(pStartDate.getTime()));

			m_stmt.setString(4, pJobName);

			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				ETLLoad load = new ETLLoad();

				load.LoadID = m_rs.getInt(1);
				load.start_job_id = m_rs.getString(2);
				load.start_date = m_rs.getTimestamp(3);
				load.project_id = m_rs.getInt(4);
				load.end_date = m_rs.getTimestamp(5);
				load.ignored_parents = m_rs.getString(6) == null ? false : true;
				load.failed = m_rs.getString(7) == null ? false : true;
				load.running = m_rs.getInt(8) == 0 ? true : false;
				load.jobExecutionID = m_rs.getInt(9);
				loads.add(load);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		ETLLoad[] tmp = new ETLLoad[loads.size()];
		loads.toArray(tmp);
		return tmp;
	}

	/**
	 * @modified dnguyen 2006-07-27 update getLoadJobs with pExecID to return a
	 *           single execution status/details
	 * @return the job execution details for one execution or the entire load
	 * @param pExecID
	 *            int - execution id
	 */
	public ETLJob[] getLoadJobs(java.util.Date pStartDate, int pLoadID)
			throws Exception {

		ETLJob[] tmp = getExecutionDetails(pStartDate, pLoadID, -1);

		this.populateJobDetails(tmp);

		for (int i = 0; i < tmp.length; i++) {
			ETLJob j = tmp[i];
			j.dependencies = this.getJobDependencies(j.getJobID());
		}

		return tmp;
	}

	public ETLJob[] getExecutionDetails(java.util.Date pStartDate, int pLoadID,
			int pExecID) throws Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList jobs = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			String sql = "SELECT  job_id,start_date,status_id,end_date,message,dm_load_id,retry_attempts,execution_date,server_id FROM  "
					+ tablePrefix
					+ "JOB_LOG A where start_date >= coalesce(?,start_date) and load_id = ?";
			if (pExecID > 0)
				sql += " and dm_load_id = ?";
			sql += " union all SELECT  job_id,start_date,status_id,end_date,message,dm_load_id,retry_attempts,execution_date,server_id FROM  "
					+ tablePrefix
					+ "JOB_LOG_HIST A where start_date >= coalesce(?,start_date) and load_id = ?";
			if (pExecID > 0)
				sql += " and dm_load_id = ?";

			m_stmt = metadataConnection.prepareStatement(sql);

			int iDate2, iLoad2;
			if (pExecID > 0) {
				m_stmt.setInt(3, pExecID);
				m_stmt.setInt(6, pExecID);
				iDate2 = 4;
				iLoad2 = 5;
			} else {
				iDate2 = 3;
				iLoad2 = 4;
			}
			if (pStartDate == null) {
				m_stmt.setNull(1, Types.TIMESTAMP);
				m_stmt.setNull(iDate2, Types.TIMESTAMP);
			} else {
				m_stmt.setTimestamp(1, new Timestamp(pStartDate.getTime()));
				m_stmt
						.setTimestamp(iDate2, new Timestamp(pStartDate
								.getTime()));
			}
			m_stmt.setInt(2, pLoadID);
			m_stmt.setInt(iLoad2, pLoadID);

			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				ETLJob job = new ETLJob();
				job.setLoadID(pLoadID);
				job.setJobID(m_rs.getString(1));
				job.getStatus().setStartDate(m_rs.getTimestamp(2));
				job.getStatus().setStatusCode(m_rs.getInt(3));
				job.getStatus().setEndDate(m_rs.getTimestamp(4));
				job.getStatus().setExtendedMessage(m_rs.getString(5));
				job.setJobExecutionID(m_rs.getInt(6));
				job.setRetryAttempts(m_rs.getInt(7));
				job.getStatus().setExecutionDate(m_rs.getTimestamp(8));
				job.getStatus().setServerID(m_rs.getInt(9));
				jobs.add(job);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}

			ETLJob[] tmp = new ETLJob[jobs.size()];
			jobs.toArray(tmp);

			return tmp;
		}
	}

	public ETLJob[] getProjectJobs(java.util.Date pStartDate, int pProjectID)
			throws Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList jobs = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT  job_id FROM  " + tablePrefix
							+ "JOB A where project_id = ?");

			m_stmt.setInt(1, pProjectID);

			m_rs = m_stmt.executeQuery();

			// cycle through jobs
			while (m_rs.next()) {
				ETLJob job = new ETLJob();
				job.setJobID(m_rs.getString(1));
				jobs.add(job);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}

			ETLJob[] tmp = new ETLJob[jobs.size()];
			jobs.toArray(tmp);
			this.populateJobDetails(tmp);

			for (int i = 0; i < tmp.length; i++) {
				ETLJob j = tmp[i];
				j.dependencies = this.getJobDependencies(j.getJobID());
			}

			return tmp;
		}
	}

	public boolean sendEmail(String pNew_job_id, String pSubject,
			String pMessage) throws SQLException, java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		SMTPClient client = new SMTPClient();

		try {
			int reply;

			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
					"Attempting to send email, list of recipients follows:");

			m_stmt = metadataConnection
					.prepareStatement("select disable_alerting,project_id,ACTION from "
							+ tablePrefix + "job where job_id = ?");
			m_stmt.setString(1, pNew_job_id);
			m_rs = m_stmt.executeQuery();

			String sAlertingDisabled = null;
			int iProjectID = -1;

			while (m_rs.next()) {
				sAlertingDisabled = m_rs.getString(1);
				iProjectID = m_rs.getInt(2);
			}

			if (m_stmt != null) {
				m_stmt.close();
			}

			if ((sAlertingDisabled == null)
					|| (sAlertingDisabled.compareTo("Y") != 0)) {
				m_stmt = metadataConnection
						.prepareStatement("select hostname,login,pwd,from_address	from  "
								+ tablePrefix + "mail_server_detail");
				m_rs = m_stmt.executeQuery();

				String sMailHost = null;
				String sFromAddress = null;

				// String sLogin;
				// String sPWD;
				while (m_rs.next()) {
					sMailHost = m_rs.getString(1);
					sFromAddress = m_rs.getString(4);
				}

				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
						"Using mail server: " + sMailHost);
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
						"From address: " + sFromAddress);

				if (m_stmt != null) {
					m_stmt.close();
				}

				try {
					client.connect(sMailHost);
				} catch (com.kni.util.net.smtp.SMTPConnectionClosedException e) {
					System.err.println("SMTP server refused connection.");

					return false;
				}

				// After connection attempt, you should check the reply code
				// to verify
				// success.
				reply = client.getReplyCode();

				if (!SMTPReply.isPositiveCompletion(reply)) {
					client.disconnect();
					System.err.println("SMTP server refused connection.");

					return false;
				}

				client.login();

				if (sMailHost != null) {

					// After connection attempt, you should check the reply code
					// to verify
					// success.
					m_stmt = metadataConnection
							.prepareStatement("SELECT ADDRESS,SUBJECT_PREFIX,MAX_MESSAGE_LENGTH,ADDRESS_NAME FROM  "
									+ tablePrefix
									+ "alert_subscription a,  "
									+ tablePrefix
									+ "alert_address b WHERE a.address_id = b.address_id AND (a.job_id = ? OR a.project_id = ? OR a.all_errors = 'Y')");
					m_stmt.setString(1, pNew_job_id);
					m_stmt.setInt(2, iProjectID);
					m_rs = m_stmt.executeQuery();

					String[] msgParts = new String[5];

					while (m_rs.next()) {
						try {
							// Create the email message

							int maxMsgLength = m_rs.getInt(3);

							if (maxMsgLength == 0) {
								maxMsgLength = 16096;
							}

							String toAddress = m_rs.getString(1);

							if (toAddress != null) {
								String[] parts = toAddress.split("@");
								if (parts.length > 2) {
									toAddress = parts[0] + "@" + parts[1];
								}
							}

							msgParts[0] = pMessage;

							client
									.sendSimpleMessage(
											sFromAddress,
											toAddress,
											"Content-Type: multipart/mixed; boundary=\"DataSeparatorString\"\r\nMIME-Version: 1.0\r\nSubject:"
													+ pSubject
													+ "\r\n"
													+ pMessage);

							ResourcePool.LogMessage(this,
									ResourcePool.INFO_MESSAGE, "Recipient: "
											+ toAddress);
						} catch (Exception e) {
							ResourcePool.LogMessage(this,
									ResourcePool.ERROR_MESSAGE,
									"Email send error " + e.toString());
							ResourcePool.LogException(e, this);

						}
					}

					client.logout();
					client.disconnect();

					if (m_stmt != null) {
						m_stmt.close();

					}
				}
			}

			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
					"Emails sent");

			// Do useful stuff here.
		} catch (Exception e) {
			ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
					"Could not connect to SMTP server.");
			ResourcePool.LogException(e, this);

			return false;
		}

		return true;
	}

	public boolean sendErrorEmail(String pNew_job_id, String pNew_CODE,
			String pNew_MESSAGE, String pNew_EXTENDED_MESSAGE,
			java.util.Date pNew_ERROR_DATETIME, int pNew_DM_LOAD_ID,
			String pAttachment) throws SQLException, java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		SMTPClient client = new SMTPClient();

		try {
			int reply;

			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
					"Attempting to send email, list of recipients follows:");

			m_stmt = metadataConnection
					.prepareStatement("select disable_alerting,project_id,ACTION from "
							+ tablePrefix + "job where job_id = ?");
			m_stmt.setString(1, pNew_job_id);
			m_rs = m_stmt.executeQuery();

			String sAlertingDisabled = null;
			int iProjectID = -1;

			while (m_rs.next()) {
				sAlertingDisabled = m_rs.getString(1);
				iProjectID = m_rs.getInt(2);
			}

			if (m_stmt != null) {
				m_stmt.close();
			}

			if ((sAlertingDisabled == null)
					|| (sAlertingDisabled.compareTo("Y") != 0)) {
				m_stmt = metadataConnection
						.prepareStatement("select hostname,login,pwd,from_address	from  "
								+ tablePrefix + "mail_server_detail");
				m_rs = m_stmt.executeQuery();

				String sMailHost = null;
				String sFromAddress = null;

				// String sLogin;
				// String sPWD;
				while (m_rs.next()) {
					sMailHost = m_rs.getString(1);
					sFromAddress = m_rs.getString(4);
				}

                if(sMailHost == null){
                    return false;
                }
                
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
						"Using mail server: " + sMailHost);
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
						"From address: " + sFromAddress);

				if (m_stmt != null) {
					m_stmt.close();
				}

				try {
					client.connect(sMailHost);
				} catch (com.kni.util.net.smtp.SMTPConnectionClosedException e) {
					System.err.println("SMTP server refused connection.");

					return false;
				}

				// After connection attempt, you should check the reply code
				// to verify
				// success.
				reply = client.getReplyCode();

				if (!SMTPReply.isPositiveCompletion(reply)) {
					client.disconnect();
					System.err.println("SMTP server refused connection.");

					return false;
				}

				client.login();

				if (sMailHost != null) {

					// After connection attempt, you should check the reply code
					// to verify
					// success.
					m_stmt = metadataConnection
							.prepareStatement("SELECT ADDRESS,SUBJECT_PREFIX,MAX_MESSAGE_LENGTH,ADDRESS_NAME FROM  "
									+ tablePrefix
									+ "alert_subscription a,  "
									+ tablePrefix
									+ "alert_address b WHERE a.address_id = b.address_id AND (a.job_id = ? OR a.project_id = ? OR a.all_errors = 'Y')");
					m_stmt.setString(1, pNew_job_id);
					m_stmt.setInt(2, iProjectID);
					m_rs = m_stmt.executeQuery();

					String[] msgParts = new String[5];

					while (m_rs.next()) {
						try {
							// Create the email message

							String subjPrefix = m_rs.getString(2);

							int maxMsgLength = m_rs.getInt(3);

							if (maxMsgLength == 0) {
								maxMsgLength = 16096;
							}

							if ((subjPrefix == null)
									|| (subjPrefix.length() <= 1)) {
								subjPrefix = "";
							} else {
								subjPrefix = subjPrefix + " - ";
							}

							String toAddress = m_rs.getString(1);
							boolean plainTxt = false;

							if (toAddress != null) {
								String[] parts = toAddress.split("@");
								if (parts.length > 2) {
									if (parts[2] != null
											&& parts[2].equals("HTML"))
										plainTxt = false;

									toAddress = parts[0] + "@" + parts[1];
								} else
									plainTxt = true;
							}

							if (plainTxt) {
								msgParts[0] = "Error Message:\t" + pNew_MESSAGE
										+ "\n\n";
								msgParts[1] = "Job ID:\t\t" + pNew_job_id
										+ "\n\n";
								msgParts[2] = "Error Date Time:\t"
										+ pNew_ERROR_DATETIME + "\n\n";
								msgParts[3] = "Error Code:\t\t" + pNew_CODE
										+ "\n\n";
								msgParts[4] = "Extended Message:\t"
										+ pNew_EXTENDED_MESSAGE + "\n\n";
							} else {
								msgParts[0] = pNew_MESSAGE;
								msgParts[1] = pNew_job_id;
								msgParts[2] = pNew_ERROR_DATETIME.toString();
								msgParts[3] = pNew_CODE;
								msgParts[4] = pNew_EXTENDED_MESSAGE;
							}

							client
									.sendSimpleMessage(
											sFromAddress,
											toAddress,
											"Content-Type: multipart/mixed; boundary=\"DataSeparatorString\"\r\nMIME-Version: 1.0\r\nSubject:"
													+ subjPrefix
													+ "Job Error ("
													+ pNew_job_id
													+ ")\r\n"
													+ (plainTxt ? this
															.getNewTextEmail(
																	msgParts,
																	maxMsgLength)
															: getNewHTMLEmail(msgParts))
													+ (pAttachment != null
															&& new File(
																	pAttachment)
																	.exists() ? "\r\n--DataSeparatorString\r\nContent-Disposition: attachment;filename=\"trace.log\"\r\nContent-transfer-encoding: base64\r\n\r\n"
															+ EncodeBase64
																	.encode(pAttachment)
															+ "\r\n\r\n"
															: "")
													+ "\r\n--DataSeparatorString--");

							ResourcePool.LogMessage(this,
									ResourcePool.INFO_MESSAGE, "Recipient: "
											+ toAddress);
						} catch (Exception e) {
							ResourcePool.LogMessage(this,
									ResourcePool.ERROR_MESSAGE,
									"Email send error " + e.toString());
							ResourcePool.LogException(e, this);

						}
					}

					client.logout();
					client.disconnect();

					if (m_stmt != null) {
						m_stmt.close();

					}
				}
			}

			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
					"Emails sent");

			// Do useful stuff here.
		} catch (Exception e) {
			ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
					"Could not connect to SMTP server.");
			ResourcePool.LogException(e, this);

			return false;
		}

		return true;
	}

	private String getNewHTMLEmail(String[] msgParts) {
		return getMessageAsHTML(msgParts);

	}

	private String writeHTMLRow(String field, String value) {
		return "<tr><td width=\"9%\" class=\"row\">" + field
				+ "</td><td width=\"90%\" class=\"row\">" + value
				+ "</td></tr>";
	}

	private String getNewTextEmail(String[] msgParts, int maxMsgLength) {

		String msg = "--DataSeparatorString\r\nContent-Type: text/plain; charset=\"us-ascii\"\n";
		int msgPartIdx = 0;

		while ((msg.length() < maxMsgLength) & (msgPartIdx < 5)) {
			if (msgParts[msgPartIdx] != null) {
				msg = msg + msgParts[msgPartIdx];
			}

			msgPartIdx++;
		}

		if (msg.length() > maxMsgLength) {
			msg = msg.substring(0, maxMsgLength - 2) + "..";
		}

		return msg;
	}

	private String getMessageAsHTML(String[] msgParts) {

		StringBuilder sb = new StringBuilder();
		sb
				.append("--DataSeparatorString\r\nContent-Type: text/html; charset=\"us-ascii\"\r\n\r\n<html><head><meta http-equiv=\"Content-Language\" content=\"en-us\"><meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1252\"><style>\n<!--\n.tbl { border-left-style: none; border-right-style: none;border-top: 1.5pt solid green; border-bottom: 1.5pt solid green }\n.ms-simple1-tl { border-left-style: none; border-right-style: none; border-top-style: none;border-bottom: .75pt solid green }\n.row {  border-left-style: none; border-right-style: none; border-top-style: none;border-bottom: .75pt solid green;font-size: 9pt }--></style></head><body><font face=\"Arial\"><b>KETL Error</b><table border=\"0\" width=\"100%\" id=\"table1\" class=\"tbl\">\r\n");

		sb.append(writeHTMLRow("Error Message", msgParts[0]));
		sb.append(writeHTMLRow("Job ID", msgParts[1]));
		sb.append(writeHTMLRow("Datetime", msgParts[2]));
		sb.append(writeHTMLRow("Error Code", msgParts[3]));
		sb.append(writeHTMLRow("Extended Message", msgParts[4] == null ? "NULL"
				: msgParts[4].replace("\n", "<br/>")));

		return sb.toString();
	}

	/**
	 * Insert the method's description here. Creation date: (9/9/2002 11:23:23
	 * AM)
	 * 
	 * @param pJobDependencies
	 *            java.lang.String[][]
	 * @param pJobID
	 *            java.lang.String
	 */
	private static String getChildJobs(String[][] pJobDependencies,
			String pJobID, ArrayList pFinalJobList) {
		int i = 0;
		String bHasChildren = "N";

		if (pJobDependencies == null) {
			return bHasChildren;
		}

		// sJobDependencies holds all dependencies for project
		// recursively cycle through deps now
		for (i = 0; i < pJobDependencies.length; i++) {
			if ((pJobDependencies[i][1] != null)
					&& (pJobDependencies[i][1].compareTo(pJobID) == 0)) {
				bHasChildren = "Y";
				pJobDependencies[i][1] = null;
				getChildJobs(pJobDependencies, pJobDependencies[i][0],
						pFinalJobList);
			}
		}

		if (pFinalJobList == null) {
			pFinalJobList = new ArrayList();
		}

		boolean bAlreadyExists = false;

		for (i = 0; i < pFinalJobList.size(); i++) {
			String[] sJobID = (String[]) pFinalJobList.get(i);

			if (sJobID != null) {
				if (sJobID[0].compareTo(pJobID) == 0) {
					bAlreadyExists = true;
					i = pFinalJobList.size();
				}
			}
		}

		if (bAlreadyExists == false) {
			String[] sJob = new String[2];
			sJob[0] = pJobID;
			sJob[1] = bHasChildren;

			pFinalJobList.add(sJob);
		}

		return (bHasChildren);
	}

	/**
	 * Insert the method's description here. Creation date: (3/8/2002 11:47:27
	 * AM)
	 * 
	 * @return java.sql.Date
	 * @param pMonth
	 *            int
	 * @param pMonthOfYear
	 *            int
	 * @param pDay
	 *            int
	 * @param pDayOfWeek
	 *            int
	 * @param pDayOfMonth
	 *            int
	 * @param pHour
	 *            int
	 * @param pHourOfDay
	 *            int
	 */
	protected static java.util.Date getNextDate(java.util.Date pCurrentDate,
			int pMonth, int pMonthOfYear, int pDay, int pDayOfWeek,
			int pDayOfMonth, int pHour, int pHourOfDay, int pMinute,
			int pMinuteOfHour) {
		Calendar cal = Calendar.getInstance();

		cal.setTime(pCurrentDate);

		// set month of year
		if (pMonthOfYear != -1) {
			cal.set(Calendar.MONTH, pMonthOfYear);
		}

		// set day of month
		if (pDayOfMonth != -1) {
			cal.set(Calendar.DAY_OF_MONTH, pDayOfMonth);
		}

		// set day of week
		if ((pDayOfWeek >= 1) && (pDayOfWeek <= 7)) {
			int DayOfWeek = Calendar.MONDAY;

			switch (pDayOfWeek) {
			case 1:
				DayOfWeek = Calendar.MONDAY;

				break;

			case 2:
				DayOfWeek = Calendar.TUESDAY;

				break;

			case 3:
				DayOfWeek = Calendar.WEDNESDAY;

				break;

			case 4:
				DayOfWeek = Calendar.THURSDAY;

				break;

			case 5:
				DayOfWeek = Calendar.FRIDAY;

				break;

			case 6:
				DayOfWeek = Calendar.SATURDAY;

				break;

			case 7:
				DayOfWeek = Calendar.SUNDAY;
			}

			cal.set(Calendar.DAY_OF_WEEK, DayOfWeek);
		}

		// set hour of day
		if ((pHourOfDay >= 0) && (pHourOfDay <= 23)) {
			cal.set(Calendar.HOUR_OF_DAY, pHourOfDay);
		}

		// set hour of day
		if ((pMinuteOfHour >= 0) && (pMinuteOfHour <= 59)) {
			cal.set(Calendar.MINUTE, pMinuteOfHour);
		}

		// these adjust month,day and hour plus or minus from set value
		// adjust month by pMonth
		if (pMonth != -1) {
			cal.add(Calendar.MONTH, pMonth);
		}

		// adjust day by pDays days
		if (pDay != -1) {
			cal.add(Calendar.DATE, pDay);
		}

		// adjust hour by pHour
		if (pHour != -1) {
			cal.add(Calendar.HOUR_OF_DAY, pHour);
		}

		// adjust minute by pMinute
		if (pMinute != -1) {
			cal.add(Calendar.MINUTE, pMinute);
		}

		return cal.getTime();
	}

	/**
	 * Insert the method's description here. Creation date: (5/1/2002 8:03:36
	 * PM)
	 */
	public void closeMetadata() {
		synchronized (this.oLock) {
			if (metadataConnection == null) {
				return;
			}

			try {
                if(mIncIdentColStmt != null){
                    try{
                        mIncIdentColStmt.close();
                    }catch(Exception e){
                        System.out.println(e);
                    }
                }
                
				metadataConnection.rollback();
				metadataConnection.close();
				metadataConnection = null;
			} catch (SQLException ee) {
				metadataConnection = null;
				System.out.println(ee);
			}
		}
	}

	/**
	 * Insert the method's description here. Creation date: (6/6/2002 9:29:56
	 * PM)
	 * 
	 * @return boolean
	 * @param pJobName
	 *            java.lang.String
	 * @param pIgnoreDependencies
	 *            boolean
	 */
	public boolean executeJob(int pProjectID, String pJobID,
			boolean pIgnoreDependencies) throws SQLException,
			java.lang.Exception {
		return executeJob(pProjectID, pJobID, pIgnoreDependencies, false);
	}

	public KETLCluster getClusterDetails() throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;

		KETLCluster kc = new KETLCluster();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("select a.server_id,server_name,status_desc,last_ping_time,start_time,c.description,threads, "
							+ currentTimeStampSyntax
							+ " from "
							+ tablePrefix
							+ "server_executor a, "
							+ tablePrefix
							+ "job_executor_job_type b, "
							+ tablePrefix
							+ "job_type c, "
							+ tablePrefix
							+ "server d, "
							+ tablePrefix
							+ "server_status e "
							+ " where a.job_executor_id = b.job_executor_id "
							+ " and b.job_type_id = c.job_type_id "
							+ "  and d.status_id = e.status_id "
							+ "  and a.server_id = d.server_id "
							+ " order by server_name,c.description  ");
			m_rs = m_stmt.executeQuery();

			while (m_rs.next()) {
				try {
					int serverID = m_rs.getInt(1);
					kc.addServer(serverID, m_rs.getString(2), m_rs
							.getTimestamp(5), m_rs.getTimestamp(4), m_rs
							.getString(3), m_rs.getTimestamp(8));
					kc.addExecutor(serverID, m_rs.getString(6), m_rs.getInt(7));
				} catch (Exception e) {
					e.printStackTrace();
					ResourcePool.LogMessage("Error getting server states: "
							+ e.getMessage());

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}

			m_stmt = metadataConnection
					.prepareStatement("select server_id,c.description,status_desc,count(*) "
							+ " from "
							+ tablePrefix
							+ "job_log a , "
							+ tablePrefix
							+ "job_status b,  "
							+ " "
							+ tablePrefix
							+ "job_type c, "
							+ tablePrefix
							+ "job d "
							+ " where a.status_id = b.status_id "
							+ " and a.job_id = d.job_id "
							+ " and c.job_type_id = d.job_type_id "
							+ " group by server_id,status_desc,c.description");
			m_rs = m_stmt.executeQuery();

			while (m_rs.next()) {
				try {
					int serverId = m_rs.getInt(1);

					if (m_rs.wasNull()) {
						serverId = -1;
					}

					kc.addExecutorState(serverId, m_rs.getString(2), m_rs
							.getString(3), m_rs.getInt(4));
				} catch (Exception e) {
					ResourcePool.LogMessage("Error getting executor state: "
							+ e.getMessage());
					e.printStackTrace();

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return kc;
	}

	/**
	 * Insert the method's description here. Creation date: (6/6/2002 9:29:56
	 * PM)
	 * 
	 * @return boolean
	 * @param pJobName
	 *            java.lang.String
	 * @param pIgnoreDependencies
	 *            boolean
	 * @param pAllowMultiple
	 *            boolean
	 */
	public boolean executeJob(int pProjectID, String pJobID,
			boolean pIgnoreDependencies, boolean pAllowMultiple)
			throws SQLException, java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet rs = null;
		ResultSet m_rs = null;

		synchronized (this.oLock) {

			ETLJob j = this.getJob(pJobID);

			if (j == null || j.getProjectID() != pProjectID) {
				throw new Exception("Job " + pJobID
						+ " does not exist or does not exist in project "
						+ pProjectID);
			}

			ETLJobStatus etlJobStatus = new ETLJobStatus();

			// Make metadata connection alive.
			refreshMetadataConnection();

			if (pAllowMultiple == false) {
				m_stmt = metadataConnection
						.prepareStatement("SELECT COUNT(*) FROM  "
								+ tablePrefix + "JOB_LOG WHERE JOB_ID = ?");
				m_stmt.setString(1, pJobID);
				m_rs = m_stmt.executeQuery();

				int occurences = 0;

				while (m_rs.next()) {
					occurences = m_rs.getInt(1);
				}

				if (m_stmt != null) {
					m_stmt.close();
				}

				if (occurences > 0) {
					ResourcePool
							.LogMessage(
									this,
									ResourcePool.ERROR_MESSAGE,
									1,
									"Job "
											+ pJobID
											+ " already in job queue, multiple executions of job disallowed.",
									"To resolve this problem closeout the currently blocking job, "
											+ "blocking job maybe a prior load failure.",
									Thread.currentThread().getName()
											.equalsIgnoreCase("ETLDaemon") ? true
											: false);

					return false;
				}

			}

			// get next load_id
            m_stmt = metadataConnection.prepareStatement(this.useIdentityColumn?this.nextLoadIDSyntax:("SELECT "
					+ nextLoadIDSyntax + singleRowPullSyntax + ""));
			m_rs = m_stmt.executeQuery();

            
			String jobID = pJobID;
			int loadID;

			PreparedStatement newLoadStmt = metadataConnection
					.prepareStatement("INSERT INTO  "
							+ tablePrefix
							+ loadTableName() + "(LOAD_ID,START_JOB_ID,START_DATE,PROJECT_ID) VALUES(?,?,"
							+ currentTimeStampSyntax + ",?)");
			PreparedStatement dependenciesStmt = metadataConnection
					.prepareStatement("SELECT JOB_ID, PARENT_JOB_ID  FROM  "
							+ tablePrefix
							+ "JOB_DEPENDENCIE A WHERE JOB_ID IN (SELECT JOB_ID FROM  "
							+ tablePrefix
							+ "JOB WHERE PROJECT_ID = ? )  AND PARENT_JOB_ID IN (SELECT JOB_ID FROM  "
							+ tablePrefix + "JOB  WHERE PROJECT_ID = ?)");

			PreparedStatement insJobsStmt = null;
			PreparedStatement insNoDepJobStmt = null;

			// cycle through pending jobs to be queued
			while (m_rs.next()) {
				loadID = m_rs.getInt(1);

				// create a loadid
				newLoadStmt.setInt(1, loadID);
				newLoadStmt.setString(2, pJobID);
				newLoadStmt.setInt(3, pProjectID);

				newLoadStmt.executeUpdate();

				String[][] sJobDependencies = null;

				if (pIgnoreDependencies == false) {
					dependenciesStmt.setInt(1, pProjectID);
					dependenciesStmt.setInt(2, pProjectID);
					rs = dependenciesStmt.executeQuery();

					int i = 0;

					while (rs.next()) {
						if (sJobDependencies == null) {
							sJobDependencies = new String[i + 1][2];
							sJobDependencies[i][0] = rs.getString(1);
							sJobDependencies[i][1] = rs.getString(2);
						} else {
							i++;

							String[][] tmp = new String[i + 1][2];
							tmp[i][0] = rs.getString(1);
							tmp[i][1] = rs.getString(2);

							System.arraycopy(sJobDependencies, 0, tmp, 0,
									sJobDependencies.length);
							sJobDependencies = tmp;
						}
					}

					// sJobDependencies holds all dependencies for project
					// recursively cycle through deps now
					ArrayList aFinalJobList = new ArrayList();

					getChildJobs(sJobDependencies, pJobID, aFinalJobList);

					int iStatus;
					String sStatusMessage;

					if (insJobsStmt == null) {
						insJobsStmt = metadataConnection
								.prepareStatement("INSERT INTO  "
										+ tablePrefix
										+ "JOB_LOG(JOB_ID,LOAD_ID,STATUS_ID,START_DATE,MESSAGE,DM_LOAD_ID) SELECT JOB_ID,?,?,"
										+ currentTimeStampSyntax + ",?,"
										+ (this.useIdentityColumn?"null":nextLoadIDSyntax) + " FROM  "
										+ tablePrefix + "job where job_id = ?");
					}

					for (i = 0; i < aFinalJobList.size(); i++) {
						String[] sJobID = (String[]) aFinalJobList.get(i);

						if (sJobID != null) {
							if (sJobID[1].compareTo("Y") == 0) {
								iStatus = ETLJobStatus.WAITING_FOR_CHILDREN;
								sStatusMessage = etlJobStatus
										.getStatusMessageForCode(ETLJobStatus.WAITING_FOR_CHILDREN);
							} else {
								iStatus = ETLJobStatus.READY_TO_RUN;
								sStatusMessage = etlJobStatus
										.getStatusMessageForCode(ETLJobStatus.READY_TO_RUN);
							}

							insJobsStmt.setInt(1, loadID);
							insJobsStmt.setInt(2, iStatus);
							insJobsStmt.setString(3, sStatusMessage);
							insJobsStmt.setString(4, sJobID[0]);

							insJobsStmt.addBatch();
						}
					}

					insJobsStmt.executeBatch();
				}

				// if job didn't have any children it will not of been added so
				// must add now
				if (insNoDepJobStmt == null) {
					insNoDepJobStmt = metadataConnection
							.prepareStatement("INSERT INTO  "
									+ tablePrefix
									+ "JOB_LOG(JOB_ID,LOAD_ID,STATUS_ID,START_DATE,MESSAGE,DM_LOAD_ID) SELECT JOB_ID,?,?,"
									+ currentTimeStampSyntax
									+ ",?, "
									+ (this.useIdentityColumn?"null":nextLoadIDSyntax)
									+ "  FROM  "
									+ tablePrefix
									+ "JOB where not exists (select 1 from  "
									+ tablePrefix
									+ "job_log where load_id = ? AND job_id = ?) AND JOB_ID = ?");
				}

				insNoDepJobStmt.setInt(1, loadID);
				insNoDepJobStmt.setInt(2, ETLJobStatus.READY_TO_RUN);
				insNoDepJobStmt.setString(3, etlJobStatus
						.getStatusMessageForCode(ETLJobStatus.READY_TO_RUN));
				insNoDepJobStmt.setInt(4, loadID);
				insNoDepJobStmt.setString(5, jobID);
				insNoDepJobStmt.setString(6, jobID);

				insNoDepJobStmt.execute();

				this.metadataConnection.commit();
			}

			if (newLoadStmt != null) {
				newLoadStmt.close();
			}

			if (dependenciesStmt != null) {
				dependenciesStmt.close();
			}

			if (insJobsStmt != null) {
				insJobsStmt.close();
			}

			if (insNoDepJobStmt != null) {
				insNoDepJobStmt.close();
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return true;
	}

	public ETLJob getJob(String pJobID) throws SQLException,
			java.lang.Exception {
		return getJob(pJobID, 0, 0);
	}

	public ETLJob[] populateJobDetails(ETLJob[] pJobs) throws SQLException,
			java.lang.Exception {
		ETLJob newETLJob = null;
		Statement m_stmt = null;
		ResultSet m_rs = null;

		synchronized (this.oLock) {

			int i = 0;
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection.createStatement();
			while (i < pJobs.length) {

				StringBuilder sb = new StringBuilder("");
				int cnt = 0;
				HashMap hmJobs = new HashMap();

				for (; i < pJobs.length && cnt++ < 50; i++) {
					if (sb.length() > 0)
						sb.append(",");

					sb.append("'" + pJobs[i].sJobID + "'");
					hmJobs.put(pJobs[i].getJobID(), i);
				}

				m_rs = m_stmt
						.executeQuery("SELECT CLASS_NAME,PROJECT_ID,JOB_ID,ACTION,NAME,RETRY_ATTEMPTS,PARAMETER_LIST_ID,"
								+ "A.JOB_TYPE_ID,a.DESCRIPTION,b.DESCRIPTION,DISABLE_ALERTING,SECONDS_BEFORE_RETRY FROM  "
								+ tablePrefix
								+ "JOB A,  "
								+ tablePrefix
								+ "JOB_TYPE B WHERE A.JOB_TYPE_ID = B.JOB_TYPE_ID AND JOB_ID in ("
								+ sb.toString() + ")");

				// cycle through pending jobs setting next run date
				while (m_rs.next()) {
					try {
						ETLJob curETLJob = pJobs[(Integer) hmJobs.get(m_rs
								.getString(3))];
						// if class was null then it is a default job and should
						// just be passed
						String jobClass = m_rs.getString(1);

						if (m_rs.wasNull() == true) {
							newETLJob = new ETLJob();
						} else {
							newETLJob = (ETLJob) Class.forName(jobClass)
									.newInstance();
						}

						pJobs[(Integer) hmJobs.get(m_rs.getString(3))] = newETLJob;

						// transfer data
						newETLJob.setJobExecutionID(curETLJob
								.getJobExecutionID());
						newETLJob.setLoadID(curETLJob.iLoadID);
						newETLJob.jsStatus = curETLJob.jsStatus;
						newETLJob
								.setRetryAttempts(curETLJob.getRetryAttempts());

						// get action
						newETLJob.setAction(m_rs.getString(4));

						// get job id
						newETLJob.setJobID(m_rs.getString(3));

						// get job name
						newETLJob.setName(m_rs.getString(5));

						// get job description
						newETLJob.setDescription(m_rs.getString(9));

						// get job type description
						newETLJob.setJobTypeName(m_rs.getString(10));

						// get max retries
						newETLJob.setMaxRetries(m_rs.getInt(6));

						// get job type id
						newETLJob.setJobTypeID(m_rs.getInt(8));

						// get project id
						newETLJob.setProjectID(m_rs.getInt(2));

						// get alerting disabled or not
						String x = m_rs.getString(11);

						// get seconds before retry
						newETLJob.setSecondsBeforeRetry(m_rs.getInt(12));

						if ((x != null) && x.equalsIgnoreCase("Y")) {
							newETLJob.setDisableAlerting(true);
						}

						// get parameter list id
						// iParameterListID = m_rs.getInt(7);
						// if (m_rs.wasNull() == false) {
						// newETLJob.setGlobalParameterListID(iParameterListID);
						// }

					} catch (Exception e) {
						System.out.println("Error creating job: "
								+ e.getMessage());
						e.printStackTrace();

						return null;
					}
				}

				// Close open resources
				if (m_rs != null) {
					m_rs.close();
				}
			}
			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return pJobs;
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 2:01:06
	 * PM)
	 * 
	 * @return com.kni.etl.ETLJob
	 * @param pETLJob
	 *            com.kni.etl.ETLJob Revision History: 2003.02.14 (B. Sullivan) -
	 *            Added extraction of PARAMETER_LIST_ID so we can better
	 *            encapsulate the extraction of recursive parameter lists...
	 */
	public ETLJob getJob(String pJobID, int pLoadID, int pExecutionID)
			throws SQLException, java.lang.Exception {
		ETLJob newETLJob = null;
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		int iParameterListID = 0;

		System.out.println(pJobID);

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT CLASS_NAME,PROJECT_ID,JOB_ID,ACTION,NAME,RETRY_ATTEMPTS,PARAMETER_LIST_ID,"
							+ "A.JOB_TYPE_ID,a.DESCRIPTION,b.DESCRIPTION,DISABLE_ALERTING,SECONDS_BEFORE_RETRY FROM  "
							+ tablePrefix
							+ "JOB A,  "
							+ tablePrefix
							+ "JOB_TYPE B WHERE A.JOB_TYPE_ID = B.JOB_TYPE_ID AND JOB_ID = ?");
			m_stmt.setString(1, pJobID);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				try {
					// if class was null then it is a default job and should
					// just be passed
					String jobClass = m_rs.getString(1);

					if (m_rs.wasNull() == true) {
						newETLJob = new ETLJob();
					} else {
						newETLJob = (ETLJob) Class.forName(jobClass)
								.newInstance();
					}

					// set job ids
					newETLJob.setJobExecutionID(pExecutionID);
					newETLJob.setLoadID(pLoadID);

					// get action
					newETLJob.setAction(m_rs.getString(4));

					// get job id
					newETLJob.setJobID(m_rs.getString(3));

					// get job name
					newETLJob.setName(m_rs.getString(5));

					// get job description
					newETLJob.setDescription(m_rs.getString(9));

					// get job type description
					newETLJob.setJobTypeName(m_rs.getString(10));

					// get max retries
					newETLJob.setMaxRetries(m_rs.getInt(6));

					// get job type id
					newETLJob.setJobTypeID(m_rs.getInt(8));

					// get project id
					newETLJob.setProjectID(m_rs.getInt(2));

					// get alerting disabled or not
					String x = m_rs.getString(11);

					// get seconds before retry
					newETLJob.setSecondsBeforeRetry(m_rs.getInt(12));

					if ((x != null) && x.equalsIgnoreCase("Y")) {
						newETLJob.setDisableAlerting(true);
					}

					// get parameter list id
					iParameterListID = m_rs.getInt(7);

					if (m_rs.wasNull() == false) {
						newETLJob.setGlobalParameterListID(iParameterListID);
					}
				} catch (Exception e) {
					System.out.println("Error creating job: " + e.getMessage());
					e.printStackTrace();

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return (newETLJob);
	}

	public String getMetadataVersion() {
		return "1.0";
	}

	public void getJobStatus(ETLJob pJob) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT STATUS_ID,DM_LOAD_ID,LOAD_ID FROM  "
							+ tablePrefix
							+ "JOB_LOG A "
							+ "WHERE JOB_ID like ?");
			m_stmt.setString(1, pJob.getJobID());
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				pJob.getStatus().setStatusCode(m_rs.getInt(1));
				pJob.setJobExecutionID(m_rs.getInt(2));
				pJob.setLoadID(m_rs.getInt(3));
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}
	}

	public ETLJob getDetailedJobStatus(String pJobID, int pLoadID,
			int pExecutionID) throws SQLException, java.lang.Exception {
		ETLJob newETLJob = null;
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		int iParameterListID = 0;

		System.out.println(pJobID);

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT CLASS_NAME,PROJECT_ID,JOB_ID,ACTION,NAME,RETRY_ATTEMPTS,PARAMETER_LIST_ID,"
							+ "A.JOB_TYPE_ID,a.DESCRIPTION,b.DESCRIPTION,DISABLE_ALERTING,SECONDS_BEFORE_RETRY FROM  "
							+ tablePrefix
							+ "JOB A,  "
							+ tablePrefix
							+ "JOB_TYPE B WHERE A.JOB_TYPE_ID = B.JOB_TYPE_ID AND JOB_ID = ?");
			m_stmt.setString(1, pJobID);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				try {
					// if class was null then it is a default job and should
					// just be passed
					String jobClass = m_rs.getString(1);

					if (m_rs.wasNull() == true) {
						newETLJob = new ETLJob();
					} else {
						newETLJob = (ETLJob) Class.forName(jobClass)
								.newInstance();
					}

					// set job ids
					newETLJob.setJobExecutionID(pExecutionID);
					newETLJob.setLoadID(pLoadID);

					// get action
					newETLJob.setAction(m_rs.getString(4));

					// get job id
					newETLJob.setJobID(m_rs.getString(3));

					// get job name
					newETLJob.setName(m_rs.getString(5));

					// get job description
					newETLJob.setDescription(m_rs.getString(9));

					// get job type description
					newETLJob.setJobTypeName(m_rs.getString(10));

					// get max retries
					newETLJob.setMaxRetries(m_rs.getInt(6));

					// get job type id
					newETLJob.setJobTypeID(m_rs.getInt(8));

					// get project id
					newETLJob.setProjectID(m_rs.getInt(2));

					// get alerting disabled or not
					String x = m_rs.getString(11);

					// get seconds before retry
					newETLJob.setSecondsBeforeRetry(m_rs.getInt(12));

					if ((x != null) && x.equalsIgnoreCase("Y")) {
						newETLJob.setDisableAlerting(true);
					}

					// get parameter list id
					iParameterListID = m_rs.getInt(7);

					if (m_rs.wasNull() == false) {
						newETLJob.setGlobalParameterListID(iParameterListID);
					}
				} catch (Exception e) {
					System.out.println("Error creating job: " + e.getMessage());
					e.printStackTrace();

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return (newETLJob);

	}

	public ETLJob[] getJobDetails(String pJobID) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList jobsToFetch = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection.prepareStatement("SELECT JOB_ID FROM  "
					+ tablePrefix + "JOB A " + "WHERE JOB_ID like ?");
			m_stmt.setString(1, pJobID);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				try {
					jobsToFetch.add(m_rs.getString(1));
				} catch (Exception e) {
					System.out.println("Error creating job: " + e.getMessage());

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		ETLJob[] jobs = new ETLJob[jobsToFetch.size()];

		for (int i = 0; i < jobs.length; i++) {
			jobs[i] = getJob((String) jobsToFetch.get(i));
		}

		return (jobs);
	}

	public Object[][] getJobsByStatus(int pStatus) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList jobsToFetch = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("select a.job_id,d.description,start_date,end_date,b.server_name,message "
							+ " from "
							+ tablePrefix
							+ "job_log a, "
							+ tablePrefix
							+ "server b, "
							+ tablePrefix
							+ "job_type d,"
							+ tablePrefix
							+ "job e "
							+ " where a.status_id = ? "
							+ " and a.job_id = e.job_id "
							+ " and d.job_type_id = e.job_type_id "
							+ " and a.server_id = b.server_id ORDER by a.job_id");
			m_stmt.setInt(1, pStatus);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				try {
					Object[] s = new Object[6];
					s[0] = m_rs.getString(1);
					s[1] = m_rs.getString(2);
					s[2] = m_rs.getTimestamp(3);
					s[3] = m_rs.getTimestamp(4);
					s[4] = m_rs.getString(5);
					s[5] = m_rs.getString(6);
					jobsToFetch.add(s);
				} catch (Exception e) {
					System.out.println("Error creating job: " + e.getMessage());

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}

			m_stmt = metadataConnection
					.prepareStatement("select a.job_id,d.description,start_date,end_date,message "
							+ " from "
							+ tablePrefix
							+ "job_log a,  "
							+ tablePrefix
							+ "job_type d,"
							+ tablePrefix
							+ "job e "
							+ " where a.status_id = ? "
							+ " and a.job_id = e.job_id "
							+ " and d.job_type_id = e.job_type_id and a.server_id is null"
							+ " ORDER by a.job_id");
			m_stmt.setInt(1, pStatus);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				try {
					Object[] s = new Object[6];
					s[0] = m_rs.getString(1);
					s[1] = m_rs.getString(2);
					s[2] = m_rs.getTimestamp(3);
					s[3] = m_rs.getTimestamp(4);
					s[4] = "Not assigned";
					s[5] = m_rs.getString(5);
					jobsToFetch.add(s);
				} catch (Exception e) {
					System.out.println("Error creating job: " + e.getMessage());

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		Object[][] jobs = new Object[jobsToFetch.size()][];

		for (int i = 0; i < jobs.length; i++) {
			jobs[i] = (Object[]) jobsToFetch.get(i);
		}

		return (jobs);
	}

	public static final String WAITS_ON = "Y";

	public static final String DEPENDS_ON = "N";

	public boolean importJob(Node pJob) throws Exception {
		PreparedStatement m_jobStmt = null;
		PreparedStatement m_updStmt = null;
		PreparedStatement m_deps = null;
		PreparedStatement m_depDel = null, m_depSingleDel;
		String mProject;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_jobStmt = metadataConnection
					.prepareStatement("insert into "
							+ tablePrefix
							+ "job(job_id,job_type_id,project_id,parameter_list_id,name,"
							+ "description,retry_attempts,seconds_before_retry,disable_alerting,"
							+ "action) values(?,?,?,?,?,?,?,?,?,?)");

			m_updStmt = metadataConnection
					.prepareStatement("update "
							+ tablePrefix
							+ "job set job_type_id = ?,project_id = ?,parameter_list_id = ?,name = ?,"
							+ "description = ?,retry_attempts = ?,seconds_before_retry = ?,disable_alerting = ?,"
							+ "action = ? where job_id = ?");

			m_deps = metadataConnection
					.prepareStatement("insert into "
							+ tablePrefix
							+ "job_dependencie(parent_job_id,job_id,continue_if_failed) values(?,?,?)");

			m_depDel = metadataConnection.prepareStatement("delete from  "
					+ tablePrefix + "job_dependencie where parent_job_id = ?");

			m_depSingleDel = metadataConnection
					.prepareStatement("delete from  "
							+ tablePrefix
							+ "job_dependencie where parent_job_id = ? and job_id = ?");

			// find parameter lists
			HashMap list = new HashMap();
			XMLHelper.listParameterLists(pJob, list);

			// create new parameter list
			int pmID = -1;
			String pm = XMLHelper.getAttributeAsString(pJob.getAttributes(),
					"PARAMETER_LIST", null);

			Set set = list.keySet();
			Iterator iter = set.iterator();

			while (iter.hasNext()) {
				String s = (String) list.get(iter.next());
				pmID = this.getParameterListID(s);

				if (pmID == -1) {
					ResourcePool.LogMessage(Thread.currentThread(),
							ResourcePool.INFO_MESSAGE,
							"Creating new parameter list " + s);
					this.createParameterList(s);
				}
			}

			// get default parameter list
			if (pm != null) {
				pmID = this.getParameterListID(pm);
			}

			// create new project if needed
			mProject = XMLHelper.getAttributeAsString(pJob.getAttributes(),
					"PROJECT", null);

			if (mProject == null) {
				ResourcePool
						.LogMessage("No project attribute specified for job, import not possible");

				return false;
			}

			int iProject = this.getProjectID(mProject);

			if (iProject == -1) {
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.INFO_MESSAGE, "Creating new project "
								+ mProject);
				this.createProject(mProject);
				iProject = this.getProjectID(mProject);
			}

			NodeList nl = pJob.getChildNodes();

			for (int i = 0; i < nl.getLength(); i++) {
				Node n = nl.item(i);

				if (n.getNodeType() == Node.ELEMENT_NODE) {
					if (n.getNodeName().equalsIgnoreCase("DEPENDS_ON")
							|| n.getNodeName().equalsIgnoreCase("WAITS_ON")) {
						String dependentJob = n.getFirstChild().getNodeValue();

						if (this.getJob(dependentJob) == null) {
							ResourcePool.LogMessage("Inferring dependent job: "
									+ dependentJob);

							// insert blank dependent job
							m_jobStmt.setString(1, dependentJob);
							m_jobStmt.setInt(2, 0);
							m_jobStmt.setInt(3, iProject);
							m_jobStmt.setNull(4, Types.INTEGER);
							m_jobStmt.setNull(5, Types.VARCHAR);
							m_jobStmt.setNull(6, Types.VARCHAR);
							m_jobStmt.setNull(7, Types.INTEGER);
							m_jobStmt.setNull(8, Types.INTEGER);
							m_jobStmt.setNull(9, Types.VARCHAR);
							m_jobStmt.setNull(10, Types.LONGVARCHAR);
							m_jobStmt.executeUpdate();
						}
					}
				}
			}

			// Check for existance
			String ID = XMLHelper.getAttributeAsString(pJob.getAttributes(),
					"ID", null);

			if (ID == null) {
				ResourcePool
						.LogMessage("No ID attribute specified for job, import not possible");

				return false;
			}

			String type = XMLHelper.getAttributeAsString(pJob.getAttributes(),
					"TYPE", null);

			if (type == null) {
				ResourcePool
						.LogMessage("No TYPE attribute specified for job, import not possible");

				return false;
			}

			ETLJob eJob = this.getJob(ID);

			// if not existing then insert
			if (eJob == null) {
				m_jobStmt.setString(1, ID);
				m_jobStmt.setInt(2, getJobTypeID(type));
				m_jobStmt.setInt(3, iProject);

				if (pm == null) {
					m_jobStmt.setNull(4, Types.INTEGER);
				} else {
					m_jobStmt.setInt(4, pmID);
				}

				setString(m_jobStmt, 5, XMLHelper.getAttributeAsString(pJob
						.getAttributes(), "NAME", null));
				setString(m_jobStmt, 6, XMLHelper.getAttributeAsString(pJob
						.getAttributes(), "DESCRIPTION", null));
				m_jobStmt.setInt(7, XMLHelper.getAttributeAsInt(pJob
						.getAttributes(), "RETRY_ATTEMPTS", 0));
				m_jobStmt.setInt(8, XMLHelper.getAttributeAsInt(pJob
						.getAttributes(), "SECONDS_BEFORE_RETRY", 10));
				setString(m_jobStmt, 9, XMLHelper.getAttributeAsString(pJob
						.getAttributes(), "DISABLE_ALERTING", null));

				// ALL Jobs should be wrapped by tags, ignore DEPENDS_ON and
				// WAITS_ON.
				nl = pJob.getChildNodes();

				String action = "";

				for (int i = 0; i < nl.getLength(); i++) {
					Node n = nl.item(i);

					if (n.getNodeType() == Node.ELEMENT_NODE) {
						if (n.getNodeName().equalsIgnoreCase("OSJOB")
								|| n.getNodeName().equalsIgnoreCase("EMPTY")) {
							Node na = n.getFirstChild();

							if (na == null) {
								action = "";
							} else {
								action = n.getFirstChild().getNodeValue();
							}
						} else if ((n.getNodeName().equalsIgnoreCase(
								"DEPENDS_ON") == false)
								&& (n.getNodeName()
										.equalsIgnoreCase("WAITS_ON") == false)) {
							action = action + XMLHelper.outputXML(n);
						}
					}
				}

				m_jobStmt.setCharacterStream(10, new java.io.StringReader(
						action), action.length());
				m_jobStmt.executeUpdate();
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.INFO_MESSAGE, "Creating job " + ID);
			} else // update current job
			{
				m_updStmt.setInt(1, getJobTypeID(type));
				m_updStmt.setInt(2, iProject);

				if (pm == null) {
					m_updStmt.setNull(3, Types.INTEGER);
				} else {
					m_updStmt.setInt(3, pmID);
				}

				setString(m_updStmt, 4, XMLHelper.getAttributeAsString(pJob
						.getAttributes(), "NAME", null));
				setString(m_updStmt, 5, XMLHelper.getAttributeAsString(pJob
						.getAttributes(), "DESCRIPTION", null));
				m_updStmt.setInt(6, XMLHelper.getAttributeAsInt(pJob
						.getAttributes(), "RETRY_ATTEMPTS", 0));
				m_updStmt.setInt(7, XMLHelper.getAttributeAsInt(pJob
						.getAttributes(), "SECONDS_BEFORE_RETRY", 10));
				setString(m_updStmt, 8, XMLHelper.getAttributeAsString(pJob
						.getAttributes(), "DISABLE_ALERTING", null));

				// ALL Jobs should be wrapped by tags, ignore DEPENDS_ON and
				// WAITS_ON.
				nl = pJob.getChildNodes();

				String action = "";

				for (int i = 0; i < nl.getLength(); i++) {
					Node n = nl.item(i);

					if (n.getNodeType() == Node.ELEMENT_NODE) {
						if (n.getNodeName().equalsIgnoreCase("OSJOB")
								|| n.getNodeName().equalsIgnoreCase("EMPTY")) {
							Node na = n.getFirstChild();

							if (na == null) {
								action = "";
							} else {
								action = n.getFirstChild().getNodeValue();
							}
						} else if ((n.getNodeName().equalsIgnoreCase(
								"DEPENDS_ON") == false)
								&& (n.getNodeName()
										.equalsIgnoreCase("WAITS_ON") == false)) {
							action = action + XMLHelper.outputXML(n);
						}
					}
				}

				m_updStmt.setCharacterStream(9,
						new java.io.StringReader(action), action.length());
				m_updStmt.setString(10, ID);
				m_updStmt.executeUpdate();
				ResourcePool.LogMessage("Updating job " + ID);
			}

			// add dependencies
			nl = pJob.getChildNodes();

			// delete old dependencies
			m_depDel.setString(1, pJob.getAttributes().getNamedItem("ID")
					.getNodeValue());
			m_depDel.executeUpdate();

			for (int i = 0; i < nl.getLength(); i++) {
				Node n = nl.item(i);

				if (n.getNodeType() == Node.ELEMENT_NODE) {
					if (n.getNodeName().equalsIgnoreCase("DEPENDS_ON")
							|| n.getNodeName().equalsIgnoreCase("WAITS_ON")) {
						String dependentJob = n.getFirstChild().getNodeValue();

						// insert blank dependent job
						m_deps.setString(2, dependentJob);
						m_deps.setString(1, pJob.getAttributes().getNamedItem(
								"ID").getNodeValue());

						// protect against duplicate dependencies
						m_depSingleDel.setString(2, dependentJob);
						m_depSingleDel.setString(1, pJob.getAttributes()
								.getNamedItem("ID").getNodeValue());
						m_depSingleDel.executeUpdate();

						if (n.getNodeName().equalsIgnoreCase("DEPENDS_ON")) {
							m_deps.setString(3, "N");
						} else {
							m_deps.setString(3, "Y");
						}

						m_deps.executeUpdate();
					}
				}
			}

			if (m_jobStmt != null) {
				m_jobStmt.close();
			}

			if (m_updStmt != null) {
				m_updStmt.close();
			}

			if (m_deps != null) {
				m_deps.close();
			}

			if (m_depDel != null) {
				m_depDel.close();
			}

			if (m_depSingleDel != null) {
				m_depSingleDel.close();
			}

			this.metadataConnection.commit();
		}

		// check for existance of dependent jobs
		// if they do not exist create them as empty jobs
		// update job details
		// add dependencies
		return true;
	}

	/**
	 * @param m_jobStmt
	 * @param value
	 * @throws SQLException
	 */
	private void setString(PreparedStatement m_jobStmt, int pos, String value)
			throws SQLException {
		if (value == null) {
			m_jobStmt.setNull(pos, Types.VARCHAR);
		} else {
			m_jobStmt.setString(pos, value);
		}
	}

	public boolean importParameterList(Node pParameterList) throws Exception {
		PreparedStatement m_update = null;
		PreparedStatement m_insert = null;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_insert = metadataConnection
					.prepareStatement("insert into "
							+ tablePrefix
							+ "parameter(parameter_id,parameter_list_id,parameter_name,parameter_value,sub_parameter_list_name)"
							+ " select coalesce(max(parameter_id)+1,1),?,?,?,? from "
							+ tablePrefix + "parameter");

			m_update = metadataConnection
					.prepareStatement("update "
							+ tablePrefix
							+ "parameter set parameter_value = ?,sub_parameter_list_name = ? "
							+ " where parameter_name = ? and parameter_list_id = ?");

			// create new parameter list
			int pmID = -1;
			String s = pParameterList.getAttributes().getNamedItem("NAME")
					.getNodeValue();
			pmID = this.getParameterListID(s);

			if (pmID == -1) {
				ResourcePool.LogMessage(Thread.currentThread(),
						ResourcePool.INFO_MESSAGE, "Creating parameter list "
								+ s);
				this.createParameterList(s);
				pmID = this.getParameterListID(s);
			}

			NodeList nl = pParameterList.getChildNodes();

			for (int i = 0; i < nl.getLength(); i++) {
				Node n = nl.item(i);

				if (n.getNodeType() == Node.ELEMENT_NODE) {
					if (n.getNodeName().equalsIgnoreCase("PARAMETER")) {
						String value = null;
						Node x1 = n.getFirstChild();

						if (x1 != null) {
							value = x1.getNodeValue();
						}

						String name = n.getAttributes().getNamedItem("NAME")
								.getNodeValue();
						String subList = null;
						Node x = n.getAttributes().getNamedItem(
								"PARAMETER_LIST");

						if (x != null) {
							subList = x.getNodeValue();
						}

						if (mEncryptionEnabled) {
							if (name.equalsIgnoreCase("PASSWORD")
									&& value != null) {
								value = this.mEncryptor.encrypt(value);
							}
						}

						String[] res = this.getParameterValue(pmID, name);

						if ((res == null) || (res.length == 0)) {
							ResourcePool.LogMessage(Thread.currentThread(),
									ResourcePool.INFO_MESSAGE,
									"Creating new parameter " + s + "." + name);
							m_insert.setInt(1, pmID);
							m_insert.setString(2, name);

							if (value == null) {
								m_insert.setNull(3, Types.VARCHAR);
							} else {
								m_insert.setString(3, value);
							}

							if (subList != null) {
								m_insert.setString(4, subList);
							} else {
								m_insert.setNull(4, Types.VARCHAR);
							}

							m_insert.executeUpdate();
						} else {
							ResourcePool.LogMessage("Updating parameter " + s
									+ "." + name);

							if (value == null) {
								m_update.setNull(1, Types.VARCHAR);
							} else {
								m_update.setString(1, value);
							}

							if (subList != null) {
								m_update.setString(2, subList);
							} else {
								m_update.setNull(2, Types.VARCHAR);
							}

							m_update.setString(3, name);
							m_update.setInt(4, pmID);
							m_update.executeUpdate();
						}
					}
				}
			}

			this.metadataConnection.commit();

			if (m_update != null) {
				m_update.close();
			}

			if (m_insert != null) {
				m_insert.close();
			}

			this.metadataConnection.commit();
		}

		// check for existance of dependent jobs
		// if they do not exist create them as empty jobs
		// update job details
		// add dependencies
		return true;
	}

	private boolean setParameterValue(int iParameterListId,
			String strParameterName, String strValue) throws Exception {
		PreparedStatement m_update = null;

		try {
			synchronized (this.oLock) {
				// Make metadata connection alive.
				refreshMetadataConnection();

				m_update = metadataConnection
						.prepareStatement("update "
								+ tablePrefix
								+ "parameter set parameter_value = ? "
								+ " where parameter_name = ? and parameter_list_id = ?");

				if (mEncryptionEnabled) {
					if (strParameterName.equalsIgnoreCase("PASSWORD")) {
						strValue = this.mEncryptor.encrypt(strValue);
					}
				}

				if (strValue == null) {
					m_update.setNull(1, Types.VARCHAR);
				} else {
					m_update.setString(1, strValue);
				}

				m_update.setString(2, strParameterName);
				m_update.setInt(3, iParameterListId);
				m_update.executeUpdate();

				this.metadataConnection.commit();

				if (m_update != null) {
					m_update.close();
				}
			}
		} catch (Exception e) {
			if (strParameterName == null) {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
						"Error setting parameter list (id = "
								+ iParameterListId + "): " + e.getMessage());
			} else {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
						"Error setting parameter list (name = "
								+ strParameterName + "): " + e.getMessage());
			}

			return false;
		}

		// check for existance of dependent jobs
		// if they do not exist create them as empty jobs
		// update job details
		// add dependencies
		return true;
	}

	public String[][] getJobDependencies(String pJobID) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList jobsToFetch = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT JOB_ID,CONTINUE_IF_FAILED FROM  "
							+ tablePrefix + "JOB_DEPENDENCIE A "
							+ "WHERE PARENT_JOB_ID = ?");
			m_stmt.setString(1, pJobID);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				try {
					String[] res = new String[2];

					res[0] = m_rs.getString(1);

					if (m_rs.getString(2).equalsIgnoreCase(WAITS_ON)) {
						res[1] = WAITS_ON;
					} else {
						res[1] = DEPENDS_ON;
					}

					jobsToFetch.add(res);
				} catch (Exception e) {
					System.out.println("Error creating job: " + e.getMessage());

					return null;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		String[][] dependencies = new String[jobsToFetch.size()][];

		jobsToFetch.toArray(dependencies);

		return (dependencies);
	}

	static final String[][] jobReferences = {
			{ "ALERT_SUBSCRIPTION", "JOB_ID" },
			{ "JOB_DEPENDENCIE", "JOB_ID" },
			{ "JOB_DEPENDENCIE", "PARENT_JOB_ID" }, { "JOB_ERROR", "JOB_ID" },
			{ "JOB_ERROR_HIST", "JOB_ID" }, { "JOB_LOG", "JOB_ID" },
			{ "JOB_LOG_HIST", "JOB_ID" }, { "JOB_QA_HIST", "JOB_ID" },
			{ "JOB_SCHEDULE", "JOB_ID" }, { "JOB", "JOB_ID" }, };

	public boolean deleteJob(String pJobID) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			for (int i = 0; i < jobReferences.length; i++) {
				m_stmt = metadataConnection.prepareStatement("DELETE  FROM "
						+ tablePrefix + jobReferences[i][0] + " WHERE "
						+ jobReferences[i][1] + " = ?");
				m_stmt.setString(1, pJobID);
				m_stmt.executeUpdate();

				if (m_stmt != null) {
					m_stmt.close();
				}
			}

			this.metadataConnection.commit();
		}

		return (true);
	}

	public String getProjectName(int pProjectID) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		String projectName = "";

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT PROJECT_DESC FROM  "
							+ tablePrefix + "PROJECT A "
							+ "WHERE PROJECT_ID = ?");
			m_stmt.setInt(1, pProjectID);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				projectName = m_rs.getString(1);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return (projectName);
	}

	public int getProjectID(String pProjectName) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		int projectID = -1;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT PROJECT_ID FROM  " + tablePrefix
							+ "PROJECT A " + "WHERE PROJECT_DESC = ?");
			m_stmt.setString(1, pProjectName);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				projectID = m_rs.getInt(1);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return (projectID);
	}

	public Object[] getProjects() throws SQLException, java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList projects = new ArrayList();

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT PROJECT_ID, PROJECT_DESC FROM  "
							+ tablePrefix + "PROJECT A ");
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				Object[] tmp = new Object[2];
				tmp[0] = new Integer(m_rs.getInt(1));
				tmp[1] = m_rs.getString(2);
				projects.add(tmp);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return projects.toArray();
	}

	public void createProject(String mProjectName) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;

		if (this.getProjectID(mProjectName) == -1) {
			synchronized (this.oLock) {
				// Make metadata connection alive.
				refreshMetadataConnection();

				m_stmt = metadataConnection.prepareStatement("INSERT INTO "
						+ tablePrefix + "PROJECT(PROJECT_ID,PROJECT_DESC) "
						+ "SELECT COALESCE(MAX(PROJECT_ID)+1,1),? FROM "
						+ tablePrefix + "PROJECT ");
				m_stmt.setString(1, mProjectName);
				m_stmt.executeUpdate();

				this.metadataConnection.commit();

				if (m_stmt != null) {
					m_stmt.close();
				}
			}
		}

		return;
	}

	public void createParameterList(String mPlist) throws SQLException,
			java.lang.Exception {
		PreparedStatement m_stmt = null;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection.prepareStatement("INSERT INTO "
					+ tablePrefix
					+ "PARAMETER_LIST(PARAMETER_LIST_ID,PARAMETER_LIST_NAME) "
					+ "SELECT COALESCE(MAX(PARAMETER_LIST_ID)+1,1),? FROM "
					+ tablePrefix + "PARAMETER_LIST "
					+ "WHERE NOT EXISTS (SELECT PARAMETER_LIST_NAME FROM "
					+ tablePrefix + "PROJECT WHERE PARAMETER_LIST_NAME = ?)");
			m_stmt.setString(1, mPlist);
			m_stmt.setString(2, mPlist);
			m_stmt.executeUpdate();

			this.metadataConnection.commit();

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return;
	}

	/**
	 * Insert the method's description here. Creation date: (5/15/2002 1:44:11
	 * PM)
	 * 
	 * @return double
	 * @param pIDName
	 *            java.lang.String
	 */
	public long getBatchOfIDValues(String pIDName, long idBatchSize)
			throws Exception {
		PreparedStatement m_stmt = null;
		long maxID = -1;
		Exception e = null;

		synchronized (this.oLock) {
			try {
				// Make metadata connection alive.
				refreshMetadataConnection();

				m_stmt = metadataConnection
						.prepareStatement("SELECT CURRENT_VALUE FROM  "
								+ tablePrefix
								+ "ID_GENERATOR WHERE ID_NAME = ? FOR UPDATE");
				m_stmt.setString(1, pIDName);

				ResultSet m_rs = m_stmt.executeQuery();

				while (m_rs.next()) {
					maxID = m_rs.getLong(1);
				}

				// Close open resources
				if (m_rs != null) {
					m_rs.close();
				}

				if (m_stmt != null) {
					m_stmt.close();
				}

				if (maxID == -1) {
					ResourcePool.LogMessage("Sequence " + pIDName
							+ " being auto-created and defaulting to 0");
					m_stmt = metadataConnection.prepareStatement("insert into "
							+ tablePrefix
							+ "ID_GENERATOR(ID_NAME,CURRENT_VALUE) VALUES('"
							+ pIDName + "',0)");
					m_stmt.executeUpdate();

					if (m_stmt != null) {
						m_stmt.close();
					}

					maxID = 0;
				}

				m_stmt = metadataConnection
						.prepareStatement("UPDATE  "
								+ tablePrefix
								+ "ID_GENERATOR SET CURRENT_VALUE = ? WHERE ID_NAME = ?"); //$NON-NLS-1$

				m_stmt.setDouble(1, maxID + idBatchSize);
				m_stmt.setString(2, pIDName);

				m_stmt.execute();
				metadataConnection.commit();

				if (m_stmt != null) {
					m_stmt.close();
				}
			} catch (SQLException ee) {
				e = ee;
			} catch (Exception ee) {
				e = ee;
			}
		}

		if (e != null) {
			throw e;
		}

		return maxID;
	}

	/**
	 * Insert the method's description here. Creation date: (5/9/2002 10:49:22
	 * PM)
	 * 
	 * @return java.lang.Object[]
	 * @param pParameterSetID
	 *            int
	 */
	public PageParserPageDefinition[] getPageParserParameters(
			int pParameterSetID, ETLStatus pStatus) {
		PreparedStatement m_stmt = null;
		PageParserPageDefinition[] pDefs = null;
		ResultSet m_rs = null;
		PreparedStatement m_pStmt = null;
		int fieldCnt = 0;

		synchronized (this.oLock) {
			try {
				refreshMetadataConnection();

				m_stmt = metadataConnection
						.prepareStatement("select weight,protocol,hostname,directory,template,parameter_list_id from  "
								+ tablePrefix
								+ "page_set_to_page_definition a,  "
								+ tablePrefix
								+ "page_definition b where a.PAGE_ID = b.PAGE_ID and a.page_set_id = ? ORDER by weight ASC");
				m_stmt.setInt(1, pParameterSetID);
				m_rs = m_stmt.executeQuery();

				while (m_rs.next()) {
					if (pDefs == null) {
						pDefs = new PageParserPageDefinition[fieldCnt + 1];
					} else {
						fieldCnt++;

						PageParserPageDefinition[] tmp = new PageParserPageDefinition[fieldCnt + 1];
						System.arraycopy(pDefs, 0, tmp, 0, pDefs.length);
						pDefs = tmp;
					}

					pDefs[fieldCnt] = new PageParserPageDefinition();

					pDefs[fieldCnt].setWeight(m_rs.getInt(1));
					pDefs[fieldCnt].setProtocol(m_rs.getString(2));
					pDefs[fieldCnt].setHostName(m_rs.getString(3));
					pDefs[fieldCnt].setDirectory(m_rs.getString(4));
					pDefs[fieldCnt].setTemplate(m_rs.getString(5));

					if (m_pStmt == null) {
						m_pStmt = metadataConnection
								.prepareStatement(" SELECT parameter_name, parameter_value, parameter_required, remove_parameter_value, remove_parameter,value_seperator  FROM  "
										+ tablePrefix
										+ "page_parameter_list where parameter_list_id = ?");
					}

					m_pStmt.setInt(1, m_rs.getInt(6));

					ResultSet rs = m_pStmt.executeQuery();

					PageParserPageParameter[] pageParameters = null;
					int pCount = 0;

					while (rs.next()) {
						if (pageParameters == null) {
							pageParameters = new PageParserPageParameter[pCount + 1];
						} else {
							pCount++;

							PageParserPageParameter[] tmp = new PageParserPageParameter[pCount + 1];
							System.arraycopy(pageParameters, 0, tmp, 0,
									pageParameters.length);
							pageParameters = tmp;
						}

						pageParameters[pCount] = new PageParserPageParameter();
						pageParameters[pCount]
								.setParameterName(rs.getString(1));
						pageParameters[pCount].setParameterValue(rs
								.getString(2));

						if ((rs.getString(3) != null)
								&& (rs.getString(3).compareToIgnoreCase("Y") == 0)) {
							pageParameters[pCount].setParameterRequired(true);
						} else {
							pageParameters[pCount].setParameterRequired(false);
						}

						if ((rs.getString(4) != null)
								&& (rs.getString(4).compareToIgnoreCase("Y") == 0)) {
							pageParameters[pCount]
									.setRemoveParameterValue(true);
						} else {
							pageParameters[pCount]
									.setRemoveParameterValue(false);
						}

						if ((rs.getString(5) != null)
								&& (rs.getString(5).compareToIgnoreCase("Y") == 0)) { //$NON-NLS-1$
							pageParameters[pCount].setRemoveParameter(true);
						} else {
							pageParameters[pCount].setRemoveParameter(false);
						}

						pageParameters[pCount].setValueSeperator(rs
								.getString(6));
					}

					pDefs[fieldCnt].setValidPageParameters(pageParameters);

					if (rs != null) {
						rs.close();
					}
				}

				// Close open resources
				if (m_rs != null) {
					m_rs.close();
				}

				if (m_stmt != null) {
					m_stmt.close();
				}

				if (m_pStmt != null) {
					m_pStmt.close();
				}
			} catch (SQLException ee) {
				if (pStatus != null) {
					pStatus.setErrorCode(ee.getErrorCode());
					pStatus
							.setErrorMessage("getPageParserParameterDefinition: "
									+ ee);
				} else {
					System.err.println("getPageParserParameterDefinition: "
							+ ee);
				}

				return (null);
			} catch (Exception ee) {
				if (pStatus != null) {
					pStatus
							.setErrorMessage("getPageParserParameterDefinition: "
									+ ee);
				} else {
					System.err.println("getPageParserParameterDefinition: "
							+ ee);
				}

				return (null);
			}
		}

		return (pDefs);
	}

	public Object[][] getParameterList(String strParameterListName) {
		return getParameterList(strParameterListName, this
				.getParameterListID(strParameterListName));
	}

	public Object[][] getParameterList(int iParameterListID) {
		return getParameterList(null, iParameterListID);
	}

	// Use name first to find the parameters for this list. If name is null,
	// then use ID.
	// Returns: Object[num_parameters][3] where:
	// Object[x][PARAMETER_NAME] = parameter name
	// Object[x][PARAMETER_VALUE] = parameter value
	// Object[x][SUB_PARAMETER_LIST_NAME] = sub parameter list name or null if
	// none
	// Object[x][SUB_PARAMETER_LIST] = sub parameter array or null if none
	public Object[][] getParameterList(String strParameterListName,
			int iParameterListID) {
		PreparedStatement stmt;
		ResultSet rs;
		Object[][] parameterList = null;
		int fieldCnt = 0;
		int iSubParameterListID;
		ArrayList postEncrypt = new ArrayList();
		try {
			synchronized (this.oLock) {
				refreshMetadataConnection();

				if (strParameterListName == null) {
					String sql = null;

					if (bAnsi92OuterJoin) {
						sql = "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
								+ tablePrefix
								+ "PARAMETER p "
								+ " inner join "
								+ tablePrefix
								+ "PARAMETER_LIST pl on (p.PARAMETER_LIST_ID = pl.PARAMETER_LIST_ID) "
								+ " left outer join "
								+ tablePrefix
								+ "PARAMETER_LIST spl "
								+ " on (p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID) "
								+ " WHERE pl.PARAMETER_LIST_ID = ?";
					} else {
						sql = "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
								+ tablePrefix
								+ "PARAMETER p,  "
								+ tablePrefix
								+ "PARAMETER_LIST spl WHERE p.PARAMETER_LIST_ID = ? AND p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID (+)"; //$NON-NLS-1$
					}

					stmt = metadataConnection.prepareStatement(sql);
					stmt.setInt(1, iParameterListID);
				} else // use name
				{
					String sql = null;

					if (bAnsi92OuterJoin) {
						sql = "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
								+ tablePrefix
								+ "PARAMETER p "
								+ " inner join "
								+ tablePrefix
								+ "PARAMETER_LIST pl on (p.PARAMETER_LIST_ID = pl.PARAMETER_LIST_ID) "
								+ " left outer join "
								+ tablePrefix
								+ "PARAMETER_LIST spl "
								+ " on (p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID) "
								+ " WHERE pl.PARAMETER_LIST_NAME LIKE ?";
					} else {
						sql = "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
								+ tablePrefix
								+ "PARAMETER p,  "
								+ tablePrefix
								+ "PARAMETER_LIST pl,  "
								+ tablePrefix
								+ "PARAMETER_LIST spl WHERE p.PARAMETER_LIST_ID = pl.PARAMETER_LIST_ID AND pl.PARAMETER_LIST_NAME LIKE ? AND p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID (+)";
					}

					stmt = metadataConnection.prepareStatement(sql);
					stmt.setString(1, strParameterListName);
				}

				rs = stmt.executeQuery();

				while (rs.next()) {
					if (parameterList == null) {
						parameterList = new Object[fieldCnt + 1][4];
					} else {
						fieldCnt++;

						Object[][] tmp = new Object[fieldCnt + 1][4];
						System.arraycopy(parameterList, 0, tmp, 0,
								parameterList.length);
						parameterList = tmp;
					}

					parameterList[fieldCnt][PARAMETER_NAME] = rs.getString(1);
					parameterList[fieldCnt][PARAMETER_VALUE] = rs.getString(2);

					if (mEncryptionEnabled) {
						// auto encrypt any passwords
						if (((String) parameterList[fieldCnt][PARAMETER_NAME])
								.equalsIgnoreCase("PASSWORD")) {
							try {
								parameterList[fieldCnt][PARAMETER_VALUE] = this.mEncryptor
										.decrypt((String) parameterList[fieldCnt][PARAMETER_VALUE]);
							} catch (Exception e) {
								if (iParameterListID >= 0)
									postEncrypt
											.add(new Object[] {
													new Integer(
															iParameterListID),
													parameterList[fieldCnt][PARAMETER_NAME],
													parameterList[fieldCnt][PARAMETER_VALUE] });
							}
						}
					}

					// Check for sub parameter list...
					iSubParameterListID = rs.getInt(3);

					if (rs.wasNull() == false) {
						parameterList[fieldCnt][SUB_PARAMETER_LIST] = new Integer(
								iSubParameterListID);
					}

					parameterList[fieldCnt][SUB_PARAMETER_LIST_NAME] = rs
							.getString(4);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}

			for (int i = 0; i < postEncrypt.size(); i++) {
				Object[] tmp = (Object[]) postEncrypt.get(i);

				this.setParameterValue(((Integer) tmp[0]).intValue(),
						(String) tmp[1], (String) tmp[2]);
			}
		} catch (Exception e) {
			if (strParameterListName == null) {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
						"Error getting parameter list (id = "
								+ iParameterListID + "): " + e.getMessage());
			} else {
				ResourcePool
						.LogMessage(this, ResourcePool.ERROR_MESSAGE,
								"Error getting parameter list (name = "
										+ strParameterListName + "): "
										+ e.getMessage());
			}

			return null;
		}

		return parameterList;
	}

	// 
	public String[] getParameterValue(int iParameterListID,
			String strParameterName) {
		PreparedStatement stmt;
		ResultSet rs;
		ArrayList vals = new ArrayList();
		ArrayList postEncrypt = new ArrayList();
		try {
			synchronized (this.oLock) {
				refreshMetadataConnection();

				stmt = metadataConnection
						.prepareStatement("SELECT PARAMETER_VALUE FROM "
								+ tablePrefix
								+ "PARAMETER WHERE PARAMETER_LIST_ID = ? AND PARAMETER_NAME = ?");
				stmt.setInt(1, iParameterListID);
				stmt.setString(2, strParameterName);

				rs = stmt.executeQuery();

				while (rs.next()) {
					String value = rs.getString(1);

					if (mEncryptionEnabled) {
						// auto encrypt any passwords
						if (strParameterName.equalsIgnoreCase("PASSWORD")) {
							try {
								value = this.mEncryptor.decrypt(value);
							} catch (Exception e) {
								postEncrypt.add(new Object[] {
										new Integer(iParameterListID),
										strParameterName, value });
							}
						}
					}
					vals.add(value);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}

			for (int i = 0; i < postEncrypt.size(); i++) {
				Object[] tmp = (Object[]) postEncrypt.get(i);

				this.setParameterValue(((Integer) tmp[0]).intValue(),
						(String) tmp[1], (String) tmp[2]);
			}

		} catch (Exception e) {
			if (strParameterName == null) {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
						"Error getting parameter list (id = "
								+ iParameterListID + "): " + e.getMessage());
			} else {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
						"Error getting parameter list (name = "
								+ strParameterName + "): " + e.getMessage());
			}

			return null;
		}

		String[] res = new String[vals.size()];
		vals.toArray(res);

		return res;
	}

	public String[] getValidParameterListName(String strParameterListName) {
		PreparedStatement stmt;
		ResultSet rs;
		ArrayList parameterLists = new ArrayList();

		try {
			synchronized (this.oLock) {
				stmt = metadataConnection
						.prepareStatement("SELECT DISTINCT PARAMETER_LIST_NAME FROM  "
								+ tablePrefix
								+ "PARAMETER_LIST WHERE PARAMETER_LIST_NAME LIKE ?"); //$NON-NLS-1$
				stmt.setString(1, strParameterListName);

				rs = stmt.executeQuery();

				while (rs.next()) {
					parameterLists.add(rs.getString(1));
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}
		} catch (Exception e) {
			ResourcePool.LogMessage(Thread.currentThread().getName(),
					ResourcePool.ERROR_MESSAGE,
					"Error getting parameter list (name = "
							+ strParameterListName + "): " + e.getMessage());

			return null;
		}

		if (parameterLists.size() == 0) {
			return null;
		}

		String[] res = new String[parameterLists.size()];

		parameterLists.toArray(res);

		return res;
	}

	public String getParameterListName(int strParameterID) {
		PreparedStatement stmt;
		ResultSet rs;
		String parameterListName = null;

		try {
			synchronized (this.oLock) {
				stmt = metadataConnection
						.prepareStatement("SELECT PARAMETER_LIST_NAME FROM  "
								+ tablePrefix
								+ "PARAMETER_LIST WHERE PARAMETER_LIST_ID = ?"); //$NON-NLS-1$
				stmt.setInt(1, strParameterID);

				rs = stmt.executeQuery();

				while (rs.next()) {
					parameterListName = rs.getString(1);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}
		} catch (Exception e) {
			ResourcePool.LogMessage(Thread.currentThread().getName(),
					ResourcePool.ERROR_MESSAGE,
					"Error getting parameter list (ID = " + strParameterID
							+ "): " + e.getMessage());

			return null;
		}

		return parameterListName;
	}

	public int getParameterListID(String strParameterID) {
		PreparedStatement stmt;
		ResultSet rs;
		int parameterListID = -1;

		try {
			synchronized (this.oLock) {
				stmt = metadataConnection
						.prepareStatement("SELECT PARAMETER_LIST_ID FROM  "
								+ tablePrefix
								+ "PARAMETER_LIST WHERE PARAMETER_LIST_NAME = ?"); //$NON-NLS-1$
				stmt.setString(1, strParameterID);

				rs = stmt.executeQuery();

				while (rs.next()) {
					parameterListID = rs.getInt(1);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}
		} catch (Exception e) {
			ResourcePool.LogMessage(Thread.currentThread().getName(),
					ResourcePool.ERROR_MESSAGE,
					"Error getting parameter list (ID = " + strParameterID
							+ "): " + e.getMessage());

			return -2;
		}

		return parameterListID;
	}

	public int getJobTypeID(String strParameterID) {
		PreparedStatement stmt;
		ResultSet rs;
		int parameterListID = -1;

		try {
			synchronized (this.oLock) {
				stmt = metadataConnection
						.prepareStatement("SELECT JOB_TYPE_ID FROM  "
								+ tablePrefix
								+ "JOB_TYPE WHERE DESCRIPTION = ?"); //$NON-NLS-1$
				stmt.setString(1, strParameterID);

				rs = stmt.executeQuery();

				while (rs.next()) {
					parameterListID = rs.getInt(1);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}
		} catch (Exception e) {
			ResourcePool.LogMessage(Thread.currentThread().getName(),
					ResourcePool.ERROR_MESSAGE, "Error getting job type (ID = "
							+ strParameterID + "): " + e.getMessage());

			return -2;
		}

		return parameterListID;
	}

	public String getJobExecutorClassForTypeID(int pTypeID) {
		PreparedStatement stmt;
		ResultSet rs;
		String className = null;

		try {
			synchronized (this.oLock) {
				stmt = metadataConnection
						.prepareStatement("SELECT CLASS_NAME FROM  "
								+ tablePrefix
								+ "JOB_TYPE WHERE JOB_TYPE_ID = ?");
				stmt.setInt(1, pTypeID);

				rs = stmt.executeQuery();

				while (rs.next()) {
					className = rs.getString(1);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			}
		} catch (Exception e) {
			ResourcePool.LogMessage(Thread.currentThread().getName(),
					ResourcePool.ERROR_MESSAGE,
					"Error getting job class (ID = " + pTypeID + "): "
							+ e.getMessage());

			return null;
		}

		return className;
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 1:20:28
	 * PM)
	 * 
	 * @return java.lang.Object[]
	 * @param pClassName
	 *            java.lang.String
	 */
	public ArrayList getServerExecutorJobTypes(String pClassName)
			throws SQLException, java.lang.Exception {
		ArrayList res = new ArrayList();
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();
			m_stmt = metadataConnection
					.prepareStatement("SELECT JOB_TYPE.CLASS_NAME FROM  "
							+ tablePrefix
							+ "JOB_EXECUTOR ,  "
							+ tablePrefix
							+ "JOB_EXECUTOR_JOB_TYPE ,  "
							+ tablePrefix
							+ "JOB_TYPE WHERE JOB_TYPE.JOB_TYPE_ID = JOB_EXECUTOR_JOB_TYPE.JOB_TYPE_ID AND JOB_EXECUTOR.JOB_EXECUTOR_ID = JOB_EXECUTOR_JOB_TYPE.JOB_EXECUTOR_ID AND JOB_EXECUTOR.CLASS_NAME = ?"); //$NON-NLS-1$
			m_stmt.setString(1, pClassName);
			m_rs = m_stmt.executeQuery();

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				res.add(m_rs.getString(1));
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return (res);
	}

	public boolean recordQAHistory(String job_id, String step_name,
			String qa_id, String qa_type, Date execDate, String details) {
		PreparedStatement m_stmt = null;

		if ((job_id == null) || (step_name == null) || (qa_id == null)
				|| (qa_type == null) || (execDate == null) || (details == null)) {
			ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
					"QA History details incomplete, no record will be stored");

			return false;
		}

		synchronized (this.oLock) {
			String sql = "INSERT INTO  "
					+ tablePrefix
					+ "JOB_QA_HIST(JOB_ID,QA_ID,QA_TYPE,STEP_NAME,DETAILS,RECORD_DATE) VALUES(?,?,?,?,?,?)";

			try {
				// Make metadata connection alive.
				refreshMetadataConnection();

				m_stmt = metadataConnection.prepareStatement(sql);

				m_stmt.setString(1, job_id);
				m_stmt.setString(2, qa_id);
				m_stmt.setString(3, qa_type);
				m_stmt.setString(4, step_name);
				m_stmt.setString(5, details);
				m_stmt.setTimestamp(6, new java.sql.Timestamp(execDate
						.getTime()));

				m_stmt.execute();

				if (m_stmt != null) {
					m_stmt.close();
				}

				metadataConnection.commit();
			} catch (Exception e) {
				ResourcePool.LogException(e, this);

				return false;
			}
		}

		return true;
	}

	public String[] getQAHistory(String job_id, String step_name, String qa_id,
			String qa_type, int sampleOffSet, int sampleSize)
			throws SQLException, java.lang.Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		String[] res = null;

		synchronized (this.oLock) {
			String sql = "SELECT DETAILS " + " FROM  " + tablePrefix
					+ "JOB_QA_HIST " + " WHERE JOB_ID = ?  "
					+ " AND QA_ID = ?  " + " AND QA_TYPE = ?  "
					+ " AND STEP_NAME = ?  ";

			Date[][] dates = new Date[sampleSize][2];

			// build array of start and end dates for offsets
			if (sampleOffSet > 0) {
				sql = sql + "AND (";

				Date today = new Date();

				Calendar sd = Calendar.getInstance();
				Calendar ed = Calendar.getInstance();

				sd.setTime(today);
				ed.setTime(today);

				sd.set(Calendar.HOUR_OF_DAY, 0);
				sd.set(Calendar.MINUTE, 0);
				sd.set(Calendar.SECOND, 0);

				ed.set(Calendar.HOUR_OF_DAY, 23);
				ed.set(Calendar.MINUTE, 59);
				ed.set(Calendar.SECOND, 59);

				for (int i = 0; i < sampleSize; i++) {
					sd.roll(Calendar.DATE, -sampleOffSet);
					ed.roll(Calendar.DATE, -sampleOffSet);

					dates[i][0] = sd.getTime();
					dates[i][1] = ed.getTime();

					if (i > 0) {
						sql = sql + " OR ";
					}

					sql = sql + " RECORD_DATE between ? and ? ";
				}

				sql = sql + ")";
			}

			sql = sql + " ORDER BY RECORD_DATE DESC";

			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection.prepareStatement(sql);

			m_stmt.setString(1, job_id);
			m_stmt.setString(2, qa_id);
			m_stmt.setString(3, qa_type);
			m_stmt.setString(4, step_name);

			int pos = 5;

			if (sampleOffSet > 0) {
				for (int i = 0; i < sampleSize; i++) {
					m_stmt.setTimestamp(pos, new java.sql.Timestamp(dates[i][0]
							.getTime()));
					pos++;
					m_stmt.setTimestamp(pos, new java.sql.Timestamp(dates[i][1]
							.getTime()));
					pos++;
				}
			}

			m_rs = m_stmt.executeQuery();

			ArrayList results = new ArrayList();

			// cycle through pending jobs setting next run date
			while (m_rs.next()
					&& ((results.size() < sampleSize) || (sampleSize == -1))) {
				results.add(m_rs.getString(1));
			}

			if (results.size() > 0) {
				res = new String[results.size()];

				results.toArray(res);
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return res;
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 11:23:34
	 * PM)
	 * 
	 * @return java.lang.Object[]
	 * @param pServerID
	 *            int
	 */
	public Object[][] getServerExecutors(int pServerID) throws SQLException,
			java.lang.Exception {
		Object[][] res = null;
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			m_stmt = metadataConnection
					.prepareStatement("SELECT CLASS_NAME,THREADS,QUEUE_SIZE FROM  "
							+ tablePrefix
							+ "SERVER_EXECUTOR A,  "
							+ tablePrefix
							+ "JOB_EXECUTOR B WHERE A.JOB_EXECUTOR_ID = B.JOB_EXECUTOR_ID AND A.SERVER_ID = ?"); //$NON-NLS-1$
			m_stmt.setInt(1, pServerID);
			m_rs = m_stmt.executeQuery();

			int i = 0;

			// cycle through pending jobs setting next run date
			while (m_rs.next()) {
				if (res == null) {
					res = new Object[i + 1][3];
					res[i][0] = m_rs.getString(1);
					res[i][1] = new Integer(m_rs.getInt(2));
					res[i][2] = new Integer(m_rs.getInt(3));
				} else {
					i++;

					Object[][] tmp = new Object[i + 1][3];
					tmp[i][0] = m_rs.getString(1);
					tmp[i][1] = new Integer(m_rs.getInt(2));
					tmp[i][2] = new Integer(m_rs.getInt(3));

					System.arraycopy(res, 0, tmp, 0, res.length);
					res = tmp;
				}
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (m_stmt != null) {
				m_stmt.close();
			}
		}

		return (res);
	}

	/**
	 * Insert the method's description here. Creation date: (4/9/2002 10:36:44
	 * AM)
	 * 
	 * @return datasources.SessionDefinition
	 * @param pSessionDefinitionID
	 *            int
	 */
	public SessionDefinition getSessionDefinition(int pSessionDefinitionID,
			ETLStatus pStatus) {
		synchronized (this.oLock) {
			try {
				if (metadataConnection == null) {
					refreshMetadataConnection();
				}

				// set field definition
				SessionDefinition srcSessionDefinition = new SessionDefinition();

				// ideally sql should be loaded from a sql database to allow for
				// easy platform migration
				PreparedStatement stmt = this.metadataConnection
						.prepareStatement("SELECT object_type_id, "
								+ " data_item_data_type_id, "
								+ " variable_name, "
								+ " weight, "
								+ " format_string, "
								+ " session_timeout, "
								+ " webserver_id, "
								+ " destination_field_type_id, "
								+ " peak_sessions_an_hour, "
								+ " case_sensitive, "
								+ " (coalesce(FIRST_CLICK_ID_TIMEOUT,SESSION_TIMEOUT)),"
								+ " (coalesce(MAIN_ID_TIMEOUT,SESSION_TIMEOUT)),"
								+ " (coalesce(PERSISTANT_ID_TIMEOUT,SESSION_TIMEOUT)),"
								+ " (coalesce(IP_BROWSER_ID_TIMEOUT,SESSION_TIMEOUT))"
								+ " FROM  "
								+ tablePrefix
								+ "session_identifier a, "
								+ "       "
								+ tablePrefix
								+ "session_definition b "
								+ " WHERE b.session_definition_id = ? "
								+ " AND b.session_definition_id = a.session_definition_id "
								+ " ORDER BY weight, "
								+ " session_identifier_id "); //$NON-NLS-1$
				stmt.setInt(1, pSessionDefinitionID);

				ResultSet rs = stmt.executeQuery();

				// Access query results
				while (rs.next()) {
					SessionIdentifier tmpSessionIdentifier = new SessionIdentifier();

					if (srcSessionDefinition.TimeOut == 0) {
						srcSessionDefinition.TimeOut = rs.getInt(6);
					}

					if (srcSessionDefinition.FirstClickIdentifierTimeOut == 0) {
						srcSessionDefinition.FirstClickIdentifierTimeOut = rs
								.getInt(11);
					}

					if (srcSessionDefinition.MainIdentifierTimeOut == 0) {
						srcSessionDefinition.MainIdentifierTimeOut = rs
								.getInt(12);
					}

					if (srcSessionDefinition.PersistantIdentifierTimeOut == 0) {
						srcSessionDefinition.PersistantIdentifierTimeOut = rs
								.getInt(13);
					}

					if (srcSessionDefinition.IPBrowserTimeOut == 0) {
						srcSessionDefinition.IPBrowserTimeOut = rs.getInt(14);
					}

					if (srcSessionDefinition.WebServerType == -1) {
						srcSessionDefinition.WebServerType = rs.getInt(7);
					}

					if (srcSessionDefinition.PeakSessionsAnHour == -1) {
						srcSessionDefinition.PeakSessionsAnHour = rs.getInt(9);
					}

					tmpSessionIdentifier.ObjectType = rs.getInt(1);
					tmpSessionIdentifier.DataType = rs.getInt(2);
					tmpSessionIdentifier.Weight = rs.getInt(4);
					tmpSessionIdentifier.FormatString = rs.getString(5);
					tmpSessionIdentifier.DestinationObjectType = rs.getInt(8);

					if (rs.getInt(10) == 1) {
						tmpSessionIdentifier.setVariableName(rs.getString(3),
								true);
					} else {
						tmpSessionIdentifier.setVariableName(rs.getString(3),
								false);
					}

					srcSessionDefinition
							.addSessionIdentifier(tmpSessionIdentifier);
				}

				// Close open resources
				if (rs != null) {
					rs.close();
				}

				if (stmt != null) {
					stmt.close();
				}

				return (srcSessionDefinition);
			} catch (SQLException ee) {
				if (pStatus != null) {
					pStatus.setErrorCode(ee.getErrorCode());
					pStatus.setErrorMessage("SessionDefinition:" + ee);
				} else {
					System.err.println("getFlatFileSourceDefinition: " + ee);
				}

				return (null);
			} catch (Exception ee) {
				if (pStatus != null) {
					pStatus.setErrorMessage("SessionDefinition:" + ee);
				} else {
					System.err.println("getFlatFileSourceDefinition: " + ee);
				}

				return (null);
			}
		}
	}

	/**
	 * Insert the method's description here. Creation date: (3/5/2002 3:22:37
	 * PM)
	 * 
	 * @return jobscheduler.EtlJob[]
	 * @param pExecutingJobs
	 *            boolean
	 * @param pPendingJobs
	 *            boolean
	 * @param pExecutedJobs
	 *            boolean
	 * @param pLoadID
	 *            int
	 */
	public ETLJob[] getStatus(boolean pExecutingJobs, boolean pPendingJobs,
			boolean pExecutedJobs, int pLoadID) {
		return null;
	}

	/**
	 * Insert the method's description here. Creation date: (3/5/2002 3:36:19
	 * PM)
	 */
	protected void refreshMetadataConnection() throws SQLException,
			java.lang.Exception {
		if (this.metadataConnection != null) {
			try {
				if (ResourcePool.testConnection(metadataConnection) == false) {
					System.err
							.println("checkConnection connection closed for reason unknown");
					metadataConnection = null;
				}
			} catch (Exception ee) {
				System.err.println("checkConnection Exception: " + ee);
				System.err
						.println("checkConnection SQLException: Server will attempt to reconnect");

				metadataConnection = null;
				// testConnectionStmt = null;
			}
		}

		if (metadataConnection == null) {
			Class.forName(JDBCDriver);

			metadataConnection = DriverManager.getConnection(JDBCURL, Username,
					Password);

			metadataConnection.setAutoCommit(false);

			DatabaseMetaData mdDB = metadataConnection.getMetaData();

			boolean ansi92 = mdDB.supportsANSI92EntryLevelSQL();
			boolean outerJoins = mdDB.supportsLimitedOuterJoins();

			int dbType = -1;

			for (int i = 0; i < dbTypes.length; i++) {
				if (dbTypes[i].equalsIgnoreCase(mdDB.getDatabaseProductName())) {
					dbType = i;

					// finish loop
					i = dbTypes.length;
					this.currentTimeStampSyntax = this.dbTimeStampTypes[dbType];
					this.nextLoadIDSyntax = this.dbSequenceSyntax[dbType]
							.replaceAll("#", "LOAD_ID");
                    this.useIdentityColumn = this.dbUseIdentityColumn[dbType];
                    
					this.nextServerIDSyntax = this.dbSequenceSyntax[dbType]
							.replaceAll("#", "SERVER_ID");
                    this.singleRowPullSyntax = this.dbSingleRowPull[dbType];
                    this.mResolvedLoadTableName = this.mLoadTableName[dbType];
                    
                    String incrementIdenentityColumnSyntax = this.dbIncrementIdentityColumnSyntax[dbType];
                    if(incrementIdenentityColumnSyntax != null){
                        mIncIdentColStmt  = metadataConnection.prepareStatement(incrementIdenentityColumnSyntax);
                    }
				}
			}

			if (dbType == -1) {
				throw new Exception("ERROR: " + mdDB.getDatabaseProductName()
						+ " is not supported for metadata storage");
			}

			if (ansi92 && outerJoins) {
				this.bAnsi92OuterJoin = true;
			}
		}
	}

    final protected String loadTableName() {
        return this.mResolvedLoadTableName == null?"LOAD":this.mResolvedLoadTableName;
    }
    
	/**
	 * Insert the method's description here. Creation date: (5/1/2002 7:29:49
	 * PM)
	 * 
	 * @return int
	 * @param pServerName
	 *            java.lang.String
	 */
	public int registerServer(String pServerName) {
		String sql = null;
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		int serverID = -1;

		synchronized (this.oLock) {
			try {
				// Make metadata connection alive.
				refreshMetadataConnection();
				m_stmt = metadataConnection
						.prepareStatement("SELECT MAX(SERVER_ID) FROM  "
								+ tablePrefix + "SERVER WHERE SERVER_NAME = ?"); //$NON-NLS-1$

				m_stmt.setString(1, pServerName);

				m_rs = m_stmt.executeQuery();

				// cycle through pending jobs setting next run date
				while (m_rs.next()) {
					serverID = m_rs.getInt(1);

					if (m_rs.wasNull() == true) {
						serverID = -1;
					}
				}

				// Close open resources
				if (m_rs != null) {
					m_rs.close();
				}

				if (m_stmt != null) {
					m_stmt.close();
				}

				if (serverID == -1) {
					// create new server
					PreparedStatement selNextServerID = this.metadataConnection
							.prepareStatement(this.useIdentityColumn?this.nextServerIDSyntax:("SELECT " + nextServerIDSyntax
									+ " " + singleRowPullSyntax + "")); //$NON-NLS-1$

                    if(this.mIncIdentColStmt != null){                        
                        this.mIncIdentColStmt.execute();                        
                    }
                    
                    m_rs = selNextServerID.executeQuery();

					// cycle through pending jobs setting next run date
					while (m_rs.next()) {
						serverID = m_rs.getInt(1);
					}

					if (m_rs != null) {
						m_rs.close();
					}

					if (m_stmt != null) {
						m_stmt.close();
					}

					m_stmt = this.metadataConnection
							.prepareStatement("INSERT INTO  "
									+ tablePrefix
									+ "SERVER(SERVER_ID,SERVER_NAME,STATUS_ID) VALUES(?,?,?)"); //$NON-NLS-1$
					m_stmt.setInt(1, serverID);
					m_stmt.setString(2, pServerName);
					m_stmt.setInt(3, ETLServerStatus.SERVER_ALIVE);

					m_stmt.execute();

					ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
							"Registering server " + pServerName
									+ " for the first time");
					ResourcePool
							.LogMessage("Server "
									+ pServerName
									+ " will be initialized with a predefined number of executors, to modify this go to the server_executors table.");

					m_stmt = this.metadataConnection
							.prepareStatement("INSERT INTO  "
									+ tablePrefix
									+ "SERVER_EXECUTOR(SERVER_ID,JOB_EXECUTOR_ID,THREADS) "
									+ " SELECT ?,JOB_EXECUTOR_ID,CASE JOB_EXECUTOR_ID WHEN 4 THEN 1 ELSE 2 END FROM "
									+ tablePrefix + "JOB_EXECUTOR");
					m_stmt.setInt(1, serverID);
					m_stmt.execute();

					metadataConnection.commit();
				} else {
					m_stmt = this.metadataConnection
							.prepareStatement("UPDATE  "
									+ tablePrefix
									+ "SERVER SET STATUS_ID = ? WHERE SERVER_ID = ?"); //$NON-NLS-1$
					m_stmt.setInt(1, ETLServerStatus.SERVER_ALIVE);
					m_stmt.setInt(2, serverID);
					m_stmt.execute();

					metadataConnection.commit();
				}

				if (m_rs != null) {
					m_rs.close();
				}

				if (m_stmt != null) {
					m_stmt.close();
				}
			} catch (SQLException e) {
				System.out.println("Error registering server: error:" + e + "("
						+ sql + ")");
			} catch (Exception e) {
				System.out.println("Error registering server: error:" + e);
			}
		}

		return (serverID);
	}

	public void recordJobMessage(ETLJob pETLJob, ETLStep oStep, int iType,
			int iLevel, String strMessage, String strExtendedDetails,
			boolean bSendEmail) {
		this.recordJobMessage(pETLJob, oStep, iType, iLevel, strMessage,
				strExtendedDetails, bSendEmail, new java.util.Date());
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 3:51:42
	 * PM)
	 * 
	 * @param pETLJob
	 *            com.kni.etl.ETLJob
	 */
	public void recordJobMessage(ETLJob pETLJob, ETLStep oStep, int iType,
			int iLevel, String strMessage, String strExtendedDetails,
			boolean bSendEmail, Date dDate) {
		String sql = null;
		PreparedStatement m_stmt = null;

		synchronized (this.oLock) {
			try {
				// Make metadata connection alive.
				refreshMetadataConnection();

				if (metadataConnection != null) {
					String job_id = "NA";
					String step_name = "NA";
					int executionID = -1;

					if (pETLJob != null) {
						job_id = pETLJob.getJobID();
						executionID = pETLJob.getJobExecutionID();
					}

					if (oStep != null) {
						step_name = oStep.toString();
					}

					// write error to log if errors occured
					m_stmt = metadataConnection
							.prepareStatement("INSERT INTO  "
									+ tablePrefix
									+ "Job_Error_Hist(JOB_ID,DM_LOAD_ID,STEP_NAME,MESSAGE,CODE,ERROR_DATETIME,DETAILS) VALUES(?,?,?,?,?,?,?)"); //$NON-NLS-1$

					String msg = strMessage;

					if (msg != null && msg.length() > 800) {
						System.err
								.println("Error to long, trimming stored message. Full message: "
										+ msg);
						msg = msg.substring(0, 800);
					}

					m_stmt.setString(1, job_id);
					m_stmt.setInt(2, executionID);
					m_stmt.setString(3, step_name);
					m_stmt.setString(4, msg);
					m_stmt.setString(5, Integer.toString(iLevel));
					m_stmt.setTimestamp(6, new Timestamp(dDate.getTime()));

					if (strExtendedDetails == null) {
						m_stmt.setNull(7, Types.VARCHAR);
					} else {
						m_stmt.setString(7, strExtendedDetails);
					}

					m_stmt.execute();
					metadataConnection.commit();

					if (bSendEmail) {
						sendErrorEmail(job_id, Integer.toString(iLevel),
								strMessage, strExtendedDetails,
								new java.util.Date(), executionID, pETLJob
										.getDumpFile());
					}

					if (m_stmt != null) {
						m_stmt.close();
					}
				}
			} catch (SQLException e) {
				System.out.println("Error setting status: error:" + e + "("
						+ sql + ")");
			} catch (Exception e) {
				System.out.println("Error setting status: error:" + e);
			}
		}
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 3:51:42
	 * PM)
	 * 
	 * @param pETLJob
	 *            com.kni.etl.ETLJob
	 */
	public void setJobStatus(ETLJob pETLJob) {
		String sql = null;
		PreparedStatement m_stmt = null;

		synchronized (this.oLock) {
			try {
				// Make metadata connection alive.
				refreshMetadataConnection();

				switch (pETLJob.getStatus().getStatusCode()) {
				case ETLJobStatus.WAITING_TO_BE_RETRIED:
				case ETLJobStatus.PENDING_CLOSURE_FAILED:
				case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
					m_stmt = metadataConnection
							.prepareStatement("UPDATE  "
									+ tablePrefix
									+ "JOB_LOG SET END_DATE = "
									+ currentTimeStampSyntax
									+ ", STATUS_ID = ?,MESSAGE =  ? WHERE DM_LOAD_ID = ?"); //$NON-NLS-1$

					break;

				default:
					m_stmt = metadataConnection
							.prepareStatement("UPDATE  "
									+ tablePrefix
									+ "JOB_LOG SET STATUS_ID = ?, MESSAGE = ? WHERE DM_LOAD_ID = ?"); //$NON-NLS-1$

					break;
				}

				m_stmt.setInt(1, pETLJob.getStatus().getStatusCode());
				m_stmt.setString(2,
						pETLJob.getStatus().getStatusMessage() == null ? null
								: (pETLJob.getStatus().getStatusMessage()
										.getBytes().length > 2000 ? pETLJob
										.getStatus().getStatusMessage()
										.substring(0, 1000)
										+ ".." : pETLJob.getStatus()
										.getStatusMessage()));
				m_stmt.setInt(3, pETLJob.getJobExecutionID());

				try {
					m_stmt.execute();
				} catch (SQLException e) {
					ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, e
							.getMessage());
					m_stmt.getConnection().rollback();
					m_stmt.setInt(1, pETLJob.getStatus().getStatusCode());
					m_stmt.setString(2, "Error logging message, see log");
					m_stmt.setInt(3, pETLJob.getJobExecutionID());
					m_stmt.execute();
				}
				if (m_stmt != null) {
					m_stmt.close();
				}

				metadataConnection.commit();

				// write error to log if errors occured
				if ((pETLJob.getStatus().getStatusCode() == ETLJobStatus.PENDING_CLOSURE_FAILED)
						|| (pETLJob.getStatus().getStatusCode() == ETLJobStatus.WAITING_TO_BE_RETRIED)) {
					m_stmt = metadataConnection
							.prepareStatement("INSERT INTO  "
									+ tablePrefix
									+ "Job_Error(JOB_ID,DM_LOAD_ID,MESSAGE,CODE,ERROR_DATETIME) VALUES(?,?,?,?,"
									+ currentTimeStampSyntax + ")"); //$NON-NLS-1$

					String msg = pETLJob.getStatus().getErrorMessage() + "\n"
							+ pETLJob.getStatus().getStatusMessage();

					if (msg != null && msg.length() > 800) {
						System.err
								.println("Error to long, trimming stored message. Full message: "
										+ msg);
						msg = msg.substring(0, 800);
					}
					m_stmt.setString(1, pETLJob.getJobID());
					m_stmt.setInt(2, pETLJob.getJobExecutionID());
					m_stmt.setString(3, msg);
					m_stmt.setString(4, new Integer(pETLJob.getStatus()
							.getErrorCode()).toString());

					m_stmt.execute();
					metadataConnection.commit();

					// if code allows emails, in otherwords not do not send
					// email error code
					if (pETLJob.getStatus().getErrorCode() != ETLJobStatus.DO_NOT_SEND_EMAIL_ERROR_CODE) {
						// if not waiting to be retried
						if (pETLJob.getStatus().getStatusCode() != ETLJobStatus.WAITING_TO_BE_RETRIED) {
							sendErrorEmail(pETLJob.getJobID(), new Integer(
									pETLJob.getStatus().getErrorCode())
									.toString(), pETLJob.getStatus()
									.getErrorMessage(), pETLJob.getStatus()
									.getStatusMessage(), new java.util.Date(),
									pETLJob.getJobExecutionID(), pETLJob
											.getDumpFile());
						}
					}
				}

				if (m_stmt != null) {
					m_stmt.close();
				}
			} catch (SQLException e) {
				System.out.println("Error setting status: error:" + e + "("
						+ sql + ")");
			} catch (Exception e) {
				System.out.println("Error setting status: error:" + e);
			}
		}
	}

	/**
	 * Insert the method's description here. Creation date: (5/15/2002 1:44:11
	 * PM)
	 * 
	 * @return double
	 * @param pIDName
	 *            java.lang.String
	 */
	public void setMaxIDValue(String pIDName, double pValue) {
		String sql = null;
		PreparedStatement m_stmt = null;

		synchronized (this.oLock) {
			try {
				// Make metadata connection alive.
				refreshMetadataConnection();

				m_stmt = metadataConnection
						.prepareStatement("UPDATE  "
								+ tablePrefix
								+ "ID_GENERATOR SET CURRENT_VALUE = ? WHERE ID_NAME = ?"); //$NON-NLS-1$

				m_stmt.setDouble(1, pValue);
				m_stmt.setString(2, pIDName);

				m_stmt.execute();
				metadataConnection.commit();

				if (m_stmt != null) {
					m_stmt.close();
				}
			} catch (SQLException e) {
				System.out.println("Error getting maxID for" + pIDName
						+ ": error:" + e + "(" + sql + ")");
			} catch (Exception e) {
				System.out.println("Error getting maxID for" + pIDName
						+ ": error:" + e);
			}
		}
	}

	/**
	 * Insert the method's description here. Creation date: (3/5/2002 3:16:13
	 * PM)
	 * 
	 * @param pUserName
	 *            java.lang.String
	 * @param pPassword
	 *            java.lang.String
	 * @param pJDBCConnection
	 *            java.lang.String
	 */
	public void setRepository(String pUserName, String pPassword,
			String pJDBCURL, String pJDBCDriver, String pMDPrefix)
			throws Exception {
		JDBCDriver = pJDBCDriver;
		JDBCURL = pJDBCURL;
		Username = pUserName;
		Password = pPassword;

		if (pMDPrefix != null) {
			this.tablePrefix = pMDPrefix;
		}

		refreshMetadataConnection();
		this.checkPassphrase();

	}

	private void checkPassphrase() throws Exception {

		if (mEncryptionEnabled) {
			int id = this.getParameterListID("$INTERNAL");

			String[] o = null;
			if (id != -1)
				o = this.getParameterValue(id, "CHECK");

			if (id == -1 || o == null || o.length == 0) {
				String tmp = "<ETL><PARAMETER_LIST NAME=\"$INTERNAL\"><PARAMETER NAME=\"CHECK\"></PARAMETER></PARAMETER_LIST></ETL>";
				// Build a DOM out of the XML string...
				DocumentBuilderFactory dmf = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder builder = dmf.newDocumentBuilder();
				Document xmlConfig = builder.parse(new InputSource(
						new StringReader(tmp)));
				this.importParameterList(xmlConfig.getElementsByTagName(
						"PARAMETER_LIST").item(0));
				this.setParameterValue(this.getParameterListID("$INTERNAL"),
						"CHECK", this.mEncryptor.encrypt("KETLAVAYA"));
			} else {
				try {
					o[0] = this.mEncryptor.decrypt(o[0]);
				} catch (Exception e) {
					throw new PassphraseException(
							"Pass phrase supplied does not match system pass phrase check, connection disallowed",
							this.mPassphrase, this.mPassphraseFilePath);
				}
				if (!o[0].equals("KETLAVAYA"))
					throw new PassphraseException(
							"Pass phrase supplied does not match system pass phrase check, connection disallowed",
							this.mPassphrase, this.mPassphraseFilePath);

			}
		}

	}

	public java.util.Date dStartTime = null;

	/**
	 * Insert the method's description here. Creation date: (5/1/2002 7:56:40
	 * PM)
	 * 
	 * @return boolean
	 * @param pServerID
	 *            int
	 */
	public int shutdownServer(int pServerID) throws SQLException,
			java.lang.Exception {
		PreparedStatement stmt1;
		PreparedStatement stmt2 = null;
		ResultSet m_rs = null;
		int returnStatus = -1;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();
			stmt1 = metadataConnection
					.prepareStatement("SELECT SHUTDOWN_NOW,"
							+ currentTimeStampSyntax
							+ ",STATUS_ID FROM  " + tablePrefix + "SERVER WHERE SERVER_ID = ?"); //$NON-NLS-1$

			stmt1.setInt(1, pServerID);

			m_rs = stmt1.executeQuery();

			stmt2 = metadataConnection
					.prepareStatement("UPDATE  "
							+ tablePrefix
							+ "SERVER SET STATUS_ID = ?,START_TIME = (coalesce(?,START_TIME)),SHUTDOWN_TIME = ?,LAST_PING_TIME = "
							+ currentTimeStampSyntax
							+ ", SHUTDOWN_NOW = NULL WHERE SERVER_ID = ?");

			// cycle through results
			while (m_rs.next()) {
				java.sql.Timestamp td = m_rs.getTimestamp(2);
				int status_id = m_rs.getInt(3);

				String sd = m_rs.getString(1);

				if (this.dStartTime == null) {
					this.dStartTime = new java.util.Date(td.getTime());
				}

				if (m_rs.wasNull() == true) {
					// then check to see if pause was requested
					if (status_id == ETLServerStatus.PAUSED) {
						stmt2.setInt(1, ETLServerStatus.PAUSED);
						returnStatus = ETLServerStatus.PAUSED;
					} else {
						stmt2.setInt(1, ETLServerStatus.SERVER_ALIVE);
						returnStatus = ETLServerStatus.SERVER_ALIVE;
					}

					stmt2.setTimestamp(2, new java.sql.Timestamp(
							this.dStartTime.getTime()));
					stmt2.setNull(3, java.sql.Types.TIMESTAMP);
					stmt2.setInt(4, pServerID);
				} else if (sd.equalsIgnoreCase("Y")) { // must set flag back to
					// null //$NON-NLS-1$
					stmt2.setInt(1, ETLServerStatus.SERVER_SHUTDOWN);
					stmt2.setNull(2, java.sql.Types.DATE);
					stmt2.setTimestamp(3, td);
					stmt2.setInt(4, pServerID);

					returnStatus = ETLServerStatus.SERVER_SHUTTING_DOWN;
				} else if (sd.equalsIgnoreCase("K")) { // must set flag back to
					// null //$NON-NLS-1$
					stmt2.setInt(1, ETLServerStatus.SERVER_KILLED);
					stmt2.setNull(2, java.sql.Types.DATE);
					stmt2.setTimestamp(3, td);
					stmt2.setInt(4, pServerID);
					returnStatus = ETLServerStatus.SERVER_KILLED;
				}

				stmt2.executeUpdate();
				metadataConnection.commit();
			}

			// Close open resources
			if (m_rs != null) {
				m_rs.close();
			}

			if (stmt1 != null) {
				stmt1.close();
			}

			if (stmt2 != null) {
				stmt2.close();
			}
		}

		return (returnStatus);
	}

	/**
	 * Insert the method's description here. Creation date: (5/1/2002 7:56:40
	 * PM)
	 * 
	 * @return boolean
	 * @param pServerID
	 *            int
	 */
	public boolean shutdownServer(String pServerName, boolean bImmediate)
			throws SQLException, java.lang.Exception {
		PreparedStatement stmt1;
		boolean shutdown = false;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();
			stmt1 = metadataConnection.prepareStatement("UPDATE " + tablePrefix
					+ "SERVER set SHUTDOWN_NOW = ? WHERE SERVER_NAME = ?"); //$NON-NLS-1$

			if (bImmediate) {
				stmt1.setString(1, "K");
			} else {
				stmt1.setString(1, "Y");
			}

			stmt1.setString(2, pServerName);

			if (stmt1.executeUpdate() > 0) {
				shutdown = true;
			}

			metadataConnection.commit();

			if (stmt1 != null) {
				stmt1.close();
			}
		}

		return (shutdown);
	}

	/**
	 * Insert the method's description here. Creation date: (5/1/2002 7:56:40
	 * PM)
	 * 
	 * @return boolean
	 * @param pServerID
	 *            int
	 */
	public boolean shutdownServer(int pServerID, boolean bImmediate)
			throws SQLException, java.lang.Exception {
		PreparedStatement stmt1;
		boolean shutdown = false;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();
			stmt1 = metadataConnection.prepareStatement("UPDATE " + tablePrefix
					+ "SERVER set SHUTDOWN_NOW = ? WHERE SERVER_ID = ?"); //$NON-NLS-1$

			if (bImmediate) {
				stmt1.setString(1, "K");
			} else {
				stmt1.setString(1, "Y");
			}

			stmt1.setInt(2, pServerID);

			if (stmt1.executeUpdate() > 0) {
				shutdown = true;
			}

			metadataConnection.commit();

			if (stmt1 != null) {
				stmt1.close();
			}
		}

		return (shutdown);
	}

	/**
	 * Insert the method's description here. Creation date: (5/1/2002 7:56:40
	 * PM)
	 * 
	 * @return boolean
	 * @param pServerID
	 *            int
	 */
	public boolean pauseServer(String pServerName, boolean bState)
			throws SQLException, java.lang.Exception {
		PreparedStatement stmt1;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();
			stmt1 = metadataConnection.prepareStatement("UPDATE " + tablePrefix
					+ "SERVER set STATUS_ID = ? WHERE SERVER_NAME = ?"); //$NON-NLS-1$

			if (bState) {
				stmt1.setInt(1, ETLServerStatus.PAUSED);
			} else {
				stmt1.setInt(1, ETLServerStatus.SERVER_ALIVE);
			}

			stmt1.setString(2, pServerName);

			stmt1.executeUpdate();

			metadataConnection.commit();

			if (stmt1 != null) {
				stmt1.close();
			}
		}

		return (true);
	}

	/**
	 * Insert the method's description here. Creation date: (5/1/2002 7:56:40
	 * PM)
	 * 
	 * @return boolean
	 * @param pServerID
	 *            int
	 */
	public boolean pauseServer(int pServerID, boolean bState)
			throws SQLException, java.lang.Exception {
		PreparedStatement stmt1;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();
			stmt1 = metadataConnection.prepareStatement("UPDATE " + tablePrefix
					+ "SERVER set STATUS_ID = ? WHERE SERVER_ID = ?"); //$NON-NLS-1$

			if (bState) {
				stmt1.setInt(1, ETLServerStatus.PAUSED);
			} else {
				stmt1.setInt(1, ETLServerStatus.SERVER_ALIVE);
			}

			stmt1.setInt(2, pServerID);

			stmt1.executeUpdate();

			metadataConnection.commit();

			if (stmt1 != null) {
				stmt1.close();
			}
		}

		return (true);
	}

	/**
	 * @return Returns the mKETLPath.
	 */
	public static final String getKETLPath() {
		if (mKETLPath == null)
			return ".";
		return mKETLPath;
	}

	/**
	 * @return Returns a new schedule id or -1 if failed.
	 * @author dnguyen 2006-07-27
	 */
	public int scheduleJob(String pJobID, int pMonth, int pMonthOfYear,
			int pDay, int pDayOfWeek, int pDayOfMonth, int pHour,
			int pHourOfDay, int pMinute, int pMinuteOfHour,
			String pDescription, Date pOnceOnlyDate, Date pEnableDate,
			Date pDisableDate) throws SQLException, java.lang.Exception {

		// TODO create a class for the scheduler
		if (pJobID.length() == 0)
			return -1;
		Statement m_stmt_sel = null;
		PreparedStatement m_stmt_add = null;
		ResultSet m_rs = null;

		// TODO finish & test these validations
		// setJobScheduleDefaults(pMonth, pMonthOfYear, pDay, pDayOfWeek,
		// pDayOfMonth, pHour, pHourOfDay, pMinute,
		// pMinuteOfHour, pOnceOnlyDate);
		// TODO add pEnableDate & pDisableDate
		// TODO calc the next_run_date from the pEnableDate, etc.
		if (pOnceOnlyDate != null)
			pMonth = 12; // this is a hack to keep the server from creating
							// infinite loads.

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			// A.make sure this exact schedule has not been created already
			String sql = "SELECT schedule_id FROM " + tablePrefix
					+ "job_schedule WHERE job_id='" + pJobID + "'";
			if (pMonth > 0)
				sql += " AND month=" + pMonth;
			if (pMonthOfYear >= 0 && pMonthOfYear <= 11)
				sql += " AND month_of_year=" + pMonthOfYear; // in the xth
																// month of the
																// year 0-11
			if (pDay > 0)
				sql += " AND day=" + pDay; // every x days
			if (pDayOfWeek >= 1 && pDayOfWeek <= 7)
				sql += " AND day_of_week=" + pDayOfWeek; // 1-7 for Sun-Sat
			// NOTE: front end should test for end of month, leap year, etc.
			if (pDayOfMonth >= 1 && pDayOfMonth <= 31)
				sql += " AND day_of_month=" + pDayOfMonth;
			if (pHour > 0)
				sql += " AND hour=" + pHour; // every x hours
			if (pHourOfDay >= 0 && pHourOfDay <= 23)
				sql += " AND hour_of_day=" + pHourOfDay; // 0-23
			if (pMinute > 0)
				sql += " AND minute=" + pMinute; // every x minutes
			if (pMinuteOfHour >= 0 && pMinuteOfHour <= 59)
				sql += " AND minute_of_hour=" + pMinuteOfHour;// 0-59

			m_stmt_sel = metadataConnection.createStatement();
			m_rs = m_stmt_sel.executeQuery(sql);
			if (m_rs.next()) {
				int sched_id = m_rs.getInt(1);
				ResourcePool.LogMessage(Thread.currentThread().getName(),
						ResourcePool.ERROR_MESSAGE,
						"Duplicate schedule found; existing schedule_id = "
								+ sched_id);
				if (m_rs != null)
					m_rs.close();
				if (m_stmt_sel != null)
					m_stmt_sel.close();
				return -1;
			}
			if (m_rs != null)
				m_rs.close();
			if (m_stmt_sel != null)
				m_stmt_sel.close();

			// B.create a new schedule for this job
			m_stmt_add = metadataConnection
					.prepareStatement("INSERT INTO "
							+ tablePrefix
							+ "job_schedule (schedule_id, job_id, month, month_of_year, day, day_of_week, day_of_month,"
							+ "hour, hour_of_day, minute, minute_of_hour, next_run_date, schedule_desc) "
							+ "SELECT COALESCE(MAX(schedule_id)+1,1), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM "
							+ tablePrefix + "job_schedule ");
			// .first setup the defaults
			m_stmt_add.setString(1, pJobID);
			m_stmt_add.setNull(2, java.sql.Types.INTEGER);
			m_stmt_add.setNull(3, java.sql.Types.INTEGER);
			m_stmt_add.setNull(4, java.sql.Types.INTEGER);
			m_stmt_add.setNull(5, java.sql.Types.INTEGER);
			m_stmt_add.setNull(6, java.sql.Types.INTEGER);
			m_stmt_add.setNull(7, java.sql.Types.INTEGER);
			m_stmt_add.setNull(8, java.sql.Types.INTEGER);
			m_stmt_add.setNull(9, java.sql.Types.INTEGER);
			m_stmt_add.setNull(10, java.sql.Types.INTEGER);
			m_stmt_add.setNull(11, java.sql.Types.TIMESTAMP);
			m_stmt_add.setString(12, "");

			// .second set the option selected -- this maps to java api 1.5
			// Calendar object
			if (pMonth > 0)
				m_stmt_add.setInt(2, pMonth); // every x months
			if (pMonthOfYear >= 0 && pMonthOfYear <= 11)
				m_stmt_add.setInt(3, pMonthOfYear); // in the xth month of the
													// year 0-11
			if (pDay > 0)
				m_stmt_add.setInt(4, pDay); // every x days
			if (pDayOfWeek >= 1 && pDayOfWeek <= 7)
				m_stmt_add.setInt(5, pDayOfWeek); // 1-7 for Sun-Sat
			// NOTE: front end should test for end of month, leap year, etc.
			if (pDayOfMonth >= 1 && pDayOfMonth <= 31)
				m_stmt_add.setInt(6, pDayOfMonth);
			if (pHour > 0)
				m_stmt_add.setInt(7, pHour); // every x hours
			if (pHourOfDay >= 0 && pHourOfDay <= 23)
				m_stmt_add.setInt(8, pHourOfDay); // 0-23
			if (pMinute > 0)
				m_stmt_add.setInt(9, pMinute); // every x minutes
			if (pMinuteOfHour >= 0 && pMinuteOfHour <= 59)
				m_stmt_add.setInt(10, pMinuteOfHour);// 0-59
			if (pOnceOnlyDate != null)
				m_stmt_add.setTimestamp(11, new Timestamp(pOnceOnlyDate
						.getTime()));
			if (pDescription.length() > 0)
				m_stmt_add.setString(12, pDescription);
			m_stmt_add.executeUpdate();
			this.metadataConnection.commit();
			if (m_stmt_add != null)
				m_stmt_add.close();

			// C.get & return the new schedule_id
			try {
				m_rs = m_stmt_sel.executeQuery(sql);
				if (m_rs.next()) {
					int sched_id = m_rs.getInt(1);
					return sched_id;
				}
			} catch (Exception e) {
				ResourcePool.LogMessage(Thread.currentThread().getName(),
						ResourcePool.ERROR_MESSAGE,
						"Unknown exception encountered: " + e.getMessage());
				return -1;
			} finally {
				if (m_rs != null)
					m_rs.close();
				if (m_stmt_sel != null)
					m_stmt_sel.close();
			}

		}
		return -1;
	}

	private void setJobScheduleDefaults(int pMonth, int pMonthOfYear, int pDay,
			int pDayOfWeek, int pDayOfMonth, int pHour, int pHourOfDay,
			int pMinute, int pMinuteOfHour, Date pOnceOnlyDate)
			throws SQLException, java.lang.Exception {

		// TODO move this business logic into a common class

		// NOTE: THIS IS A PLACE HOLDER. THIS METHOD HAS NOT BEEN
		// TESTED/DEBUGGED!!!!!
		// Set increment(s) if needed. This calculates the next run date.
		// One Time: only the next_run_date is set, otherwise do not set this
		// field as the server will calculate it.
		boolean isValidCombo = false;

		// if pMonthOfYear is set (Jan, Feb, Mar, etc.) then the increment is
		// pMonth=12 (every year)
		if (pMonthOfYear >= 0) {
			if (pMonthOfYear > 11)
				throw new Exception(
						"Invalid job schedule value: Month-Of-Year values are 0-11 for Jan-Dec."); // in
			// the
			// xth
			// month
			// of
			// the
			// year
			// 0-11
			if ((pMonthOfYear >= 0) && (pMonth > 0))
				throw new Exception(
						"Invalid job schedule combination: Only the Month-Increment or Month-Of-Year should be set.");
			else
				pMonth = 12;
		}
		// if pDayOfMonth is set (1st, 5th day of the month, etc.) and the month
		// increment is not set, then default to 1
		// (every month)
		if (pDayOfMonth > 0) {
			if ((pDay > 0) || (pDayOfWeek > 0))
				throw new Exception(
						"Invalid job schedule combination: Only the Day-Increment or Day-Of-Month or Day-Of-Week should be set.");
			else if (pDayOfMonth > 31)
				throw new Exception(
						"Invalid job schedule value: Day-Of-Month values are 1-31.");
			else if (pMonth <= 0)
				pMonth = 1; // if (pMonth >= 0) this is fine. Eg.every 3 months
							// on the x day of the month
		}

		// if pDayOfWeek is set (Sun, Mon, etc.), and no month recurrance was
		// set, then this is every week (vs. the
		// first weekday of the month)
		if (pDayOfWeek >= 1) {
			if ((pDay > 0) || (pDayOfMonth > 0))
				throw new Exception(
						"Invalid job schedule combination: Only the Day-Increment or Day-Of-Month or Day-Of-Week should be set.");
			else if (pDayOfWeek > 7)
				throw new Exception(
						"Invalid job schedule value: Day-Of-Week values are 1-7 for Sun-Sat.");
			else if ((pMonth <= 0))
				pDay = 7; // to set weekly job, set Day increment = 7
		}

		// if pHourOfDay is set and no day recurrance was set, then default to
		// every day
		if (pHourOfDay > 0) {
			if (pHourOfDay > 23)
				throw new Exception(
						"Invalid job schedule value: Hour-Of-Day values are 0-23.");
			else if (pHour > 0)
				throw new Exception(
						"Invalid job schedule combination: Only the Hour-Increment or Hour-Of-Day should be set.");
			else if ((pDay <= 0) && (pDayOfMonth <= 0) && (pDayOfWeek <= 0))
				pDay = 1;
		}

		if ((pHour == -1))
			;

		if ((pHour > 0) && (pMinute > 0))
			throw new Exception(
					"Invalid job schedule combination: Only the Hour-Increment or Minute-Increment should be set.");
		// only one increment is allowed for the day part
		if ((pMonth >= 0) && (pDay >= 0))
			throw new Exception(
					"Invalid job schedule combination: Only the Month-Increment or Month-Of-Year or Day-Increment should be set.");

	}

	/**
	 * @author dnguyen 2006-07-27
	 * @return Returns a list of error objects.
	 * @param pStartDate
	 *            Date - return all rows updated since this date
	 * @param pExecID
	 *            int - returns all errors with this execution id
	 * @param maxRows
	 *            int - cap the number of rows to return; -1 is unlimited.
	 */
	public ETLJobError[] getExecutionErrors(java.util.Date pLastModified,
			int pExecID, int maxRows) throws Exception {
		PreparedStatement m_stmt = null;
		ResultSet m_rs = null;
		ArrayList errors = new ArrayList();
		String sql;

		synchronized (this.oLock) {
			// Make metadata connection alive.
			refreshMetadataConnection();

			sql = "SELECT e.dm_load_id, e.job_id, e.message, e.code, e.error_datetime, "
					+ "NULL as details, NULL as step_name FROM "
					+ tablePrefix
					+ "JOB_ERROR e"
					+ " where e.error_datetime >= coalesce(?,e.error_datetime)"
					+ " and e.dm_load_id = coalesce(?,e.dm_load_id)"
					+ " union all "
					+ "SELECT e.dm_load_id, e.job_id, e.message, e.code, e.error_datetime, "
					+ "e.details, e.step_name FROM "
					+ tablePrefix
					+ "JOB_ERROR_HIST e"
					+ " where e.error_datetime >= coalesce(?,e.error_datetime)"
					+ " and e.dm_load_id = coalesce(?,e.dm_load_id)";

			m_stmt = metadataConnection.prepareStatement(sql);
			if (pLastModified == null) {
				m_stmt.setNull(1, Types.TIMESTAMP);
				m_stmt.setNull(3, Types.TIMESTAMP);
			} else {
				m_stmt.setTimestamp(1, new Timestamp(pLastModified.getTime()));
				m_stmt.setTimestamp(3, new Timestamp(pLastModified.getTime()));
			}

			if (pExecID < 0) {
				m_stmt.setNull(2, Types.INTEGER);
				m_stmt.setNull(4, Types.INTEGER);
			} else {
				m_stmt.setInt(2, pExecID);
				m_stmt.setInt(4, pExecID);
			}

			m_stmt.setMaxRows(maxRows);
			m_rs = m_stmt.executeQuery();
			while (m_rs.next()) {
				ETLJobError err = new ETLJobError();
				err.setExecID(m_rs.getInt("dm_load_id"));
				err.setJobID(m_rs.getString("job_id"));
				err.setMessag(m_rs.getString("message"));
				err.setCode(m_rs.getString("code"));
				err.setDate(m_rs.getDate("error_datetime"));
				err.setDetails(m_rs.getString("details"));
				err.setStepName(m_rs.getString("step_name"));
				errors.add(err);
			}
			if (m_rs != null)
				m_rs.close();
			if (m_stmt != null)
				m_stmt.close();

			ETLJobError[] tmp = new ETLJobError[errors.size()];
			errors.toArray(tmp);
			return tmp;
		}
	}

}