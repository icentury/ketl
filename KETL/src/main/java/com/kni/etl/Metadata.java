/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.h2.jdbc.JdbcSQLException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLCluster;
import com.kni.etl.ketl.RecordChecker;
import com.kni.etl.sessionizer.PageParserPageDefinition;
import com.kni.etl.sessionizer.PageParserPageParameter;
import com.kni.etl.sessionizer.SessionDefinition;
import com.kni.etl.sessionizer.SessionIdentifier;
import com.kni.etl.stringtools.DesEncrypter;
import com.kni.etl.util.EncodeBase64;
import com.kni.etl.util.XMLHelper;
import com.kni.util.net.smtp.SMTPClient;
import com.kni.util.net.smtp.SMTPReply;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (3/5/2002 3:13:11 PM)
 * 
 * @author: Kinetic Networks Inc
 */
public class Metadata {

  // private int ActiveJobs = 0;

  // private SimpleDateFormat DateTimeFormatter;

  /** The JDBC driver. */
  private String JDBCDriver;

  /** The JDBCURL. */
  private java.lang.String JDBCURL;

  // private Integer MaxActiveJobs;

  /** The metadata connection. */
  protected Connection metadataConnection;

  /** The Password. */
  private String Password;

  // private PreparedStatement testConnectionStmt = null;

  /** The Username. */
  private String Username;

  /** The o lock. */
  protected Object oLock;

  // Indexes into the parameter list array that is returned...
  /** The Constant PARAMETER_NAME. */
  public static final int PARAMETER_NAME = 0;

  /** The Constant PARAMETER_VALUE. */
  public static final int PARAMETER_VALUE = 1;

  /** The Constant SUB_PARAMETER_LIST_NAME. */
  public static final int SUB_PARAMETER_LIST_NAME = 2;

  /** The Constant SUB_PARAMETER_LIST. */
  public static final int SUB_PARAMETER_LIST = 3;

  /** The table prefix. */
  protected String tablePrefix = "";

  /** The b ansi92 outer join. */
  private boolean bAnsi92OuterJoin = false;

  /** The current time stamp syntax. */
  protected String currentTimeStampSyntax;

  /** The next load ID syntax. */
  private String nextLoadIDSyntax;

  /** The use identity column. */
  private boolean useIdentityColumn;

  /** The next server ID syntax. */
  private String nextServerIDSyntax;

  /** The single row pull syntax. */
  private String singleRowPullSyntax;

  /** The db types. */
  private enum ValidMDDBTypes {
    POSTGRESQL, ORACLE, MYSQL, HSQLDB, H2
  };

  /** The db use identity column. */
  private final boolean[] dbUseIdentityColumn = {false, false, true, false, false};

  /** The db time stamp types. */
  private final String[] dbTimeStampTypes = {"CURRENT_TIMESTAMP", "SYSDATE", "CURRENT_TIMESTAMP",
      "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"};

  /** The db sequence syntax. */
  private final String[] dbSequenceSyntax = {"nextval('#')", "#.NEXTVAL",
      "SELECT LAST_INSERT_ID()", "NEXT VALUE FOR #", "nextval('#')"};

  /** The db sequence syntax. */
  private final String[] dbSecondsBeforeRetry = {
      "a.end_date + ((interval '1' second) * seconds_before_retry)",
      "a.end_date + ((interval '1' second) * seconds_before_retry)",
      "a.end_date + ((interval '1' second) * seconds_before_retry)",
      "dateadd('SECOND',seconds_before_retry,a.end_date)",
      "dateadd('SECOND',seconds_before_retry,a.end_date)"};

  /** The db increment identity column syntax. */
  private final String[] dbIncrementIdentityColumnSyntax = {null, null,
      "UPDATE mysql_sequence SET id=LAST_INSERT_ID(id+1)", null, null};

  /** The db single row pull. */
  private final String[] dbSingleRowPull = {"", " FROM DUAL ", "", "", ""};

  /** The load table name. */
  private final String[] mLoadTableName = {null, null, "root_load", null, null};

  /** The passphrase. */
  private String mPassphrase;

  /** The passphrase file path. */
  private String mPassphraseFilePath = null;

  /** The KETL path. */
  static private String mKETLPath = null;

  /**
   * Sets the KETL path.
   * 
   * @param arg0 the new KETL path
   */
  static public void setKETLPath(String arg0) {
    Metadata.mKETLPath = arg0 == null ? "." : arg0;
  }

  /**
   * Load config file.
   * 
   * @param configFile the config file
   * @param ketlpath the ketlpath
   * @return the document
   */
  static public Document LoadConfigFile(String ketlpath, String configFile) {
    Metadata.setKETLPath(ketlpath);

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
        DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
        builder = dmf.newDocumentBuilder();
        xmlConfig = builder.parse(new InputSource(new StringReader(sb.toString())));
      } catch (org.xml.sax.SAXException e) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
            "Parsing XML document, " + e.toString());

        System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
      } catch (Exception e) {
        ResourcePool.LogException(e, Thread.currentThread());

        System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
      }
    } catch (Exception e) {
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
          "Config file not found or readable, some commands will not be available");
    }

    return xmlConfig;
  }

  /** The Constant CONFIG_FILE. */
  final static public String CONFIG_FILE = "xml" + File.separator + "KETLServers.xml";

  /** The Constant SYSTEM_FILE. */
  final static public String SYSTEM_FILE = "xml" + File.separator + "system.xml";

  /** The encryptor. */
  private DesEncrypter mEncryptor;

  /** The encryption enabled. */
  private boolean mEncryptionEnabled = true;

  /** The inc ident col stmt. */
  private PreparedStatement mIncIdentColStmt = null;

  /** The resolved load table name. */
  private String mResolvedLoadTableName;

  protected String secondsBeforeRetry;

  /**
   * Instantiates a new metadata.
   * 
   * @param pEnableEncryption the enable encryption
   * @throws Exception the exception
   */
  public Metadata(boolean pEnableEncryption) throws Exception {
    this(pEnableEncryption, null);
  }

  /**
   * Metadata constructor comment.
   * 
   * @param pEnableEncryption TODO
   * @param pPassphrase the passphrase
   * @throws Exception the exception
   */
  public Metadata(boolean pEnableEncryption, String pPassphrase) throws Exception {
    super();

    this.mEncryptionEnabled = pEnableEncryption;

    if (this.mEncryptionEnabled) {
      if (pPassphrase == null) {
        this.mPassphrase = "default KETL";

        File fs =
            new File((Metadata.mKETLPath == null ? "" : Metadata.mKETLPath + File.separator)
                + ".ketl_pass");

        if (fs.exists()) {
          FileReader inputFileReader = new FileReader(fs);
          int c;

          StringBuilder sb = new StringBuilder();
          while ((c = inputFileReader.read()) != -1) {
            sb.append((char) c);
          }

          if (sb.length() > 5) {
            this.mPassphrase = sb.toString();
          } else
            throw new Exception("Pass phrase needs to be more than 5 characters");
        } else {
          FileWriter out = new FileWriter(fs);
          java.util.Date dt = new java.util.Date();
          this.mPassphrase = new DesEncrypter(this.mPassphrase).encrypt(dt.toString());
          out.append(this.mPassphrase);
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
      this.mEncryptor = new DesEncrypter(this.mPassphrase);
    }

    // DateTimeFormatter = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
    // MaxActiveJobs = new Integer(5);

    this.oLock = new Object();
  }

  /**
   * Enable encryption.
   * 
   * @param arg0 the arg0
   */
  public void enableEncryption(boolean arg0) {
    this.mEncryptionEnabled = arg0;
  }

  /**
   * Gets the loads.
   * 
   * @param pStartDate the start date
   * @param pLoadID the load ID
   * @return the loads
   * @throws Exception the exception
   */
  public ETLLoad[] getLoads(java.util.Date pStartDate, int pLoadID) throws Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList loads = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT load_id, start_job_id, start_date, project_id, end_date, ignored_parents, failed, 0  FROM  "
                  + this.tablePrefix
                  + this.loadTableName()
                  + " A where start_date >= coalesce(?,start_date) and load_id = coalesce(?,load_id) and load_id in (select load_id from "
                  + this.tablePrefix
                  + "JOB_LOG) union all "
                  + " SELECT load_id, start_job_id, start_date, project_id, end_date, ignored_parents, failed, 1  FROM  "
                  + this.tablePrefix
                  + this.loadTableName()
                  + " A where start_date >= coalesce(?,start_date) and load_id = coalesce(?,load_id) and load_id in (select load_id from "
                  + this.tablePrefix + "JOB_LOG_HIST) order by start_job_id, start_date desc");

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
   * Gets the job loads.
   * 
   * @param pStartDate Date - return all rows updated since this date
   * @param pJobName String - the job in question
   * @return Returns a list of all loads that contain this job.
   * @throws Exception the exception
   * @author dnguyen 2006-07-27
   */
  public ETLLoad[] getJobLoads(java.util.Date pStartDate, String pJobName) throws Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList loads = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // the job_id's are in the JOB_LOG and JOB_LOG_HIST tables; join to
      // LOAD table to get load info
      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT a.load_id, a.start_job_id, a.start_date as load_start_date, a.project_id, "
                  + "a.end_date as load_end_date, a.ignored_parents, a.failed, 0 as is_running, b.dm_load_id FROM  "
                  + this.tablePrefix
                  + this.loadTableName()
                  + " a, "
                  + this.tablePrefix
                  + "JOB_LOG b "
                  + "where a.load_id=b.load_id and b.start_date >= coalesce(?,b.start_date) and job_id = coalesce(?,job_id) "
                  + " union all "
                  + "SELECT a.load_id, a.start_job_id, a.start_date as load_start_date, a.project_id, "
                  + "a.end_date as load_end_date, a.ignored_parents, a.failed, 1 as is_running, b.dm_load_id FROM  "
                  + this.tablePrefix
                  + this.loadTableName()
                  + " a, "
                  + this.tablePrefix
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
   * Gets the load jobs.
   * 
   * @param pStartDate the start date
   * @param pLoadID the load ID
   * @return the job execution details for one execution or the entire load
   * @throws Exception the exception
   * @modified dnguyen 2006-07-27 update getLoadJobs with pExecID to return a single execution
   *           status/details
   */
  public ETLJob[] getLoadJobs(java.util.Date pStartDate, int pLoadID) throws Exception {

    ETLJob[] tmp = this.getExecutionDetails(pStartDate, pLoadID, -1);

    this.populateJobDetails(tmp);

    for (ETLJob j : tmp) {
      j.dependencies = this.getJobDependencies(j.getJobID());
    }

    return tmp;
  }

  /**
   * Gets the execution details.
   * 
   * @param pStartDate the start date
   * @param pLoadID the load ID
   * @param pExecID the exec ID
   * @return the execution details
   * @throws Exception the exception
   */
  public ETLJob[] getExecutionDetails(java.util.Date pStartDate, int pLoadID, int pExecID)
      throws Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList jobs = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // test to see if column exist
      String JOB_LOG_LAST_UPDATE_COL = "start_date";
      if (this.columnExists("JOB_LOG", "LAST_UPDATE_DATE"))
        JOB_LOG_LAST_UPDATE_COL = "LAST_UPDATE_DATE";
      String JOB_LOG_HIST_LAST_UPDATE_COL = "start_date";
      if (this.columnExists("JOB_LOG_HIST", "LAST_UPDATE_DATE"))
        JOB_LOG_HIST_LAST_UPDATE_COL = "LAST_UPDATE_DATE";

      String sql =
          "SELECT  job_id,start_date,status_id,end_date,message,dm_load_id,retry_attempts,execution_date,server_id FROM  "
              + this.tablePrefix
              + "JOB_LOG A where "
              + JOB_LOG_LAST_UPDATE_COL
              + " >= coalesce(?,"
              + JOB_LOG_LAST_UPDATE_COL + ") and load_id = ?";
      if (pExecID > 0)
        sql += " and dm_load_id = ?";
      sql +=
          " union all SELECT  job_id,start_date,status_id,end_date,message,dm_load_id,retry_attempts,execution_date,server_id FROM  "
              + this.tablePrefix
              + "JOB_LOG_HIST A where "
              + JOB_LOG_HIST_LAST_UPDATE_COL
              + " >= coalesce(?," + JOB_LOG_HIST_LAST_UPDATE_COL + ") and load_id = ?";
      if (pExecID > 0)
        sql += " and dm_load_id = ?";

      m_stmt = this.metadataConnection.prepareStatement(sql);

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
        m_stmt.setTimestamp(iDate2, new Timestamp(pStartDate.getTime()));
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

  /**
   * Gets the project jobs.
   * 
   * @param pStartDate the start date
   * @param pProjectID the project ID
   * @return the project jobs
   * @throws Exception the exception
   */
  public ETLJob[] getProjectJobs(java.util.Date pStartDate, int pProjectID) throws Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList jobs = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // test to see if column exist
      String JOB_LOG_LAST_UPDATE_SQL = "";
      if (this.columnExists("JOB", "LAST_UPDATE_DATE"))
        JOB_LOG_LAST_UPDATE_SQL = " and LAST_UPDATE_DATE >= coalesce(?,LAST_UPDATE_DATE) ";

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT  job_id FROM  " + this.tablePrefix
              + "JOB A where project_id = ?" + JOB_LOG_LAST_UPDATE_SQL);

      m_stmt.setInt(1, pProjectID);
      if (JOB_LOG_LAST_UPDATE_SQL.length() > 0)
        if (pStartDate == null)
          m_stmt.setNull(2, Types.TIMESTAMP);
        else
          m_stmt.setTimestamp(2, new Timestamp(pStartDate.getTime()));

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

      for (ETLJob j : tmp) {
        j.dependencies = this.getJobDependencies(j.getJobID());
      }

      return tmp;
    }
  }

  /**
   * Send email.
   * 
   * @param pNew_job_id the new_job_id
   * @param pSubject the subject
   * @param pMessage the message
   * @return true, if successful
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean sendEmail(String pNew_job_id, String pSubject, String pMessage)
      throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    SMTPClient client = new SMTPClient();

    try {
      int reply;

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
          "Attempting to send email, list of recipients follows:");

      m_stmt =
          this.metadataConnection
              .prepareStatement("select disable_alerting,project_id,ACTION from "
                  + this.tablePrefix + "job where job_id = ?");
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

      if ((sAlertingDisabled == null) || (sAlertingDisabled.compareTo("Y") != 0)) {
        m_stmt =
            this.metadataConnection
                .prepareStatement("select hostname,login,pwd,from_address    from  "
                    + this.tablePrefix + "mail_server_detail");
        m_rs = m_stmt.executeQuery();

        String sMailHost = null;
        String sFromAddress = null;

        // String sLogin;
        // String sPWD;
        while (m_rs.next()) {
          sMailHost = m_rs.getString(1);
          sFromAddress = m_rs.getString(4);
        }

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Using mail server: " + sMailHost);
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "From address: " + sFromAddress);

        if (m_stmt != null) {
          m_stmt.close();
        }

        try {
          client.connect(sMailHost);
        } catch (com.kni.util.net.smtp.SMTPConnectionClosedException e) {
          System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

          return false;
        }

        // After connection attempt, you should check the reply code
        // to verify
        // success.
        reply = client.getReplyCode();

        if (!SMTPReply.isPositiveCompletion(reply)) {
          client.disconnect();
          System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

          return false;
        }

        client.login();

        if (sMailHost != null) {

          // After connection attempt, you should check the reply code
          // to verify
          // success.
          m_stmt =
              this.metadataConnection
                  .prepareStatement("SELECT ADDRESS,SUBJECT_PREFIX,MAX_MESSAGE_LENGTH,ADDRESS_NAME FROM  "
                      + this.tablePrefix
                      + "alert_subscription a,  "
                      + this.tablePrefix
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

              client.sendSimpleMessage(sFromAddress, toAddress,
                  "Content-Type: multipart/mixed; boundary=\"DataSeparatorString\"\r\nMIME-Version: 1.0\r\nSubject:"
                      + pSubject + "\r\n" + pMessage);

              ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Recipient: " + toAddress);
            } catch (Exception e) {
              ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
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

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Emails sent");

      // Do useful stuff here.
    } catch (Exception e) {
      ResourcePool
          .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not connect to SMTP server.");
      ResourcePool.LogException(e, this);

      return false;
    }

    return true;
  }

  /**
   * Send email.
   * 
   * @param pNew_job_id the new_job_id
   * @param pSubject the subject
   * @param pMessage the message
   * @return true, if successful
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean sendEmailToAll(String pSubject, String pMessage) throws SQLException,
      java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    SMTPClient client = new SMTPClient();

    try {
      int reply;

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
          "Attempting to send email, list of recipients follows:");

      m_stmt =
          this.metadataConnection
              .prepareStatement("select hostname,login,pwd,from_address    from  "
                  + this.tablePrefix + "mail_server_detail");
      m_rs = m_stmt.executeQuery();

      String sMailHost = null;
      String sFromAddress = null;

      // String sLogin;
      // String sPWD;
      while (m_rs.next()) {
        sMailHost = m_rs.getString(1);
        sFromAddress = m_rs.getString(4);
      }

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Using mail server: " + sMailHost);
      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "From address: " + sFromAddress);

      if (m_stmt != null) {
        m_stmt.close();
      }

      try {
        client.connect(sMailHost);
      } catch (com.kni.util.net.smtp.SMTPConnectionClosedException e) {
        System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

        return false;
      }

      // After connection attempt, you should check the reply code
      // to verify
      // success.
      reply = client.getReplyCode();

      if (!SMTPReply.isPositiveCompletion(reply)) {
        client.disconnect();
        System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

        return false;
      }

      client.login();

      if (sMailHost != null) {

        // After connection attempt, you should check the reply code
        // to verify
        // success.
        m_stmt =
            this.metadataConnection
                .prepareStatement("SELECT ADDRESS,SUBJECT_PREFIX,MAX_MESSAGE_LENGTH,ADDRESS_NAME FROM  "
                    + this.tablePrefix
                    + "alert_subscription a,  "
                    + this.tablePrefix
                    + "alert_address b WHERE a.address_id = b.address_id AND a.all_errors = 'Y'");
        m_rs = m_stmt.executeQuery();

        String[] msgParts = new String[1];

        while (m_rs.next()) {
          try {
            // Create the email message

            int maxMsgLength = m_rs.getInt(3);

            if (maxMsgLength == 0) {
              maxMsgLength = 16096;
            }

            String toAddress = m_rs.getString(1);

            boolean plainTxt = false;

            if (toAddress != null) {
              String[] parts = toAddress.split("@");
              if (parts.length > 2) {
                if (parts[2] != null && !parts[2].equals("HTML"))
                  plainTxt = true;

                toAddress = parts[0] + "@" + parts[1];
              } else
                plainTxt = false;
            }
            msgParts[0] = pMessage;

            client.sendSimpleMessage(
                sFromAddress,
                toAddress,
                "Content-Type: multipart/mixed; boundary=\"DataSeparatorString\"\r\nMIME-Version: 1.0\r\nSubject:"
                    + pSubject
                    + "\r\n"
                    + (plainTxt ? this.getNewTextEmail(msgParts, maxMsgLength) : this
                        .getNewHTMLEmail("", msgParts)));

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Recipient: " + toAddress);
          } catch (Exception e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
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

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Emails sent");

      // Do useful stuff here.
    } catch (Exception e) {
      ResourcePool
          .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not connect to SMTP server.");
      ResourcePool.LogException(e, this);

      return false;
    }

    return true;
  }

  /**
   * Send email.
   * 
   * @param pNew_job_id the new_job_id
   * @param pSubject the subject
   * @param pMessage the message
   * @param maxMsgLength
   * @return true, if successful
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public static boolean sendEmailDirect(String sFromAddress, String toAddress, String sMailHost,
      String pSubject, String pMessage, int maxMsgLength) throws SQLException, java.lang.Exception {
    SMTPClient client = new SMTPClient();

    try {
      int reply;

      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
          "Attempting to send email, list of recipients follows:");

      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
          "Using mail server: " + sMailHost);
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "From address: "
          + sFromAddress);

      try {
        client.connect(sMailHost);
      } catch (com.kni.util.net.smtp.SMTPConnectionClosedException e) {
        System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

        return false;
      }

      // After connection attempt, you should check the reply code
      // to verify
      // success.
      reply = client.getReplyCode();

      if (!SMTPReply.isPositiveCompletion(reply)) {
        client.disconnect();
        System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

        return false;
      }

      client.login();

      if (sMailHost != null) {

        // After connection attempt, you should check the reply code
        // to verify
        // success.

        String[] msgParts = new String[1];

        {
          try {
            // Create the email message

            boolean plainTxt = false;

            if (toAddress != null) {
              String[] parts = toAddress.split("@");
              if (parts.length > 2) {
                if (parts[2] != null && !parts[2].equals("HTML"))
                  plainTxt = true;

                toAddress = parts[0] + "@" + parts[1];
              } else
                plainTxt = false;
            }
            msgParts[0] = pMessage;

            client.sendSimpleMessage(
                sFromAddress,
                toAddress,
                "Content-Type: multipart/mixed; boundary=\"DataSeparatorString\"\r\nMIME-Version: 1.0\r\nSubject:"
                    + pSubject
                    + "\r\n"
                    + (plainTxt ? getNewTextEmail(msgParts, maxMsgLength) : getNewHTMLEmail("",
                        msgParts)));

            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                "Recipient: " + toAddress);
          } catch (Exception e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                "Email send error " + e.toString());
            ResourcePool.LogException(e, Thread.currentThread());

          }
        }

        client.logout();
        client.disconnect();

      }

      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Emails sent");

      // Do useful stuff here.
    } catch (Exception e) {
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
          "Could not connect to SMTP server.");
      ResourcePool.LogException(e, Thread.currentThread());

      return false;
    }

    return true;
  }

  /**
   * Send alert email.
   * 
   * @param pLevel the level
   * @param pJobId the new_job_id
   * @param pCode the new_ CODE
   * @param pExecutionId the new_ D m_ LOA d_ ID
   * @param pLoadId TODO
   * @param pTimestamp the new_ alert_ timestamp
   * @param pMessage the new_ MESSAGE
   * @param pExtendedMessage the new_ EXTENDE d_ MESSAGE
   * @param pAttachment the attachment
   * @return true, if successful
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean sendAlertEmail(int pLevel, String pJobId, String pCode, int pExecutionId,
      int pLoadId, java.util.Date pTimestamp, String pMessage, String pExtendedMessage,
      String pAttachment) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    SMTPClient client = new SMTPClient();

    try {
      int reply;

      String levelStr =
          ResourcePool.getAlertLevelName(pLevel).charAt(0)
              + ResourcePool.getAlertLevelName(pLevel).toLowerCase().substring(1);
      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
          "Attempting to send email, list of recipients follows:");

      m_stmt =
          this.metadataConnection
              .prepareStatement("select disable_alerting,project_id,ACTION from "
                  + this.tablePrefix + "job where job_id = ?");
      m_stmt.setString(1, pJobId);
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

      if ((sAlertingDisabled == null) || (sAlertingDisabled.compareTo("Y") != 0)) {
        m_stmt =
            this.metadataConnection
                .prepareStatement("select hostname,login,pwd,from_address    from  "
                    + this.tablePrefix + "mail_server_detail");
        m_rs = m_stmt.executeQuery();

        String sMailHost = null;
        String sFromAddress = null;

        // String sLogin;
        // String sPWD;
        while (m_rs.next()) {
          sMailHost = m_rs.getString(1);
          sFromAddress = m_rs.getString(4);
        }

        if (sMailHost == null) {
          return false;
        }

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Using mail server: " + sMailHost);
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "From address: " + sFromAddress);

        if (m_stmt != null) {
          m_stmt.close();
        }

        try {
          client.connect(sMailHost);
        } catch (com.kni.util.net.smtp.SMTPConnectionClosedException e) {
          System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

          return false;
        }

        // After connection attempt, you should check the reply code
        // to verify
        // success.
        reply = client.getReplyCode();

        if (!SMTPReply.isPositiveCompletion(reply)) {
          client.disconnect();
          System.err.println("[" + new java.util.Date() + "] SMTP server refused connection.");

          return false;
        }

        client.login();

        if (sMailHost != null) {

          // After connection attempt, you should check the reply code
          // to verify
          // success.
          m_stmt =
              this.metadataConnection
                  .prepareStatement("SELECT ADDRESS,SUBJECT_PREFIX,MAX_MESSAGE_LENGTH,ADDRESS_NAME FROM  "
                      + this.tablePrefix
                      + "alert_subscription a,  "
                      + this.tablePrefix
                      + "alert_address b WHERE a.address_id = b.address_id AND (a.job_id = ? OR a.project_id = ? OR a.all_errors = 'Y')");
          m_stmt.setString(1, pJobId);
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

              if ((subjPrefix == null) || (subjPrefix.length() <= 1)) {
                subjPrefix = "";
              } else {
                subjPrefix = subjPrefix + " - ";
              }

              String toAddress = m_rs.getString(1);
              boolean plainTxt = false;

              if (toAddress != null) {
                String[] parts = toAddress.split("@");
                if (parts.length > 2) {
                  if (parts[2] != null && !parts[2].equals("HTML"))
                    plainTxt = true;

                  toAddress = parts[0] + "@" + parts[1];
                } else
                  plainTxt = false;
              }

              if (plainTxt) {
                msgParts[0] = levelStr + " Message:\t" + pMessage + "\n\n";
                msgParts[1] =
                    "Job ID:\t\t" + pJobId + " - [Load ID:" + pLoadId + ", Execution ID:"
                        + pExecutionId + "]\n\n";
                msgParts[2] = levelStr + " Date Time:\t" + pTimestamp + "\n\n";
                msgParts[3] = levelStr + " Code:\t\t" + pCode + "\n\n";
                msgParts[4] = "Extended Message:\t" + pExtendedMessage + "\n\n";
              } else {
                msgParts[0] = pMessage;
                msgParts[1] =
                    pJobId + " - Load ID:" + pLoadId + ", Execution ID:" + pExecutionId + "]";
                msgParts[2] = pTimestamp.toString();
                msgParts[3] = pCode;
                msgParts[4] = pExtendedMessage;
              }

              client
                  .sendSimpleMessage(
                      sFromAddress,
                      toAddress,
                      "Content-Type: multipart/mixed; boundary=\"DataSeparatorString\"\r\nMIME-Version: 1.0\r\nSubject:"
                          + subjPrefix
                          + "Job "
                          + levelStr
                          + " ("
                          + pJobId
                          + ")\r\n"
                          + (plainTxt ? this.getNewTextEmail(msgParts, maxMsgLength) : this
                              .getNewHTMLEmail(levelStr, msgParts))
                          + (pAttachment != null && new File(pAttachment).exists() ? "\r\n--DataSeparatorString\r\nContent-Disposition: attachment;filename=\"trace.log\"\r\nContent-transfer-encoding: base64\r\n\r\n"
                              + EncodeBase64.encode(pAttachment) + "\r\n\r\n"
                              : "") + "\r\n--DataSeparatorString--");

              ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Recipient: " + toAddress);
            } catch (Exception e) {
              ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Email send " + levelStr
                  + e.toString());
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

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Emails sent");

      // Do useful stuff here.
    } catch (Exception e) {
      ResourcePool
          .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not connect to SMTP server.");
      ResourcePool.LogException(e, this);

      return false;
    }

    return true;
  }

  /**
   * Gets the new HTML email.
   * 
   * @param level the level
   * @param msgParts the msg parts
   * @return the new HTML email
   */
  private static String getNewHTMLEmail(String level, String[] msgParts) {
    return getMessageAsHTML(level, msgParts);

  }

  /**
   * Write HTML row.
   * 
   * @param field the field
   * @param value the value
   * @return the string
   */
  private static String writeHTMLRow(String field, String value) {
    return "<tr><td width=\"9%\" class=\"row\">" + field + "</td><td width=\"90%\" class=\"row\">"
        + value + "</td></tr>";
  }

  /**
   * Gets the new text email.
   * 
   * @param msgParts the msg parts
   * @param maxMsgLength the max msg length
   * @return the new text email
   */
  private static String getNewTextEmail(String[] msgParts, int maxMsgLength) {

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

  /**
   * Gets the message as HTML.
   * 
   * @param level the level
   * @param msgParts the msg parts
   * @return the message as HTML
   */
  private static String getMessageAsHTML(String level, String[] msgParts) {

    StringBuilder sb = new StringBuilder();
    sb.append("--DataSeparatorString\r\nContent-Type: text/html; charset=\"us-ascii\"\r\n\r\n<html><head><meta http-equiv=\"Content-Language\" content=\"en-us\"><meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1252\"><style>\n<!--\n.tbl { border-left-style: none; border-right-style: none;border-top: 1.5pt solid green; border-bottom: 1.5pt solid green }\n.ms-simple1-tl { border-left-style: none; border-right-style: none; border-top-style: none;border-bottom: .75pt solid green }\n.row {  border-left-style: none; border-right-style: none; border-top-style: none;border-bottom: .75pt solid green;font-size: 9pt }--></style></head><body><font face=\"Arial\"><b>KETL "
        + level + "</b><table border=\"0\" width=\"100%\" id=\"table1\" class=\"tbl\">\r\n");

    if (msgParts.length > 0)
      sb.append(writeHTMLRow(level + " Message", msgParts[0]));
    if (msgParts.length > 1)
      sb.append(writeHTMLRow("Job ID", msgParts[1]));
    if (msgParts.length > 2)
      sb.append(writeHTMLRow("Datetime", msgParts[2]));
    if (msgParts.length > 3)
      sb.append(writeHTMLRow(level + " Code", msgParts[3]));
    if (msgParts.length > 4)
      sb.append(writeHTMLRow("Extended Message",
          msgParts[4] == null ? "NULL" : msgParts[4].replace("\n", "<br/>")));

    return sb.toString();
  }

  /**
   * Insert the method's description here. Creation date: (9/9/2002 11:23:23 AM)
   * 
   * @param pJobDependencies java.lang.String[][]
   * @param pJobID java.lang.String
   * @param pFinalJobList the final job list
   * @return the child jobs
   */
  private static String getChildJobs(String[][] pJobDependencies, String pJobID,
      ArrayList pFinalJobList) {
    int i = 0;
    String bHasChildren = "N";

    if (pJobDependencies == null) {
      return bHasChildren;
    }

    // sJobDependencies holds all dependencies for project
    // recursively cycle through deps now
    for (i = 0; i < pJobDependencies.length; i++) {
      if ((pJobDependencies[i][1] != null) && (pJobDependencies[i][1].compareTo(pJobID) == 0)) {
        bHasChildren = "Y";
        pJobDependencies[i][1] = null;
        Metadata.getChildJobs(pJobDependencies, pJobDependencies[i][0], pFinalJobList);
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
   * Insert the method's description here. Creation date: (3/8/2002 11:47:27 AM)
   * 
   * @param pMonth int
   * @param pMonthOfYear int
   * @param pDay int
   * @param pDayOfWeek int
   * @param pDayOfMonth int
   * @param pHour int
   * @param pHourOfDay int
   * @param pCurrentDate the current date
   * @param pMinute the minute
   * @param pMinuteOfHour the minute of hour
   * @return java.sql.Date
   */
  protected static java.util.Date getNextDate(java.util.Date pCurrentDate, int pMonth,
      int pMonthOfYear, int pDay, int pDayOfWeek, int pDayOfMonth, int pHour, int pHourOfDay,
      int pMinute, int pMinuteOfHour) {
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
   * Insert the method's description here. Creation date: (5/1/2002 8:03:36 PM)
   */
  public void closeMetadata() {
    synchronized (this.oLock) {
      try {
        this.columnExists.clear();
        if (this.metadataConnection == null) {
          return;
        }

        try {
          if (this.mIncIdentColStmt != null) {
            try {
              this.mIncIdentColStmt.close();
            } catch (Exception e) {
              ResourcePool.LogMessage(e);
            }
          }

          this.metadataConnection.rollback();
          this.metadataConnection.close();
          this.metadataConnection = null;

        } catch (SQLException ee) {
          this.metadataConnection = null;
          ResourcePool.logMessage(ee);
        }
      } finally {

        if (h2Server != null) {
          h2Server.stop();
          h2Server = null;
        }
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (6/6/2002 9:29:56 PM)
   * 
   * @param pIgnoreDependencies boolean
   * @param pProjectID the project ID
   * @param pJobID the job ID
   * @return boolean
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean executeJob(int pProjectID, String pJobID, boolean pIgnoreDependencies)
      throws SQLException, java.lang.Exception {
    return this.executeJob(pProjectID, pJobID, pIgnoreDependencies, false) != -1;
  }

  /**
   * Gets the cluster details.
   * 
   * @return the cluster details
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public KETLCluster getClusterDetails() throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    KETLCluster kc = new KETLCluster();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("select a.server_id,server_name,status_desc,last_ping_time,start_time,c.description,threads, "
                  + this.currentTimeStampSyntax
                  + ",a.pool from "
                  + this.tablePrefix
                  + "server_executor a, "
                  + this.tablePrefix
                  + "job_executor_job_type b, "
                  + this.tablePrefix
                  + "job_type c, "
                  + this.tablePrefix
                  + "server d, "
                  + this.tablePrefix
                  + "server_status e "
                  + " where a.job_executor_id = b.job_executor_id "
                  + " and b.job_type_id = c.job_type_id "
                  + "  and d.status_id = e.status_id "
                  + "  and a.server_id = d.server_id " + " order by server_name,c.description  ");
      m_rs = m_stmt.executeQuery();

      while (m_rs.next()) {
        try {
          int serverID = m_rs.getInt(1);
          kc.addServer(serverID, m_rs.getString(2), m_rs.getTimestamp(5), m_rs.getTimestamp(4),
              m_rs.getString(3), m_rs.getTimestamp(8));
          kc.addExecutor(serverID, m_rs.getString(6), m_rs.getInt(7), m_rs.getString(9));
        } catch (Exception e) {
          e.printStackTrace();
          ResourcePool.LogMessage("Error getting server states: " + e.getMessage());

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

      m_stmt =
          this.metadataConnection
              .prepareStatement("select server_id,c.description,status_desc,pool,count(*) "
                  + " from " + this.tablePrefix + "job_log a , " + this.tablePrefix
                  + "job_status b,  " + " " + this.tablePrefix + "job_type c, " + this.tablePrefix
                  + "job d " + " where a.status_id = b.status_id " + " and a.job_id = d.job_id "
                  + " and c.job_type_id = d.job_type_id "
                  + " group by server_id,status_desc,c.description,pool");
      m_rs = m_stmt.executeQuery();

      while (m_rs.next()) {
        try {
          int serverId = m_rs.getInt(1);

          if (m_rs.wasNull()) {
            serverId = -1;
          }

          String srv = m_rs.getString(4);
          srv = srv == null ? EngineConstants.DEFAULT_POOL : srv;
          kc.addExecutorState(serverId, m_rs.getString(2), m_rs.getString(3), m_rs.getInt(5), srv);
        } catch (Exception e) {
          ResourcePool.LogMessage("Error getting executor state: " + e.getMessage());
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
   * Gets the cluster details.
   * 
   * @return the cluster details
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public List<KETLCluster.Server> getAliveServers() throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    List<KETLCluster.Server> servers = new ArrayList<KETLCluster.Server>();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("select server_id,server_name,start_time,last_ping_time, "
                  + this.currentTimeStampSyntax + " from " + this.tablePrefix + "server "
                  + " where  status_id = 1 " + " order by server_name ");
      m_rs = m_stmt.executeQuery();

      while (m_rs.next()) {
        servers.add(new KETLCluster().newServer(m_rs.getInt(1), m_rs.getString(2),
            m_rs.getTimestamp(3), m_rs.getTimestamp(4), "Alive", m_rs.getTimestamp(5)));
      }

      // Close open resources
      if (m_rs != null) {
        m_rs.close();
      }

      if (m_stmt != null) {
        m_stmt.close();
      }

    }

    return servers;
  }

  /**
   * Insert the method's description here. Creation date: (6/6/2002 9:29:56 PM)
   * 
   * @param pIgnoreDependencies boolean
   * @param pAllowMultiple boolean
   * @param pProjectID the project ID
   * @param pJobID the job ID
   * @return load id or -1 if not executed
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public int executeJob(int pProjectID, String pJobID, boolean pIgnoreDependencies,
      boolean pAllowMultiple) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet rs = null;
    ResultSet m_rs = null;
    int loadID = -1;

    synchronized (this.oLock) {

      ETLJob j = this.getJob(pJobID);

      if (j == null || j.getProjectID() != pProjectID) {
        throw new Exception("Job " + pJobID + " does not exist or does not exist in project "
            + pProjectID);
      }

      ETLStatus etlJobStatus = new ETLJobStatus();

      // Make metadata connection alive.
      this.refreshMetadataConnection();

      if (pAllowMultiple == false) {
        m_stmt =
            this.metadataConnection.prepareStatement("SELECT COUNT(*) FROM  " + this.tablePrefix
                + "JOB_LOG WHERE JOB_ID = ?");
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
          ResourcePool.LogMessage(j, -1, ResourcePool.ERROR_MESSAGE, "Job " + pJobID
              + " already in job queue, multiple executions of job disallowed.",
              "To resolve this problem closeout the currently blocking job, "
                  + "blocking job maybe a prior load failure.", Thread.currentThread().getName()
                  .equalsIgnoreCase("ETLDaemon") ? true : false);

          return -1;
        }

      }

      // get next load_id
      m_stmt =
          this.metadataConnection.prepareStatement(this.useIdentityColumn ? this.nextLoadIDSyntax
              : ("SELECT " + this.nextLoadIDSyntax + this.singleRowPullSyntax + ""));
      m_rs = m_stmt.executeQuery();

      String jobID = pJobID;

      PreparedStatement newLoadStmt =
          this.metadataConnection.prepareStatement("INSERT INTO  " + this.tablePrefix
              + this.loadTableName() + "(LOAD_ID,START_JOB_ID,START_DATE,PROJECT_ID) VALUES(?,?,"
              + this.currentTimeStampSyntax + ",?)");
      PreparedStatement dependenciesStmt =
          this.metadataConnection
              .prepareStatement("SELECT A.JOB_ID, PARENT_JOB_ID, ALLOW_DUPLICATES, B.JOB_ID FROM  "
                  + this.tablePrefix
                  + "JOB_DEPENDENCIE A LEFT OUTER JOIN (SELECT DISTINCT JOB_ID FROM "
                  + this.tablePrefix
                  + "JOB_LOG) B ON (A.JOB_ID = B.JOB_ID) WHERE A.JOB_ID IN (SELECT JOB_ID FROM  "
                  + this.tablePrefix
                  + "JOB WHERE PROJECT_ID = ? )  AND PARENT_JOB_ID IN (SELECT JOB_ID FROM  "
                  + this.tablePrefix + "JOB  WHERE PROJECT_ID = ?)");
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
            boolean duplicatesAllowed =
                rs.getString(3) == null ? false : rs.getString(3).equalsIgnoreCase("Y");

            if (!duplicatesAllowed && rs.getString(4) != null) {
              ResourcePool.logMessage("Skipping dependence, due to duplicate. Skipped job: "
                  + rs.getString(1));
              continue;
            }
            if (sJobDependencies == null) {
              sJobDependencies = new String[i + 1][2];
              sJobDependencies[i][0] = rs.getString(1);
              sJobDependencies[i][1] = rs.getString(2);
            } else {
              i++;

              String[][] tmp = new String[i + 1][2];
              tmp[i][0] = rs.getString(1);
              tmp[i][1] = rs.getString(2);

              System.arraycopy(sJobDependencies, 0, tmp, 0, sJobDependencies.length);
              sJobDependencies = tmp;
            }
          }

          // sJobDependencies holds all dependencies for project
          // recursively cycle through deps now
          ArrayList aFinalJobList = new ArrayList();

          Metadata.getChildJobs(sJobDependencies, pJobID, aFinalJobList);

          int iStatus;
          String sStatusMessage;

          if (insJobsStmt == null) {
            insJobsStmt =
                this.metadataConnection
                    .prepareStatement("INSERT INTO  "
                        + this.tablePrefix
                        + "JOB_LOG(JOB_ID,LOAD_ID,STATUS_ID,START_DATE,MESSAGE,DM_LOAD_ID) SELECT JOB_ID,?,?,"
                        + this.currentTimeStampSyntax + ",?,"
                        + (this.useIdentityColumn ? "null" : this.nextLoadIDSyntax) + " FROM  "
                        + this.tablePrefix + "job where job_id = ?");
          }

          for (i = 0; i < aFinalJobList.size(); i++) {
            String[] sJobID = (String[]) aFinalJobList.get(i);

            if (sJobID != null) {
              if (sJobID[1].compareTo("Y") == 0) {
                iStatus = ETLJobStatus.WAITING_FOR_CHILDREN;
                sStatusMessage =
                    etlJobStatus.getStatusMessageForCode(ETLJobStatus.WAITING_FOR_CHILDREN);
              } else {
                iStatus = ETLJobStatus.READY_TO_RUN;
                sStatusMessage = etlJobStatus.getStatusMessageForCode(ETLJobStatus.READY_TO_RUN);
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
          insNoDepJobStmt =
              this.metadataConnection
                  .prepareStatement("INSERT INTO  "
                      + this.tablePrefix
                      + "JOB_LOG(JOB_ID,LOAD_ID,STATUS_ID,START_DATE,MESSAGE,DM_LOAD_ID) SELECT JOB_ID,?,?,"
                      + this.currentTimeStampSyntax + ",?, "
                      + (this.useIdentityColumn ? "null" : this.nextLoadIDSyntax) + "  FROM  "
                      + this.tablePrefix + "JOB where not exists (select 1 from  "
                      + this.tablePrefix
                      + "job_log where load_id = ? AND job_id = ?) AND JOB_ID = ?");
        }

        insNoDepJobStmt.setInt(1, loadID);
        insNoDepJobStmt.setInt(2, ETLJobStatus.READY_TO_RUN);
        insNoDepJobStmt.setString(3,
            etlJobStatus.getStatusMessageForCode(ETLJobStatus.READY_TO_RUN));
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

    return loadID;
  }

  /**
   * Gets the job.
   * 
   * @param pJobID the job ID
   * @return the job
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public ETLJob getJob(String pJobID) throws SQLException, java.lang.Exception {
    return this.getJob(pJobID, 0, 0);
  }

  /**
   * Populate job details.
   * 
   * @param pJobs the jobs
   * @return the ETL job[]
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public ETLJob[] populateJobDetails(ETLJob[] pJobs) throws SQLException, java.lang.Exception {
    ETLJob newETLJob = null;
    Statement m_stmt = null;
    ResultSet m_rs = null;

    synchronized (this.oLock) {

      int i = 0;
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt = this.metadataConnection.createStatement();
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

        m_rs =
            m_stmt
                .executeQuery("SELECT CLASS_NAME,PROJECT_ID,JOB_ID,ACTION,NAME,RETRY_ATTEMPTS,PARAMETER_LIST_ID,"
                    + "A.JOB_TYPE_ID,a.DESCRIPTION,b.DESCRIPTION,DISABLE_ALERTING,SECONDS_BEFORE_RETRY FROM  "
                    + this.tablePrefix
                    + "JOB A,  "
                    + this.tablePrefix
                    + "JOB_TYPE B WHERE A.JOB_TYPE_ID = B.JOB_TYPE_ID AND JOB_ID in ("
                    + sb.toString() + ")");

        // cycle through pending jobs setting next run date
        while (m_rs.next()) {
          try {
            ETLJob curETLJob = pJobs[(Integer) hmJobs.get(m_rs.getString(3))];
            // if class was null then it is a default job and should
            // just be passed
            String jobClass = m_rs.getString(1);

            if (m_rs.wasNull() == true) {
              newETLJob = new ETLJob();
            } else {
              newETLJob = (ETLJob) Class.forName(jobClass).newInstance();
            }

            pJobs[(Integer) hmJobs.get(m_rs.getString(3))] = newETLJob;

            // transfer data
            newETLJob.setJobExecutionID(curETLJob.getJobExecutionID());
            newETLJob.setLoadID(curETLJob.iLoadID);
            newETLJob.jsStatus = curETLJob.jsStatus;
            newETLJob.setRetryAttempts(curETLJob.getRetryAttempts());

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
            ResourcePool.logMessage("Error creating job: " + e.getMessage());
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
   * Insert the method's description here. Creation date: (5/8/2002 2:01:06 PM)
   * 
   * @param pJobID the job ID
   * @param pLoadID the load ID
   * @param pExecutionID the execution ID
   * @return com.kni.etl.ETLJob
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public ETLJob getJob(String pJobID, int pLoadID, int pExecutionID) throws SQLException,
      java.lang.Exception {
    ETLJob newETLJob = null;
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    int iParameterListID = 0;

    ResourcePool.logMessage(pJobID);

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT CLASS_NAME,PROJECT_ID,JOB_ID,ACTION,NAME,RETRY_ATTEMPTS,PARAMETER_LIST_ID,"
                  + "A.JOB_TYPE_ID,a.DESCRIPTION,b.DESCRIPTION,DISABLE_ALERTING,SECONDS_BEFORE_RETRY,A.POOL,A.PRIORITY FROM  "
                  + this.tablePrefix
                  + "JOB A,  "
                  + this.tablePrefix
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
            newETLJob = (ETLJob) Class.forName(jobClass).newInstance();
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
          newETLJob.setPool(m_rs.getString(13));
          newETLJob.setPriority(m_rs.getInt(14));
          if (m_rs.wasNull())
            newETLJob.setPriority(EngineConstants.DEFAULT_PRIORITY);
          if ((x != null) && x.equalsIgnoreCase("Y")) {
            newETLJob.setDisableAlerting(true);
          }

          // get parameter list id
          iParameterListID = m_rs.getInt(7);

          if (m_rs.wasNull() == false) {
            newETLJob.setGlobalParameterListID(iParameterListID);
          }
        } catch (Exception e) {
          ResourcePool.logMessage("Error creating job: " + e.getMessage());
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

  /**
   * Gets the metadata version.
   * 
   * @return the metadata version
   */
  public String getMetadataVersion() {
    return "1.0";
  }

  /**
   * Gets the job status.
   * 
   * @param pJob the job
   * @return the job status
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public void getJobStatus(ETLJob pJob) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT STATUS_ID,DM_LOAD_ID,LOAD_ID FROM  "
              + this.tablePrefix + "JOB_LOG A " + "WHERE JOB_ID like ?");
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

  /**
   * Gets the job status by execution id.
   * 
   * @param pLoadID the load ID
   * @return the job status by execution id
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public int getJobStatusByExecutionId(int pLoadID) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT STATUS_ID FROM  " + this.tablePrefix
              + "JOB_LOG A " + "WHERE DM_LOAD_ID = ?");
      m_stmt.setInt(1, pLoadID);
      m_rs = m_stmt.executeQuery();

      Integer status_id = null;
      // cycle through pending jobs setting next run date
      while (m_rs.next()) {
        status_id = m_rs.getInt(1);
      }

      // Close open resources
      if (m_rs != null) {
        m_rs.close();
      }

      if (m_stmt != null) {
        m_stmt.close();
      }

      if (status_id == null) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
            "Job not in job_log table, issuing cancel for load id = " + pLoadID
                + ", restart of server recommended");
        return ETLJobStatus.FATAL_STATE;
      }
      return status_id.intValue();
    }

  }

  /**
   * Gets the job status by execution id.
   * 
   * @param pLoadID the load ID
   * @return the job status by execution id
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public int getJobExecutionIdByLoadId(String pJobId, int pLoadID) throws SQLException,
      java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT DM_LOAD_ID FROM  " + this.tablePrefix
              + "JOB_LOG A " + "WHERE LOAD_ID = ? AND JOB_ID = ?");
      m_stmt.setInt(1, pLoadID);
      m_stmt.setString(2, pJobId);
      m_rs = m_stmt.executeQuery();

      Integer executionId = null;
      // cycle through pending jobs setting next run date
      while (m_rs.next()) {
        executionId = m_rs.getInt(1);
      }

      // Close open resources
      if (m_rs != null) {
        m_rs.close();
      }

      if (m_stmt != null) {
        m_stmt.close();
      }

      if (executionId == null) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
            "Job not in job_log table, issuing cancel for load id = " + pLoadID
                + ", restart of server recommended");
        return ETLJobStatus.FATAL_STATE;
      }
      return executionId.intValue();
    }

  }

  /**
   * Gets the detailed job status.
   * 
   * @param pJobID the job ID
   * @param pLoadID the load ID
   * @param pExecutionID the execution ID
   * @return the detailed job status
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public ETLJob getDetailedJobStatus(String pJobID, int pLoadID, int pExecutionID)
      throws SQLException, java.lang.Exception {
    ETLJob newETLJob = null;
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    int iParameterListID = 0;

    ResourcePool.logMessage(pJobID);

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT CLASS_NAME,PROJECT_ID,JOB_ID,ACTION,NAME,RETRY_ATTEMPTS,PARAMETER_LIST_ID,"
                  + "A.JOB_TYPE_ID,a.DESCRIPTION,b.DESCRIPTION,DISABLE_ALERTING,SECONDS_BEFORE_RETRY FROM  "
                  + this.tablePrefix
                  + "JOB A,  "
                  + this.tablePrefix
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
            newETLJob = (ETLJob) Class.forName(jobClass).newInstance();
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
          ResourcePool.logMessage("Error creating job: " + e.getMessage());
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

  /**
   * Gets the job details.
   * 
   * @param pJobID the job ID
   * @return the job details
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public ETLJob[] getJobDetails(String pJobID) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList jobsToFetch = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT JOB_ID FROM  " + this.tablePrefix
              + "JOB A " + "WHERE JOB_ID like ?");
      m_stmt.setString(1, pJobID);
      m_rs = m_stmt.executeQuery();

      // cycle through pending jobs setting next run date
      while (m_rs.next()) {
        try {
          jobsToFetch.add(m_rs.getString(1));
        } catch (Exception e) {
          ResourcePool.logMessage("Error creating job: " + e.getMessage());

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
      jobs[i] = this.getJob((String) jobsToFetch.get(i));
    }

    return (jobs);
  }

  /**
   * Gets the jobs by status.
   * 
   * @param pStatus the status
   * @return the jobs by status
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public Object[][] getJobsByStatus(int pStatus) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList jobsToFetch = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("select a.job_id,d.description,start_date,execution_date,end_date,b.server_name,message,a.load_id,a.dm_load_id "
                  + " from "
                  + this.tablePrefix
                  + "job_log a, "
                  + this.tablePrefix
                  + "server b, "
                  + this.tablePrefix
                  + "job_type d,"
                  + this.tablePrefix
                  + "job e "
                  + " where a.status_id = ? "
                  + " and a.job_id = e.job_id "
                  + " and d.job_type_id = e.job_type_id "
                  + " and a.server_id = b.server_id ORDER by a.job_id");
      m_stmt.setInt(1, pStatus);
      m_rs = m_stmt.executeQuery();

      jobsToFetch.add(new Object[] {"Job", "Type", "Start Date", "Exec Date", "End Date",
          "Load ID", "Exec ID", "Server", "Message"});
      // cycle through pending jobs setting next run date
      while (m_rs.next()) {
        try {
          Object[] s = new Object[9];
          s[0] = m_rs.getString(1);
          s[1] = m_rs.getString(2);
          s[2] = m_rs.getTimestamp(3);
          s[3] = m_rs.getTimestamp(4);
          s[4] = m_rs.getTimestamp(5);
          s[5] = m_rs.getInt(8);
          s[6] = m_rs.getInt(9);
          s[7] = m_rs.getString(6);
          s[8] = m_rs.getString(7);
          jobsToFetch.add(s);
        } catch (Exception e) {
          ResourcePool.logMessage("Error creating job: " + e.getMessage());

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

      m_stmt =
          this.metadataConnection
              .prepareStatement("select a.job_id,d.description,start_date,execution_date,end_date,message,a.load_id,a.dm_load_id "
                  + " from "
                  + this.tablePrefix
                  + "job_log a,  "
                  + this.tablePrefix
                  + "job_type d,"
                  + this.tablePrefix
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
          Object[] s = new Object[9];
          s[0] = m_rs.getString(1);
          s[1] = m_rs.getString(2);
          s[2] = m_rs.getTimestamp(3);
          s[3] = m_rs.getTimestamp(4);
          s[4] = m_rs.getTimestamp(5);
          s[5] = m_rs.getInt(7);
          s[6] = m_rs.getInt(8);
          s[7] = "Not assigned";
          s[8] = m_rs.getString(6);

          jobsToFetch.add(s);
        } catch (Exception e) {
          ResourcePool.logMessage("Error creating job: " + e.getMessage());

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

    if (jobsToFetch.size() == 1)
      jobsToFetch.clear();

    Object[][] jobs = new Object[jobsToFetch.size()][];

    for (int i = 0; i < jobs.length; i++) {
      jobs[i] = (Object[]) jobsToFetch.get(i);
    }

    return (jobs);
  }

  public void recoverServerJobs(int serverID) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("update " + this.tablePrefix
              + "job_log set status_id = " + ETLJobStatus.PENDING_CLOSURE_FAILED
              + ", message = 'Failed due to server failure' where status_id = "
              + ETLJobStatus.EXECUTING + " and server_id = ?");
      m_stmt.setInt(1, serverID);
      m_stmt.executeUpdate();

      m_stmt.getConnection().commit();

      if (m_stmt != null) {
        m_stmt.close();
      }

      if (m_stmt != null) {
        m_stmt.close();
      }
    }

  }

  /** The Constant WAITS_ON. */
  public static final String WAITS_ON = "Y";

  /** The Constant DEPENDS_ON. */
  public static final String DEPENDS_ON = "N";

  /**
   * Import job.
   * 
   * @param pJob the job
   * @return true, if successful
   * @throws Exception the exception
   */
  public boolean importJob(Node pJob) throws Exception {
    PreparedStatement m_jobStmt = null;
    PreparedStatement m_updStmt = null;
    PreparedStatement m_deps = null;
    PreparedStatement m_depDel = null, m_depSingleDel;
    String mProject;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_jobStmt =
          this.metadataConnection.prepareStatement("insert into " + this.tablePrefix
              + "job(job_id,job_type_id,project_id,parameter_list_id,name,"
              + "description,retry_attempts,seconds_before_retry,disable_alerting,"
              + "action,pool,priority) values(?,?,?,?,?,?,?,?,?,?,?,?)");

      m_updStmt =
          this.metadataConnection.prepareStatement("update " + this.tablePrefix
              + "job set job_type_id = ?,project_id = ?,parameter_list_id = ?,name = ?,"
              + "description = ?,retry_attempts = ?,seconds_before_retry = ?,disable_alerting = ?,"
              + "action = ?, pool = ?, priority = ? where job_id = ?");

      m_deps =
          this.metadataConnection
              .prepareStatement("insert into "
                  + this.tablePrefix
                  + "job_dependencie(parent_job_id,job_id,continue_if_failed,allow_duplicates) values(?,?,?,?)");

      m_depDel =
          this.metadataConnection.prepareStatement("delete from  " + this.tablePrefix
              + "job_dependencie where parent_job_id = ?");

      m_depSingleDel =
          this.metadataConnection.prepareStatement("delete from  " + this.tablePrefix
              + "job_dependencie where parent_job_id = ? and job_id = ?");

      // find parameter lists
      HashMap list = new HashMap();
      XMLHelper.listParameterLists(pJob, list);

      // create new parameter list
      int pmID = -1;
      String pm = XMLHelper.getAttributeAsString(pJob.getAttributes(), "PARAMETER_LIST", null);

      Set set = list.keySet();
      Iterator iter = set.iterator();

      while (iter.hasNext()) {
        String s = (String) list.get(iter.next());
        pmID = this.getParameterListID(s);

        if (pmID == -1) {
          ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
              "Creating new parameter list " + s);
          this.createParameterList(s);
        }
      }

      // get default parameter list
      if (pm != null) {
        pmID = this.getParameterListID(pm);
      }

      // create new project if needed
      mProject = XMLHelper.getAttributeAsString(pJob.getAttributes(), "PROJECT", null);

      if (mProject == null) {
        ResourcePool.LogMessage("No project attribute specified for job, import not possible");

        return false;
      }

      int iProject = this.getProjectID(mProject);

      if (iProject == -1) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
            "Creating new project " + mProject);
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
              ResourcePool.LogMessage("Inferring dependent job: " + dependentJob);

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
              m_jobStmt.setNull(11, Types.VARCHAR);
              m_jobStmt.setNull(12, Types.INTEGER);
              m_jobStmt.executeUpdate();
            }
          }
        }
      }

      // Check for existance
      String ID = XMLHelper.getAttributeAsString(pJob.getAttributes(), "ID", null);

      if (ID == null) {
        ResourcePool.LogMessage("No ID attribute specified for job, import not possible");

        return false;
      }

      String type = XMLHelper.getAttributeAsString(pJob.getAttributes(), "TYPE", null);

      if (type == null) {
        ResourcePool.LogMessage("No TYPE attribute specified for job, import not possible");

        return false;
      }

      ETLJob eJob = this.getJob(ID);

      // if not existing then insert
      if (eJob == null) {
        m_jobStmt.setString(1, ID);
        m_jobStmt.setInt(2, this.getJobTypeID(type));
        m_jobStmt.setInt(3, iProject);

        if (pm == null) {
          m_jobStmt.setNull(4, Types.INTEGER);
        } else {
          m_jobStmt.setInt(4, pmID);
        }

        this.setString(m_jobStmt, 5,
            XMLHelper.getAttributeAsString(pJob.getAttributes(), "NAME", null));
        this.setString(m_jobStmt, 6,
            XMLHelper.getAttributeAsString(pJob.getAttributes(), "DESCRIPTION", null));
        m_jobStmt.setInt(7, XMLHelper.getAttributeAsInt(pJob.getAttributes(), "RETRY_ATTEMPTS", 0));
        m_jobStmt.setInt(8,
            XMLHelper.getAttributeAsInt(pJob.getAttributes(), "SECONDS_BEFORE_RETRY", 10));
        this.setString(m_jobStmt, 9,
            XMLHelper.getAttributeAsString(pJob.getAttributes(), "DISABLE_ALERTING", null));

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
                action = XMLHelper.outputXML(n);
              }
            } else if ((n.getNodeName().equalsIgnoreCase("DEPENDS_ON") == false)
                && (n.getNodeName().equalsIgnoreCase("WAITS_ON") == false)) {
              action = action + XMLHelper.outputXML(n);
            }
          }
        }

        m_jobStmt.setCharacterStream(10, new java.io.StringReader(action), action.length());
        m_jobStmt.setString(11, XMLHelper.getAttributeAsString(pJob.getAttributes(), "POOL",
            EngineConstants.DEFAULT_POOL));
        m_jobStmt.setInt(12, XMLHelper.getAttributeAsInt(pJob.getAttributes(), "PRIORITY",
            EngineConstants.DEFAULT_PRIORITY));
        m_jobStmt.executeUpdate();
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Creating job "
            + ID);
      } else // update current job
      {
        m_updStmt.setInt(1, this.getJobTypeID(type));
        m_updStmt.setInt(2, iProject);

        if (pm == null) {
          m_updStmt.setNull(3, Types.INTEGER);
        } else {
          m_updStmt.setInt(3, pmID);
        }

        this.setString(m_updStmt, 4,
            XMLHelper.getAttributeAsString(pJob.getAttributes(), "NAME", null));
        this.setString(m_updStmt, 5,
            XMLHelper.getAttributeAsString(pJob.getAttributes(), "DESCRIPTION", null));
        m_updStmt.setInt(6, XMLHelper.getAttributeAsInt(pJob.getAttributes(), "RETRY_ATTEMPTS", 0));
        m_updStmt.setInt(7,
            XMLHelper.getAttributeAsInt(pJob.getAttributes(), "SECONDS_BEFORE_RETRY", 10));
        this.setString(m_updStmt, 8,
            XMLHelper.getAttributeAsString(pJob.getAttributes(), "DISABLE_ALERTING", null));

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
                action = XMLHelper.outputXML(n);
              }
            } else if ((n.getNodeName().equalsIgnoreCase("DEPENDS_ON") == false)
                && (n.getNodeName().equalsIgnoreCase("WAITS_ON") == false)) {
              action = action + XMLHelper.outputXML(n);
            }
          }
        }

        m_updStmt.setCharacterStream(9, new java.io.StringReader(action), action.length());
        m_updStmt.setString(10, XMLHelper.getAttributeAsString(pJob.getAttributes(), "POOL",
            EngineConstants.DEFAULT_POOL));
        m_updStmt.setInt(11, XMLHelper.getAttributeAsInt(pJob.getAttributes(), "PRIORITY",
            EngineConstants.DEFAULT_PRIORITY));
        m_updStmt.setString(12, ID);
        m_updStmt.executeUpdate();
        ResourcePool.LogMessage("Updating job " + ID);
      }

      // add dependencies
      nl = pJob.getChildNodes();

      // delete old dependencies
      m_depDel.setString(1, pJob.getAttributes().getNamedItem("ID").getNodeValue());
      m_depDel.executeUpdate();

      for (int i = 0; i < nl.getLength(); i++) {
        Node n = nl.item(i);

        if (n.getNodeType() == Node.ELEMENT_NODE) {
          if (n.getNodeName().equalsIgnoreCase("DEPENDS_ON")
              || n.getNodeName().equalsIgnoreCase("WAITS_ON")) {
            String dependentJob = n.getFirstChild().getNodeValue();

            // insert blank dependent job
            m_deps.setString(2, dependentJob);
            m_deps.setString(1, pJob.getAttributes().getNamedItem("ID").getNodeValue());

            // protect against duplicate dependencies
            m_depSingleDel.setString(2, dependentJob);
            m_depSingleDel.setString(1, pJob.getAttributes().getNamedItem("ID").getNodeValue());
            m_depSingleDel.executeUpdate();

            if (n.getNodeName().equalsIgnoreCase("DEPENDS_ON")) {
              m_deps.setString(3, "N");
            } else {
              m_deps.setString(3, "Y");
            }

            // set evaluation
            m_deps.setString(4, XMLHelper.getAttributeAsBoolean(n.getAttributes(),
                "ALLOW_DUPLICATES", false) ? "Y" : "N");

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
   * Sets the string.
   * 
   * @param m_jobStmt the m_job stmt
   * @param value the value
   * @param pos the pos
   * @throws SQLException the SQL exception
   */
  private void setString(PreparedStatement m_jobStmt, int pos, String value) throws SQLException {
    if (value == null) {
      m_jobStmt.setNull(pos, Types.VARCHAR);
    } else {
      m_jobStmt.setString(pos, value);
    }
  }

  private static Set<String> KEYS_TO_ENCRYPT = new HashSet<String>(Arrays.asList(new String[] {
      "PASSWORD", "AWSSECRET"}));

  /**
   * Import parameter list.
   * 
   * @param pParameterList the parameter list
   * @return true, if successful
   * @throws Exception the exception
   */
  public boolean importParameterList(Node pParameterList) throws Exception {
    PreparedStatement m_update = null;
    PreparedStatement m_insert = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_insert =
          this.metadataConnection
              .prepareStatement("insert into "
                  + this.tablePrefix
                  + "parameter(parameter_id,parameter_list_id,parameter_name,parameter_value,sub_parameter_list_name)"
                  + " select coalesce(max(parameter_id)+1,1),?,?,?,? from " + this.tablePrefix
                  + "parameter");

      m_update =
          this.metadataConnection.prepareStatement("update " + this.tablePrefix
              + "parameter set parameter_value = ?,sub_parameter_list_name = ? "
              + " where parameter_name = ? and parameter_list_id = ?");

      // create new parameter list
      int pmID = -1;
      String s = pParameterList.getAttributes().getNamedItem("NAME").getNodeValue();
      pmID = this.getParameterListID(s);

      if (pmID == -1) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
            "Creating parameter list " + s);
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

            String name = n.getAttributes().getNamedItem("NAME").getNodeValue();
            String subList = null;
            Node x = n.getAttributes().getNamedItem("PARAMETER_LIST");

            if (x != null) {
              subList = x.getNodeValue();
            }

            if (this.mEncryptionEnabled) {
              if (KEYS_TO_ENCRYPT.contains(name.toUpperCase()) && value != null) {
                value = this.mEncryptor.encrypt(value);
              }
            }

            String[] res = this.getParameterValue(pmID, name);

            if ((res == null) || (res.length == 0)) {
              ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
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
              ResourcePool.LogMessage("Updating parameter " + s + "." + name);

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

  /**
   * Sets the parameter value.
   * 
   * @param iParameterListId The parameter list id
   * @param strParameterName the str parameter name
   * @param strValue the str value
   * @return true, if successful
   * @throws Exception the exception
   */
  private boolean setParameterValue(int iParameterListId, String strParameterName, String strValue)
      throws Exception {
    PreparedStatement m_update = null;

    try {
      synchronized (this.oLock) {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        m_update =
            this.metadataConnection.prepareStatement("update " + this.tablePrefix
                + "parameter set parameter_value = ? "
                + " where parameter_name = ? and parameter_list_id = ?");

        if (this.mEncryptionEnabled) {
          if (KEYS_TO_ENCRYPT.contains(strParameterName.toUpperCase())) {
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
            "Error setting parameter list (id = " + iParameterListId + "): " + e.getMessage());
      } else {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Error setting parameter list (name = " + strParameterName + "): " + e.getMessage());
      }

      return false;
    }

    // check for existance of dependent jobs
    // if they do not exist create them as empty jobs
    // update job details
    // add dependencies
    return true;
  }

  /**
   * Gets the job dependencies.
   * 
   * @param pJobID the job ID
   * @return the job dependencies
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public JobDependencie[] getJobDependencies(String pJobID) throws SQLException,
      java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    List<JobDependencie> jobsToFetch = new ArrayList<JobDependencie>();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT JOB_ID,CONTINUE_IF_FAILED,ALLOW_DUPLICATES FROM  "
                  + this.tablePrefix + "JOB_DEPENDENCIE A " + "WHERE PARENT_JOB_ID = ?");
      m_stmt.setString(1, pJobID);
      m_rs = m_stmt.executeQuery();

      // cycle through pending jobs setting next run date
      while (m_rs.next()) {
        try {
          JobDependencie res = new JobDependencie();
          res.name = m_rs.getString(1);
          res.critical = !m_rs.getString(2).equalsIgnoreCase(Metadata.WAITS_ON);

          if (m_rs.getString(3) != null)
            res.allowDuplicates = m_rs.getString(3).equalsIgnoreCase("Y");

          jobsToFetch.add(res);
        } catch (Exception e) {
          ResourcePool.logMessage("Error creating job: " + e.getMessage());

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

    return jobsToFetch.toArray(new JobDependencie[0]);
  }

  /** The Constant jobReferences. */
  static final String[][] jobReferences = { {"ALERT_SUBSCRIPTION", "JOB_ID"},
      {"JOB_DEPENDENCIE", "JOB_ID"}, {"JOB_DEPENDENCIE", "PARENT_JOB_ID"}, {"JOB_ERROR", "JOB_ID"},
      {"JOB_ERROR_HIST", "JOB_ID"}, {"JOB_LOG", "JOB_ID"}, {"JOB_LOG_HIST", "JOB_ID"},
      {"JOB_QA_HIST", "JOB_ID"}, {"JOB_SCHEDULE", "JOB_ID"}, {"JOB", "JOB_ID"},};

  /**
   * Delete job.
   * 
   * @param pJobID the job ID
   * @return true, if successful
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean deleteJob(String pJobID) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      for (String[] element : Metadata.jobReferences) {
        m_stmt =
            this.metadataConnection.prepareStatement("DELETE  FROM " + this.tablePrefix
                + element[0] + " WHERE " + element[1] + " = ?");
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

  /**
   * Gets the project name.
   * 
   * @param pProjectID the project ID
   * @return the project name
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public String getProjectName(int pProjectID) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    String projectName = "";

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT PROJECT_DESC FROM  " + this.tablePrefix
              + "PROJECT A " + "WHERE PROJECT_ID = ?");
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

  /**
   * Gets the project ID.
   * 
   * @param pProjectName the project name
   * @return the project ID
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public int getProjectID(String pProjectName) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    int projectID = -1;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT PROJECT_ID FROM  " + this.tablePrefix
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

  /**
   * Gets the projects.
   * 
   * @return the projects
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public Object[] getProjects() throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList projects = new ArrayList();

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT PROJECT_ID, PROJECT_DESC FROM  "
              + this.tablePrefix + "PROJECT A ");
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

  /**
   * Creates the project.
   * 
   * @param mProjectName The project name
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public void createProject(String mProjectName) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;

    if (this.getProjectID(mProjectName) == -1) {
      synchronized (this.oLock) {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        m_stmt =
            this.metadataConnection.prepareStatement("INSERT INTO " + this.tablePrefix
                + "PROJECT(PROJECT_ID,PROJECT_DESC) "
                + "SELECT COALESCE(MAX(PROJECT_ID)+1,1),? FROM " + this.tablePrefix + "PROJECT ");
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

  /**
   * Creates the parameter list.
   * 
   * @param mPlist The plist
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public void createParameterList(String mPlist) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("INSERT INTO " + this.tablePrefix
              + "PARAMETER_LIST(PARAMETER_LIST_ID,PARAMETER_LIST_NAME) "
              + "SELECT COALESCE(MAX(PARAMETER_LIST_ID)+1,1),? FROM " + this.tablePrefix
              + "PARAMETER_LIST " + "WHERE NOT EXISTS (SELECT PARAMETER_LIST_NAME FROM "
              + this.tablePrefix + "PROJECT WHERE PARAMETER_LIST_NAME = ?)");
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
   * Insert the method's description here. Creation date: (5/15/2002 1:44:11 PM)
   * 
   * @param pIDName java.lang.String
   * @param idBatchSize the id batch size
   * @return double
   * @throws Exception the exception
   */
  public long getBatchOfIDValues(String pIDName, long idBatchSize) throws Exception {
    PreparedStatement m_stmt = null;
    long maxID = -1;
    Exception e = null;

    synchronized (this.oLock) {
      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        m_stmt =
            this.metadataConnection.prepareStatement("SELECT CURRENT_VALUE FROM  "
                + this.tablePrefix + "ID_GENERATOR WHERE ID_NAME = ? FOR UPDATE");
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
          ResourcePool
              .LogMessage("Sequence " + pIDName + " being auto-created and defaulting to 0");
          m_stmt =
              this.metadataConnection.prepareStatement("insert into " + this.tablePrefix
                  + "ID_GENERATOR(ID_NAME,CURRENT_VALUE) VALUES('" + pIDName + "',0)");
          m_stmt.executeUpdate();

          if (m_stmt != null) {
            m_stmt.close();
          }

          maxID = 0;
        }

        m_stmt =
            this.metadataConnection
                .prepareStatement("UPDATE  " + this.tablePrefix + "ID_GENERATOR SET CURRENT_VALUE = ? WHERE ID_NAME = ?"); //$NON-NLS-1$

        m_stmt.setDouble(1, maxID + idBatchSize);
        m_stmt.setString(2, pIDName);

        m_stmt.execute();
        this.metadataConnection.commit();

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
   * Insert the method's description here. Creation date: (5/9/2002 10:49:22 PM)
   * 
   * @param pParameterSetID int
   * @param pStatus the status
   * @return java.lang.Object[]
   */
  public PageParserPageDefinition[] getPageParserParameters(int pParameterSetID, ETLStatus pStatus) {
    PreparedStatement m_stmt = null;
    PageParserPageDefinition[] pDefs = null;
    ResultSet m_rs = null;
    PreparedStatement m_pStmt = null;
    int fieldCnt = 0;

    synchronized (this.oLock) {
      try {
        this.refreshMetadataConnection();

        m_stmt =
            this.metadataConnection
                .prepareStatement("select weight,protocol,hostname,directory,template,parameter_list_id from  "
                    + this.tablePrefix
                    + "page_set_to_page_definition a,  "
                    + this.tablePrefix
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
            m_pStmt =
                this.metadataConnection
                    .prepareStatement(" SELECT parameter_name, parameter_value, parameter_required, remove_parameter_value, remove_parameter,value_seperator  FROM  "
                        + this.tablePrefix + "page_parameter_list where parameter_list_id = ?");
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
              System.arraycopy(pageParameters, 0, tmp, 0, pageParameters.length);
              pageParameters = tmp;
            }

            pageParameters[pCount] = new PageParserPageParameter();
            pageParameters[pCount].setParameterName(rs.getString(1));
            pageParameters[pCount].setParameterValue(rs.getString(2));

            if ((rs.getString(3) != null) && (rs.getString(3).compareToIgnoreCase("Y") == 0)) {
              pageParameters[pCount].setParameterRequired(true);
            } else {
              pageParameters[pCount].setParameterRequired(false);
            }

            if ((rs.getString(4) != null) && (rs.getString(4).compareToIgnoreCase("Y") == 0)) {
              pageParameters[pCount].setRemoveParameterValue(true);
            } else {
              pageParameters[pCount].setRemoveParameterValue(false);
            }

            if ((rs.getString(5) != null) && (rs.getString(5).compareToIgnoreCase("Y") == 0)) { //$NON-NLS-1$
              pageParameters[pCount].setRemoveParameter(true);
            } else {
              pageParameters[pCount].setRemoveParameter(false);
            }

            pageParameters[pCount].setValueSeperator(rs.getString(6));
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
          pStatus.setErrorMessage("getPageParserParameterDefinition: " + ee);
        } else {
          System.err.println("[" + new java.util.Date() + "] getPageParserParameterDefinition: "
              + ee);
        }

        return (null);
      } catch (Exception ee) {
        if (pStatus != null) {
          pStatus.setErrorMessage("getPageParserParameterDefinition: " + ee);
        } else {
          System.err.println("[" + new java.util.Date() + "] getPageParserParameterDefinition: "
              + ee);
        }

        return (null);
      }
    }

    return (pDefs);
  }

  /**
   * Gets the parameter list.
   * 
   * @param strParameterListName the str parameter list name
   * @return the parameter list
   */
  public Object[][] getParameterList(String strParameterListName) {
    return this.getParameterList(strParameterListName,
        this.getParameterListID(strParameterListName));
  }

  /**
   * Gets the parameter list.
   * 
   * @param iParameterListID The parameter list ID
   * @return the parameter list
   */
  public Object[][] getParameterList(int iParameterListID) {
    return this.getParameterList(null, iParameterListID);
  }

  // Use name first to find the parameters for this list. If name is null,
  // then use ID.
  // Returns: Object[num_parameters][3] where:
  // Object[x][PARAMETER_NAME] = parameter name
  // Object[x][PARAMETER_VALUE] = parameter value
  // Object[x][SUB_PARAMETER_LIST_NAME] = sub parameter list name or null if
  // none
  // Object[x][SUB_PARAMETER_LIST] = sub parameter array or null if none
  /**
   * Gets the parameter list.
   * 
   * @param strParameterListName the str parameter list name
   * @param iParameterListID The parameter list ID
   * @return the parameter list
   */
  public Object[][] getParameterList(String strParameterListName, int iParameterListID) {
    PreparedStatement stmt;
    ResultSet rs;
    Object[][] parameterList = null;
    int fieldCnt = 0;
    int iSubParameterListID;
    ArrayList postEncrypt = new ArrayList();
    try {
      synchronized (this.oLock) {
        this.refreshMetadataConnection();

        if (strParameterListName == null) {
          String sql = null;

          if (this.bAnsi92OuterJoin) {
            sql =
                "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
                    + this.tablePrefix
                    + "PARAMETER p "
                    + " inner join "
                    + this.tablePrefix
                    + "PARAMETER_LIST pl on (p.PARAMETER_LIST_ID = pl.PARAMETER_LIST_ID) "
                    + " left outer join "
                    + this.tablePrefix
                    + "PARAMETER_LIST spl "
                    + " on (p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID) "
                    + " WHERE pl.PARAMETER_LIST_ID = ?";
          } else {
            sql =
                "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
                    + this.tablePrefix
                    + "PARAMETER p,  "
                    + this.tablePrefix
                    + "PARAMETER_LIST spl WHERE p.PARAMETER_LIST_ID = ? AND p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID (+)"; //$NON-NLS-1$
          }

          stmt = this.metadataConnection.prepareStatement(sql);
          stmt.setInt(1, iParameterListID);
        } else // use name
        {
          String sql = null;

          if (this.bAnsi92OuterJoin) {
            sql =
                "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
                    + this.tablePrefix
                    + "PARAMETER p "
                    + " inner join "
                    + this.tablePrefix
                    + "PARAMETER_LIST pl on (p.PARAMETER_LIST_ID = pl.PARAMETER_LIST_ID) "
                    + " left outer join "
                    + this.tablePrefix
                    + "PARAMETER_LIST spl "
                    + " on (p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID) "
                    + " WHERE pl.PARAMETER_LIST_NAME LIKE ?";
          } else {
            sql =
                "SELECT PARAMETER_NAME, PARAMETER_VALUE, SUB_PARAMETER_LIST_ID, CASE WHEN SUB_PARAMETER_LIST_ID IS NOT NULL THEN SPL.PARAMETER_LIST_NAME ELSE P.SUB_PARAMETER_LIST_NAME END FROM  "
                    + this.tablePrefix
                    + "PARAMETER p,  "
                    + this.tablePrefix
                    + "PARAMETER_LIST pl,  "
                    + this.tablePrefix
                    + "PARAMETER_LIST spl WHERE p.PARAMETER_LIST_ID = pl.PARAMETER_LIST_ID AND pl.PARAMETER_LIST_NAME LIKE ? AND p.SUB_PARAMETER_LIST_ID = spl.PARAMETER_LIST_ID (+)";
          }

          stmt = this.metadataConnection.prepareStatement(sql);
          stmt.setString(1, strParameterListName);
        }

        rs = stmt.executeQuery();

        while (rs.next()) {
          if (parameterList == null) {
            parameterList = new Object[fieldCnt + 1][4];
          } else {
            fieldCnt++;

            Object[][] tmp = new Object[fieldCnt + 1][4];
            System.arraycopy(parameterList, 0, tmp, 0, parameterList.length);
            parameterList = tmp;
          }

          parameterList[fieldCnt][Metadata.PARAMETER_NAME] = rs.getString(1);
          parameterList[fieldCnt][Metadata.PARAMETER_VALUE] = rs.getString(2);

          if (this.mEncryptionEnabled) {
            // auto encrypt any passwords
            if (KEYS_TO_ENCRYPT
                .contains(((String) parameterList[fieldCnt][Metadata.PARAMETER_NAME]).toUpperCase())) {
              try {
                parameterList[fieldCnt][Metadata.PARAMETER_VALUE] =
                    this.mEncryptor
                        .decrypt((String) parameterList[fieldCnt][Metadata.PARAMETER_VALUE]);
              } catch (Exception e) {
                if (iParameterListID >= 0)
                  postEncrypt.add(new Object[] {new Integer(iParameterListID),
                      parameterList[fieldCnt][Metadata.PARAMETER_NAME],
                      parameterList[fieldCnt][Metadata.PARAMETER_VALUE]});
              }
            }
          }

          // Check for sub parameter list...
          iSubParameterListID = rs.getInt(3);

          if (rs.wasNull() == false) {
            parameterList[fieldCnt][Metadata.SUB_PARAMETER_LIST] = new Integer(iSubParameterListID);
          }

          parameterList[fieldCnt][Metadata.SUB_PARAMETER_LIST_NAME] = rs.getString(4);
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

        this.setParameterValue(((Integer) tmp[0]).intValue(), (String) tmp[1], (String) tmp[2]);
      }
    } catch (Exception e) {
      if (strParameterListName == null) {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Error getting parameter list (id = " + iParameterListID + "): " + e.getMessage());
      } else {
        ResourcePool
            .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error getting parameter list (name = "
                + strParameterListName + "): " + e.getMessage());
      }

      return null;
    }

    return parameterList;
  }

  //
  /**
   * Gets the parameter value.
   * 
   * @param iParameterListID The parameter list ID
   * @param strParameterName the str parameter name
   * @return the parameter value
   */
  public String[] getParameterValue(int iParameterListID, String strParameterName) {
    PreparedStatement stmt;
    ResultSet rs;
    ArrayList vals = new ArrayList();
    ArrayList postEncrypt = new ArrayList();
    try {
      synchronized (this.oLock) {
        this.refreshMetadataConnection();

        stmt =
            this.metadataConnection
                .prepareStatement("SELECT PARAMETER_VALUE FROM " + this.tablePrefix
                    + "PARAMETER WHERE PARAMETER_LIST_ID = ? AND PARAMETER_NAME = ?");
        stmt.setInt(1, iParameterListID);
        stmt.setString(2, strParameterName);

        rs = stmt.executeQuery();

        while (rs.next()) {
          String value = rs.getString(1);

          if (this.mEncryptionEnabled) {
            // auto encrypt any passwords
            if (KEYS_TO_ENCRYPT.contains(strParameterName.toUpperCase())) {
              try {
                value = this.mEncryptor.decrypt(value);
              } catch (Exception e) {
                postEncrypt.add(new Object[] {new Integer(iParameterListID), strParameterName,
                    value});
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

        this.setParameterValue(((Integer) tmp[0]).intValue(), (String) tmp[1], (String) tmp[2]);
      }

    } catch (Exception e) {
      if (strParameterName == null) {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Error getting parameter list (id = " + iParameterListID + "): " + e.getMessage());
      } else {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Error getting parameter list (name = " + strParameterName + "): " + e.getMessage());
      }

      return null;
    }

    String[] res = new String[vals.size()];
    vals.toArray(res);

    return res;
  }

  /**
   * Gets the valid parameter list name.
   * 
   * @param strParameterListName the str parameter list name
   * @return the valid parameter list name
   */
  public String[] getValidParameterListName(String strParameterListName) {
    PreparedStatement stmt;
    ResultSet rs;
    ArrayList parameterLists = new ArrayList();

    try {
      synchronized (this.oLock) {
        stmt =
            this.metadataConnection
                .prepareStatement("SELECT DISTINCT PARAMETER_LIST_NAME FROM  " + this.tablePrefix + "PARAMETER_LIST WHERE PARAMETER_LIST_NAME LIKE ?"); //$NON-NLS-1$
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
      ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
          "Error getting parameter list (name = " + strParameterListName + "): " + e.getMessage());

      return null;
    }

    if (parameterLists.size() == 0) {
      return null;
    }

    String[] res = new String[parameterLists.size()];

    parameterLists.toArray(res);

    return res;
  }

  /**
   * Gets the parameter list name.
   * 
   * @param strParameterID the str parameter ID
   * @return the parameter list name
   */
  public String getParameterListName(int strParameterID) {
    PreparedStatement stmt;
    ResultSet rs;
    String parameterListName = null;

    try {
      synchronized (this.oLock) {
        stmt =
            this.metadataConnection
                .prepareStatement("SELECT PARAMETER_LIST_NAME FROM  " + this.tablePrefix + "PARAMETER_LIST WHERE PARAMETER_LIST_ID = ?"); //$NON-NLS-1$
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
      ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
          "Error getting parameter list (ID = " + strParameterID + "): " + e.getMessage());

      return null;
    }

    return parameterListName;
  }

  /**
   * Gets the parameter list ID.
   * 
   * @param strParameterID the str parameter ID
   * @return the parameter list ID
   */
  public int getParameterListID(String strParameterID) {
    PreparedStatement stmt;
    ResultSet rs;
    int parameterListID = -1;

    try {
      synchronized (this.oLock) {
        stmt =
            this.metadataConnection
                .prepareStatement("SELECT PARAMETER_LIST_ID FROM  " + this.tablePrefix + "PARAMETER_LIST WHERE PARAMETER_LIST_NAME = ?"); //$NON-NLS-1$
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
      ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
          "Error getting parameter list (ID = " + strParameterID + "): " + e.getMessage());

      return -2;
    }

    return parameterListID;
  }

  /**
   * Gets the job type ID.
   * 
   * @param strParameterID the str parameter ID
   * @return the job type ID
   */
  public int getJobTypeID(String strParameterID) {
    PreparedStatement stmt;
    ResultSet rs;
    int parameterListID = -1;

    try {
      synchronized (this.oLock) {
        stmt =
            this.metadataConnection
                .prepareStatement("SELECT JOB_TYPE_ID FROM  " + this.tablePrefix + "JOB_TYPE WHERE DESCRIPTION = ?"); //$NON-NLS-1$
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
      ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
          "Error getting job type (ID = " + strParameterID + "): " + e.getMessage());

      return -2;
    }

    return parameterListID;
  }

  /**
   * Gets the job executor class for type ID.
   * 
   * @param pTypeID the type ID
   * @return the job executor class for type ID
   */
  public String getJobExecutorClassForTypeID(int pTypeID) {
    PreparedStatement stmt;
    ResultSet rs;
    String className = null;

    try {
      synchronized (this.oLock) {
        stmt =
            this.metadataConnection.prepareStatement("SELECT CLASS_NAME FROM  " + this.tablePrefix
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
      ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
          "Error getting job class (ID = " + pTypeID + "): " + e.getMessage());

      return null;
    }

    return className;
  }

  /**
   * Insert the method's description here. Creation date: (5/8/2002 1:20:28 PM)
   * 
   * @param pClassName java.lang.String
   * @return java.lang.Object[]
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public ArrayList getServerExecutorJobTypes(String pClassName) throws SQLException,
      java.lang.Exception {
    ArrayList res = new ArrayList();
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();
      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT JOB_TYPE.CLASS_NAME FROM  "
                  + this.tablePrefix
                  + "JOB_EXECUTOR ,  "
                  + this.tablePrefix
                  + "JOB_EXECUTOR_JOB_TYPE ,  "
                  + this.tablePrefix
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

  /**
   * Record QA history.
   * 
   * @param job_id the job_id
   * @param step_name the step_name
   * @param qa_id the qa_id
   * @param qa_type the qa_type
   * @param execDate the exec date
   * @param details the details
   * @return true, if successful
   */
  public boolean recordQAHistory(String job_id, String step_name, String qa_id, String qa_type,
      Date execDate, String details) {
    PreparedStatement m_stmt = null;

    if ((job_id == null) || (step_name == null) || (qa_id == null) || (qa_type == null)
        || (execDate == null) || (details == null)) {
      ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
          "QA History details incomplete, no record will be stored");

      return false;
    }

    synchronized (this.oLock) {
      String sql =
          "INSERT INTO  "
              + this.tablePrefix
              + "JOB_QA_HIST(JOB_ID,QA_ID,QA_TYPE,STEP_NAME,DETAILS,RECORD_DATE) VALUES(?,?,?,?,?,?)";

      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        m_stmt = this.metadataConnection.prepareStatement(sql);

        m_stmt.setString(1, job_id);
        m_stmt.setString(2, qa_id);
        m_stmt.setString(3, qa_type);
        m_stmt.setString(4, step_name);
        m_stmt.setString(5, details);
        m_stmt.setTimestamp(6, new java.sql.Timestamp(execDate.getTime()));

        m_stmt.execute();

        if (m_stmt != null) {
          m_stmt.close();
        }

        this.metadataConnection.commit();
      } catch (Exception e) {
        ResourcePool.LogException(e, this);

        return false;
      }
    }

    return true;
  }

  /**
   * Gets the QA history.
   * 
   * @param job_id the job_id
   * @param step_name the step_name
   * @param qa_id the qa_id
   * @param qa_type the qa_type
   * @param sampleOffSet the sample off set
   * @param sampleSize the sample size
   * @return the QA history
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public String[] getQAHistory(String job_id, String step_name, String qa_id, String qa_type,
      int sampleOffSet, int sampleSize) throws SQLException, java.lang.Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    String[] res = null;

    synchronized (this.oLock) {
      String sql =
          "SELECT DETAILS " + " FROM  " + this.tablePrefix + "JOB_QA_HIST " + " WHERE JOB_ID = ?  "
              + " AND QA_ID = ?  " + " AND QA_TYPE = ?  " + " AND STEP_NAME = ?  ";

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
      this.refreshMetadataConnection();

      m_stmt = this.metadataConnection.prepareStatement(sql);

      m_stmt.setString(1, job_id);
      m_stmt.setString(2, qa_id);
      m_stmt.setString(3, qa_type);
      m_stmt.setString(4, step_name);

      int pos = 5;

      if (sampleOffSet > 0) {
        for (int i = 0; i < sampleSize; i++) {
          m_stmt.setTimestamp(pos, new java.sql.Timestamp(dates[i][0].getTime()));
          pos++;
          m_stmt.setTimestamp(pos, new java.sql.Timestamp(dates[i][1].getTime()));
          pos++;
        }
      }

      m_rs = m_stmt.executeQuery();

      ArrayList results = new ArrayList();

      // cycle through pending jobs setting next run date
      while (m_rs.next() && ((results.size() < sampleSize) || (sampleSize == -1))) {
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
   * Insert the method's description here. Creation date: (5/7/2002 11:23:34 PM)
   * 
   * @param pServerID int
   * @return java.lang.Object[]
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public Object[][] getServerExecutors(int pServerID) throws SQLException, java.lang.Exception {
    Object[][] res = null;
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection
              .prepareStatement("SELECT b.CLASS_NAME,THREADS,QUEUE_SIZE,d.DESCRIPTION,A.POOL FROM  "
                  + this.tablePrefix
                  + "SERVER_EXECUTOR A,  "
                  + this.tablePrefix
                  + "JOB_EXECUTOR B, "
                  + this.tablePrefix
                  + "job_executor_job_type c, "
                  + this.tablePrefix
                  + "job_type d WHERE A.JOB_EXECUTOR_ID = B.JOB_EXECUTOR_ID AND "
                  + " b.job_executor_id = c.job_executor_id "
                  + " AND c.job_type_id = d.job_type_id AND A.SERVER_ID = ?"); //$NON-NLS-1$
      m_stmt.setInt(1, pServerID);
      m_rs = m_stmt.executeQuery();

      int i = 0;

      // cycle through pending jobs setting next run date
      while (m_rs.next()) {
        if (res == null) {
          res = new Object[i + 1][5];
          res[i][0] = m_rs.getString(1);
          res[i][1] = m_rs.getInt(2);
          res[i][2] = m_rs.getInt(3);
          res[i][3] = m_rs.getString(4);
          res[i][4] = m_rs.getString(5);
        } else {
          i++;

          Object[][] tmp = new Object[i + 1][5];
          tmp[i][0] = m_rs.getString(1);
          tmp[i][1] = m_rs.getInt(2);
          tmp[i][2] = m_rs.getInt(3);
          tmp[i][3] = m_rs.getString(4);
          tmp[i][4] = m_rs.getString(5);
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
   * Insert the method's description here. Creation date: (4/9/2002 10:36:44 AM)
   * 
   * @param pSessionDefinitionID int
   * @param pStatus the status
   * @return datasources.SessionDefinition
   */
  public SessionDefinition getSessionDefinition(int pSessionDefinitionID, ETLStatus pStatus) {
    synchronized (this.oLock) {
      try {
        if (this.metadataConnection == null) {
          this.refreshMetadataConnection();
        }

        // set field definition
        SessionDefinition srcSessionDefinition = new SessionDefinition();

        // ideally sql should be loaded from a sql database to allow for
        // easy platform migration
        PreparedStatement stmt =
            this.metadataConnection
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
                    + this.tablePrefix
                    + "session_identifier a, "
                    + "       "
                    + this.tablePrefix
                    + "session_definition b "
                    + " WHERE b.session_definition_id = ? "
                    + " AND b.session_definition_id = a.session_definition_id " + " ORDER BY weight, " + " session_identifier_id "); //$NON-NLS-1$
        stmt.setInt(1, pSessionDefinitionID);

        ResultSet rs = stmt.executeQuery();

        // Access query results
        while (rs.next()) {
          SessionIdentifier tmpSessionIdentifier = new SessionIdentifier();

          if (srcSessionDefinition.TimeOut == 0) {
            srcSessionDefinition.TimeOut = rs.getInt(6);
          }

          if (srcSessionDefinition.FirstClickIdentifierTimeOut == 0) {
            srcSessionDefinition.FirstClickIdentifierTimeOut = rs.getInt(11);
          }

          if (srcSessionDefinition.MainIdentifierTimeOut == 0) {
            srcSessionDefinition.MainIdentifierTimeOut = rs.getInt(12);
          }

          if (srcSessionDefinition.PersistantIdentifierTimeOut == 0) {
            srcSessionDefinition.PersistantIdentifierTimeOut = rs.getInt(13);
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
          tmpSessionIdentifier.Weight = rs.getInt(4);
          tmpSessionIdentifier.FormatString = rs.getString(5);
          tmpSessionIdentifier.DestinationObjectType = rs.getInt(8);

          if (rs.getInt(10) == 1) {
            tmpSessionIdentifier.setVariableName(rs.getString(3), true);
          } else {
            tmpSessionIdentifier.setVariableName(rs.getString(3), false);
          }

          srcSessionDefinition.addSessionIdentifier(tmpSessionIdentifier);
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
          System.err.println("[" + new java.util.Date() + "] getFlatFileSourceDefinition: " + ee);
        }

        return (null);
      } catch (Exception ee) {
        if (pStatus != null) {
          pStatus.setErrorMessage("SessionDefinition:" + ee);
        } else {
          System.err.println("[" + new java.util.Date() + "] getFlatFileSourceDefinition: " + ee);
        }

        return (null);
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (3/5/2002 3:22:37 PM)
   * 
   * @param pExecutingJobs boolean
   * @param pPendingJobs boolean
   * @param pExecutedJobs boolean
   * @param pLoadID int
   * @return jobscheduler.EtlJob[]
   */
  public ETLJob[] getStatus(boolean pExecutingJobs, boolean pPendingJobs, boolean pExecutedJobs,
      int pLoadID) {
    return null;
  }

  /**
   * Insert the method's description here. Creation date: (3/5/2002 3:36:19 PM)
   * 
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */

  private static org.h2.tools.Server h2Server;

  private long lastMDCheck = System.currentTimeMillis();

  public synchronized boolean testMDConnection(Connection cConnection) {
    try {
      synchronized (this.oLock) {

        if (lastMDCheck + 1000 > System.currentTimeMillis()) {
          return true;
        }

        lastMDCheck = System.currentTimeMillis();
        // Test the connection first to make sure it's still alive...
        try {
          Statement stmt = cConnection.createStatement();
          ResultSet rs =
              stmt.executeQuery("select 1 from " + this.tablePrefix
                  + "JOB_STATUS WHERE STATUS_ID = " + ETLJobStatus.EXECUTING);

          int i = 0;
          while (rs.next()) {
            i = rs.getInt(1);
          }

          rs.close();
          stmt.close();
          if (i == 0) {
            cConnection.close();
            cConnection = null;

            return false;
          }
        } catch (Exception e) {
          // If can't read metadata, the connection must be dead.
          // Remove
          // it from our pool...
          cConnection.close();
          cConnection = null;

          return false;
        }
      }
    } catch (Exception e) {
      // If can't create a statement, the connection must really be dead.
      // Remove it from our pool...
      return false;
    }

    return true;
  }

  protected void refreshMetadataConnection() throws SQLException, java.lang.Exception {
    if (this.metadataConnection != null) {
      try {
        if (this.testMDConnection(this.metadataConnection) == false) {
          System.err.println("[" + new java.util.Date()
              + "] checkConnection connection closed for reason unknown");
          this.metadataConnection = null;
        }
      } catch (Exception ee) {
        System.err.println("[" + new java.util.Date() + "] checkConnection Exception: " + ee);
        System.err.println("[" + new java.util.Date()
            + "] checkConnection SQLException: Server will attempt to reconnect");

        this.metadataConnection = null;
        // testConnectionStmt = null;
      }
    }

    if (this.metadataConnection == null) {
      this.columnExists.clear();
      Class cl = Class.forName(this.JDBCDriver);

      try {
        this.metadataConnection =
            DriverManager.getConnection(this.JDBCURL, this.Username, this.Password);

      } catch (JdbcSQLException e) {
        // we are trying to use h2 then start the h2 server if not alive
        if (h2Server != null)
          throw e;

        if (e.getErrorCode() == 90067 && cl.equals(org.h2.Driver.class)) {

          if (!(Thread.currentThread().getName().equals("ETLDaemon"))) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.FATAL_MESSAGE,
                "H2 Server can only be started manually or by a KETL Server");
            throw e;
          }

          try {
            h2Server = org.h2.tools.Server.createTcpServer(new String[0]).start();
            this.metadataConnection =
                DriverManager.getConnection(this.JDBCURL, this.Username, this.Password);
          } catch (JdbcSQLException e1) {
            h2Server.stop();
            h2Server = null;
            throw e1;
          }
        }
      }

      this.metadataConnection.setAutoCommit(false);

      // perform version checks
      this.performVersionCheck();

      DatabaseMetaData mdDB = this.metadataConnection.getMetaData();

      ResourcePool.LogMessage(
          Thread.currentThread(),
          ResourcePool.INFO_MESSAGE,
          "Metadata connection established to a " + mdDB.getDatabaseProductName() + " Version: "
              + mdDB.getDatabaseMajorVersion() + "." + mdDB.getDatabaseMinorVersion()
              + " Database, using Driver Version: " + mdDB.getDriverMajorVersion() + "."
              + mdDB.getDriverMinorVersion() + " as '" + mdDB.getUserName() + "'.");
      boolean ansi92 = mdDB.supportsANSI92EntryLevelSQL();
      boolean outerJoins = mdDB.supportsLimitedOuterJoins();

      boolean found = false;
      for (ValidMDDBTypes dbType : ValidMDDBTypes.values()) {
        if (dbType.name()
            .equals(EngineConstants.cleanseDatabaseName(mdDB.getDatabaseProductName()))) {
          found = true;
          // finish loop
          this.currentTimeStampSyntax = this.dbTimeStampTypes[dbType.ordinal()];
          this.nextLoadIDSyntax =
              this.dbSequenceSyntax[dbType.ordinal()].replaceAll("#", "LOAD_ID");
          this.useIdentityColumn = this.dbUseIdentityColumn[dbType.ordinal()];
          this.secondsBeforeRetry = this.dbSecondsBeforeRetry[dbType.ordinal()];

          this.nextServerIDSyntax =
              this.dbSequenceSyntax[dbType.ordinal()].replaceAll("#", "SERVER_ID");
          this.singleRowPullSyntax = this.dbSingleRowPull[dbType.ordinal()];
          this.mResolvedLoadTableName = this.mLoadTableName[dbType.ordinal()];

          String incrementIdenentityColumnSyntax =
              this.dbIncrementIdentityColumnSyntax[dbType.ordinal()];
          if (incrementIdenentityColumnSyntax != null) {
            this.mIncIdentColStmt =
                this.metadataConnection.prepareStatement(incrementIdenentityColumnSyntax);
          }
        }
      }

      if (!found) {
        throw new Exception("ERROR: " + mdDB.getDatabaseProductName()
            + " is not supported for metadata storage");
      }

      if (ansi92 && outerJoins) {
        this.bAnsi92OuterJoin = true;
      }
    }
  }

  /**
   * Perform version check.
   * 
   * @throws SQLException the SQL exception
   */
  private void performVersionCheck() throws SQLException {

    Statement stmt = null;
    try {
      stmt = this.metadataConnection.createStatement();
      // check for update code 2.1.0
      try {
        stmt.execute("select count(*) from " + this.tablePrefix
            + "job_log_hist where LAST_UPDATE_DATE is null and 1=0 union select count(*) from "
            + this.tablePrefix + "job_log where LAST_UPDATE_DATE is null and 1=0");
      } catch (Exception e) {
        throw new RuntimeException("Metadata needs updating to 2.1, see scripts in $KETLDIR/setup");
      }
      // check for clob code 2.1.9
      try {
        stmt.execute("select count(*) from " + this.tablePrefix
            + "job_log_hist where stats is null and 1=0 union select count(*) from "
            + this.tablePrefix + "job_log where stats is null and 1=0");
      } catch (Exception e) {
        throw new RuntimeException(
            "Metadata needs updating to 2.1.9, see scripts in $KETLDIR/setup");
      }

      // check for pool code 2.1.30
      try {
        stmt.execute("select count(*) from " + this.tablePrefix
            + "server_executor where pool is null and 1=0");
      } catch (Exception e) {
        throw new RuntimeException(
            "Metadata needs updating to 2.1.30, see scripts in $KETLDIR/setup");
      }
    } finally {
      if (stmt != null)
        stmt.close();
    }
  }

  /** The column exists. */
  protected HashMap columnExists = new HashMap();

  /**
   * Column exists.
   * 
   * @param pTablename the tablename
   * @param pColumn the column
   * @return true, if successful
   * @throws Exception the exception
   */
  protected boolean columnExists(String pTablename, String pColumn) throws Exception {
    boolean found = false;
    synchronized (this.oLock) {

      String key = pTablename + (char) 0 + pColumn;

      Object res = this.columnExists.get(key);
      if (res != null)
        return (Boolean) res;

      this.refreshMetadataConnection();
      Statement stmt = this.metadataConnection.createStatement();

      try {
        ResultSet rs =
            stmt.executeQuery("SELECT count(" + pColumn + ") FROM " + this.tablePrefix + pTablename);
        found = true;
        rs.close();
      } catch (Exception e) {
        found = false;
      }

      if (stmt != null) {
        stmt.close();
      }

      this.columnExists.put(key, found);
    }
    return found;
  }

  /**
   * Insert the method's description here. Creation date: (5/1/2002 7:29:49 PM)
   * 
   * @param pServerName java.lang.String
   * @return int
   */
  public int registerServer(String pServerName) {
    String sql = null;
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    int serverID = -1;

    synchronized (this.oLock) {
      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();
        m_stmt =
            this.metadataConnection
                .prepareStatement("SELECT MAX(SERVER_ID) FROM  " + this.tablePrefix + "SERVER WHERE SERVER_NAME = ?"); //$NON-NLS-1$

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
          PreparedStatement selNextServerID =
              this.metadataConnection
                  .prepareStatement(this.useIdentityColumn ? this.nextServerIDSyntax : ("SELECT "
                      + this.nextServerIDSyntax + " " + this.singleRowPullSyntax + "")); //$NON-NLS-1$

          if (this.mIncIdentColStmt != null) {
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

          m_stmt =
              this.metadataConnection
                  .prepareStatement("INSERT INTO  " + this.tablePrefix + "SERVER(SERVER_ID,SERVER_NAME,STATUS_ID) VALUES(?,?,?)"); //$NON-NLS-1$
          m_stmt.setInt(1, serverID);
          m_stmt.setString(2, pServerName);
          m_stmt.setInt(3, ETLServerStatus.SERVER_ALIVE);

          m_stmt.execute();

          ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Registering server "
              + pServerName + " for the first time");
          ResourcePool
              .LogMessage("Server "
                  + pServerName
                  + " will be initialized with a predefined number of executors, to modify this go to the server_executors table.");

          m_stmt =
              this.metadataConnection.prepareStatement("INSERT INTO  " + this.tablePrefix
                  + "SERVER_EXECUTOR(SERVER_ID,JOB_EXECUTOR_ID,THREADS,POOL) "
                  + " SELECT ?,JOB_EXECUTOR_ID,CASE JOB_EXECUTOR_ID WHEN 4 THEN 1 ELSE 2 END,'"
                  + EngineConstants.DEFAULT_POOL + "' FROM " + this.tablePrefix + "JOB_EXECUTOR");
          m_stmt.setInt(1, serverID);
          m_stmt.execute();

          this.metadataConnection.commit();
        } else {
          m_stmt =
              this.metadataConnection
                  .prepareStatement("UPDATE  " + this.tablePrefix + "SERVER SET STATUS_ID = ? WHERE SERVER_ID = ?"); //$NON-NLS-1$
          m_stmt.setInt(1, ETLServerStatus.SERVER_ALIVE);
          m_stmt.setInt(2, serverID);
          m_stmt.execute();

          this.metadataConnection.commit();
        }

        if (m_rs != null) {
          m_rs.close();
        }

        if (m_stmt != null) {
          m_stmt.close();
        }
      } catch (SQLException e) {
        ResourcePool.logMessage("Error registering server: error:" + e + "(" + sql + ")");
      } catch (Exception e) {
        ResourcePool.logMessage("Error registering server: error:" + e);
      }
    }

    return (serverID);
  }

  /**
   * Record job message.
   * 
   * @param pETLJob the ETL job
   * @param oStep the o step
   * @param iErrorCode The error code
   * @param iLevel The level
   * @param strMessage the str message
   * @param strExtendedDetails the str extended details
   * @param bSendEmail the b send email
   */
  public void recordJobMessage(ETLJob pETLJob, RecordChecker oStep, int iErrorCode, int iLevel,
      String strMessage, String strExtendedDetails, boolean bSendEmail) {
    this.recordJobMessage(pETLJob, oStep, iErrorCode, iLevel, strMessage, strExtendedDetails,
        bSendEmail, new java.util.Date());
  }

  /**
   * Insert the method's description here. Creation date: (5/8/2002 3:51:42 PM)
   * 
   * @param pETLJob com.kni.etl.ETLJob
   * @param oStep the o step
   * @param iErrorCode The error code
   * @param iLevel The level
   * @param strMessage the str message
   * @param strExtendedDetails the str extended details
   * @param bSendEmail the b send email
   * @param dDate the d date
   */
  public void recordJobMessage(ETLJob pETLJob, RecordChecker oStep, int iErrorCode, int iLevel,
      String strMessage, String strExtendedDetails, boolean bSendEmail, Date dDate) {
    String sql = null;
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        if (this.metadataConnection != null) {
          String job_id = "NA";
          String step_name = "NA";
          int executionID = -1, loadId = -1;

          if (pETLJob != null) {
            job_id = pETLJob.getJobID();
            executionID = pETLJob.getJobExecutionID();
            loadId = pETLJob.getLoadID();
          }

          if (oStep != null) {
            step_name = oStep.toString();
          }

          // write error to log if errors occured
          m_stmt =
              this.metadataConnection
                  .prepareStatement("INSERT INTO  "
                      + this.tablePrefix
                      + "Job_Error_Hist(JOB_ID,DM_LOAD_ID,STEP_NAME,MESSAGE,CODE,ERROR_DATETIME,DETAILS) VALUES(?,?,?,?,?,?,?)"); //$NON-NLS-1$

          String msg = strMessage;

          if (msg != null && msg.length() > 800) {
            System.err.println("[" + new java.util.Date()
                + "] Error to long, trimming stored message. Full message: " + msg);
            msg = msg.substring(0, 800);
          }

          m_stmt.setString(1, job_id);
          m_stmt.setInt(2, executionID);
          m_stmt.setString(3, step_name);
          m_stmt.setString(4, msg);
          m_stmt.setString(5, Integer.toString(iErrorCode));
          m_stmt.setTimestamp(6, new Timestamp(dDate.getTime()));

          if (strExtendedDetails == null) {
            m_stmt.setNull(7, Types.VARCHAR);
          } else {
            m_stmt.setString(7, strExtendedDetails);
          }

          m_stmt.execute();
          this.metadataConnection.commit();

          if (bSendEmail) {
            this.sendAlertEmail(iLevel, job_id, Integer.toString(iErrorCode), executionID, loadId,
                new java.util.Date(), strMessage, strExtendedDetails, pETLJob == null ? null
                    : pETLJob.getDumpFile());
          }

          if (m_stmt != null) {
            m_stmt.close();
          }
        }
      } catch (SQLException e) {
        ResourcePool.logMessage("Error writing history:" + e.toString() + "(" + sql + ")");
        e.printStackTrace();
      } catch (Exception e) {
        ResourcePool.logMessage("Error writing history:" + e.toString());
        e.printStackTrace();
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (5/8/2002 3:51:42 PM)
   * 
   * @param pETLJob com.kni.etl.ETLJob
   */
  public void setJobStatus(ETLJob pETLJob) {
    String sql = null;
    PreparedStatement m_stmt = null;

    // ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.DEBUG_MESSAGE,
    // "setJobStatus: ID=" +
    // pETLJob.sJobID
    // + ", STATUS ID = " + pETLJob.getStatus().getStatusCode() + ", CANCEL
    // STATUS = "
    // + pETLJob.isCancelSuccessfull());

    synchronized (this.oLock) {
      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        boolean logStats = false;
        switch (pETLJob.getStatus().getStatusCode()) {
          case ETLJobStatus.WAITING_TO_BE_RETRIED:
          case ETLJobStatus.PENDING_CLOSURE_FAILED:
          case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
          case ETLJobStatus.PENDING_CLOSURE_SKIP:
          case ETLJobStatus.PENDING_CLOSURE_CANCELLED:
            m_stmt =
                this.metadataConnection.prepareStatement("UPDATE  " + this.tablePrefix
                    + "JOB_LOG SET END_DATE = " + this.currentTimeStampSyntax
                    + ", STATUS_ID = ?,MESSAGE =  ?, STATS = ? WHERE DM_LOAD_ID = ?"); //$NON-NLS-1$
            logStats = true;
            break;

          default:
            m_stmt =
                this.metadataConnection
                    .prepareStatement("UPDATE  " + this.tablePrefix + "JOB_LOG SET STATUS_ID = ?, MESSAGE = ? WHERE DM_LOAD_ID = ?"); //$NON-NLS-1$

            break;
        }

        m_stmt.setInt(1, pETLJob.getStatus().getStatusCode());
        m_stmt.setString(2, pETLJob.getStatus().getStatusMessage() == null ? null : (pETLJob
            .getStatus().getStatusMessage().getBytes().length > 2000 ? pETLJob.getStatus()
            .getStatusMessage().substring(0, 1000)
            + ".." : pETLJob.getStatus().getStatusMessage()));

        if (logStats) {
          String xml = pETLJob.getStatus().getXMLStats();
          if (xml == null)
            m_stmt.setNull(3, java.sql.Types.LONGVARCHAR);
          else
            m_stmt.setCharacterStream(3, new java.io.StringReader(xml), xml.length());
          m_stmt.setInt(4, pETLJob.getJobExecutionID());
        } else
          m_stmt.setInt(3, pETLJob.getJobExecutionID());

        try {
          m_stmt.execute();
        } catch (SQLException e) {
          ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, e.getMessage());
          m_stmt.getConnection().rollback();
          m_stmt.setInt(1, pETLJob.getStatus().getStatusCode());
          m_stmt.setString(2, "Error logging message, see log");
          m_stmt.setInt(3, pETLJob.getJobExecutionID());
          m_stmt.execute();
        }
        if (m_stmt != null) {
          m_stmt.close();
        }

        this.metadataConnection.commit();

        // write error to log if errors occured
        if ((pETLJob.getStatus().getStatusCode() == ETLJobStatus.PENDING_CLOSURE_FAILED)
            || (pETLJob.getStatus().getStatusCode() == ETLJobStatus.WAITING_TO_BE_RETRIED)) {
          m_stmt =
              this.metadataConnection
                  .prepareStatement("INSERT INTO  "
                      + this.tablePrefix
                      + "Job_Error(JOB_ID,DM_LOAD_ID,MESSAGE,CODE,ERROR_DATETIME) VALUES(?,?,?,?," + this.currentTimeStampSyntax + ")"); //$NON-NLS-1$

          String msg =
              pETLJob.getStatus().getErrorMessage() + "\n" + pETLJob.getStatus().getStatusMessage();

          if (msg != null && msg.length() > 800) {
            System.err.println("[" + new java.util.Date()
                + "] Error to long, trimming stored message. Full message: " + msg);
            msg = msg.substring(0, 800);
          }
          m_stmt.setString(1, pETLJob.getJobID());
          m_stmt.setInt(2, pETLJob.getJobExecutionID());
          m_stmt.setString(3, msg);
          m_stmt.setString(4, new Integer(pETLJob.getStatus().getErrorCode()).toString());

          m_stmt.execute();
          this.metadataConnection.commit();

          // if code allows emails, in otherwords not do not send
          // email error code
          if (pETLJob.getStatus().getErrorCode() != ETLJobStatus.DO_NOT_SEND_EMAIL_ERROR_CODE) {
            // if not waiting to be retried
            if (pETLJob.getStatus().getStatusCode() != ETLJobStatus.WAITING_TO_BE_RETRIED) {
              this.sendAlertEmail(ResourcePool.ERROR_MESSAGE, pETLJob.getJobID(), new Integer(
                  pETLJob.getStatus().getErrorCode()).toString(), pETLJob.getJobExecutionID(),
                  pETLJob.getLoadID(), new java.util.Date(), pETLJob.getStatus().getErrorMessage(),
                  pETLJob.getStatus().getStatusMessage(), pETLJob.getDumpFile());
            }
          }
        } else if (pETLJob.getStatus().getStatusCode() == ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL
            && pETLJob.getNotificationMode() != null) {
          this.sendAlertEmail(ResourcePool.INFO_MESSAGE, pETLJob.getJobID(), new Integer(pETLJob
              .getStatus().getStatusCode()).toString(), pETLJob.getJobExecutionID(), pETLJob
              .getLoadID(), new java.util.Date(), pETLJob.getStatus().getStatusMessage(), pETLJob
              .getStatus().getExtendedMessage(), pETLJob.getDumpFile());
          // notification sent
          pETLJob.notificationSent();
        }

        if (m_stmt != null) {
          m_stmt.close();
        }
      } catch (SQLException e) {
        ResourcePool.logMessage("Error setting status: error:" + e + "(" + sql + ")");
      } catch (Exception e) {
        ResourcePool.logMessage("Error setting status: error:" + e);
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (5/8/2002 3:51:42 PM)
   * 
   * @param pETLJob com.kni.etl.ETLJob
   */
  public void setJobMessage(ETLJob pETLJob) {
    String sql = null;
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        m_stmt =
            this.metadataConnection.prepareStatement("UPDATE  " + this.tablePrefix
                + "JOB_LOG SET MESSAGE =  ? WHERE DM_LOAD_ID = ?");

        m_stmt.setString(1, pETLJob.getStatus().getStatusMessage() == null ? null : (pETLJob
            .getStatus().getStatusMessage().getBytes().length > 2000 ? pETLJob.getStatus()
            .getStatusMessage().substring(0, 1000)
            + ".." : pETLJob.getStatus().getStatusMessage()));

        m_stmt.setInt(2, pETLJob.getJobExecutionID());

        m_stmt.execute();

        this.metadataConnection.commit();

        if (m_stmt != null) {
          m_stmt.close();
        }
      } catch (SQLException e) {
        ResourcePool.logMessage("Error setting status message: error:" + e + "(" + sql + ")");
      } catch (Exception e) {
        ResourcePool.logMessage("Error setting status message: error:" + e);
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (5/15/2002 1:44:11 PM)
   * 
   * @param pIDName java.lang.String
   * @param pValue the value
   * @return double
   */
  public void setMaxIDValue(String pIDName, double pValue) {
    String sql = null;
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      try {
        // Make metadata connection alive.
        this.refreshMetadataConnection();

        m_stmt =
            this.metadataConnection
                .prepareStatement("UPDATE  " + this.tablePrefix + "ID_GENERATOR SET CURRENT_VALUE = ? WHERE ID_NAME = ?"); //$NON-NLS-1$

        m_stmt.setDouble(1, pValue);
        m_stmt.setString(2, pIDName);

        m_stmt.execute();
        this.metadataConnection.commit();

        if (m_stmt != null) {
          m_stmt.close();
        }
      } catch (SQLException e) {
        ResourcePool.logMessage("Error getting maxID for" + pIDName + ": error:" + e + "(" + sql
            + ")");
      } catch (Exception e) {
        ResourcePool.logMessage("Error getting maxID for" + pIDName + ": error:" + e);
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (3/5/2002 3:16:13 PM)
   * 
   * @param pUserName java.lang.String
   * @param pPassword java.lang.String
   * @param pJDBCURL the JDBCURL
   * @param pJDBCDriver the JDBC driver
   * @param pMDPrefix the MD prefix
   * @throws Exception the exception
   */
  public void setRepository(String pUserName, String pPassword, String pJDBCURL,
      String pJDBCDriver, String pMDPrefix) throws Exception {
    this.JDBCDriver = pJDBCDriver;
    this.JDBCURL = pJDBCURL;
    this.Username = pUserName;
    this.Password = pPassword;

    if (pMDPrefix != null) {
      this.tablePrefix = pMDPrefix;
    }

    this.refreshMetadataConnection();
    this.checkPassphrase();

  }

  /**
   * Check passphrase.
   * 
   * @throws Exception the exception
   */
  private void checkPassphrase() throws Exception {

    if (this.mEncryptionEnabled) {
      int id = this.getParameterListID("$INTERNAL");

      String[] o = null;
      if (id != -1)
        o = this.getParameterValue(id, "CHECK");

      if (id == -1 || o == null || o.length == 0) {
        String tmp =
            "<ETL><PARAMETER_LIST NAME=\"$INTERNAL\"><PARAMETER NAME=\"CHECK\"></PARAMETER></PARAMETER_LIST></ETL>";
        // Build a DOM out of the XML string...
        DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = dmf.newDocumentBuilder();
        Document xmlConfig = builder.parse(new InputSource(new StringReader(tmp)));
        this.importParameterList(xmlConfig.getElementsByTagName("PARAMETER_LIST").item(0));
        this.setParameterValue(this.getParameterListID("$INTERNAL"), "CHECK",
            this.mEncryptor.encrypt("KETLAVAYA"));
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

  /** The d start time. */
  private java.sql.Timestamp dStartTime = null;

  /**
   * Insert the method's description here. Creation date: (5/1/2002 7:56:40 PM)
   * 
   * @param pServerID int
   * @return boolean
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public int shutdownServer(int pServerID) throws SQLException, java.lang.Exception {
    PreparedStatement stmt1;
    PreparedStatement stmt2 = null;
    ResultSet m_rs = null;
    int returnStatus = -1;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();
      stmt1 =
          this.metadataConnection.prepareStatement("SELECT SHUTDOWN_NOW,"
              + this.currentTimeStampSyntax
              + ",STATUS_ID FROM  " + this.tablePrefix + "SERVER WHERE SERVER_ID = ?"); //$NON-NLS-1$

      stmt1.setInt(1, pServerID);

      m_rs = stmt1.executeQuery();

      stmt2 =
          this.metadataConnection
              .prepareStatement("UPDATE  "
                  + this.tablePrefix
                  + "SERVER SET STATUS_ID = ?,START_TIME = (coalesce(?,START_TIME)),SHUTDOWN_TIME = ?,LAST_PING_TIME = "
                  + this.currentTimeStampSyntax + ", SHUTDOWN_NOW = NULL WHERE SERVER_ID = ?");

      // cycle through results
      while (m_rs.next()) {
        java.sql.Timestamp td = m_rs.getTimestamp(2);
        int status_id = m_rs.getInt(3);

        String sd = m_rs.getString(1);

        if (this.dStartTime == null) {
          this.dStartTime = new java.sql.Timestamp(td.getTime());
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

          stmt2.setTimestamp(2, new java.sql.Timestamp(this.dStartTime.getTime()));
          stmt2.setNull(3, java.sql.Types.TIMESTAMP);
          stmt2.setInt(4, pServerID);
        } else if (sd.equalsIgnoreCase("Y")) { // must set flag back to
          // null //$NON-NLS-1$
          stmt2.setInt(1, ETLServerStatus.SERVER_SHUTDOWN);
          stmt2.setNull(2, java.sql.Types.TIMESTAMP);
          stmt2.setTimestamp(3, td);
          stmt2.setInt(4, pServerID);

          returnStatus = ETLServerStatus.SERVER_SHUTTING_DOWN;
        } else if (sd.equalsIgnoreCase("K")) { // must set flag back to
          // null //$NON-NLS-1$
          stmt2.setInt(1, ETLServerStatus.SERVER_KILLED);
          stmt2.setNull(2, java.sql.Types.TIMESTAMP);
          stmt2.setTimestamp(3, td);
          stmt2.setInt(4, pServerID);
          returnStatus = ETLServerStatus.SERVER_KILLED;
        }

        stmt2.executeUpdate();
        this.metadataConnection.commit();
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
   * Insert the method's description here. Creation date: (5/1/2002 7:56:40 PM)
   * 
   * @param pServerName the server name
   * @param bImmediate the b immediate
   * @return boolean
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean shutdownServer(String pServerName, boolean bImmediate) throws SQLException,
      java.lang.Exception {
    PreparedStatement stmt1;
    boolean shutdown = false;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();
      stmt1 =
          this.metadataConnection
              .prepareStatement("UPDATE " + this.tablePrefix + "SERVER set SHUTDOWN_NOW = ? WHERE SERVER_NAME = ?"); //$NON-NLS-1$

      if (bImmediate) {
        stmt1.setString(1, "K");
      } else {
        stmt1.setString(1, "Y");
      }

      stmt1.setString(2, pServerName);

      if (stmt1.executeUpdate() > 0) {
        shutdown = true;
      }

      this.metadataConnection.commit();

      if (stmt1 != null) {
        stmt1.close();
      }
    }

    return (shutdown);
  }

  /**
   * Insert the method's description here. Creation date: (5/1/2002 7:56:40 PM)
   * 
   * @param pServerID int
   * @param bImmediate the b immediate
   * @return boolean
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean shutdownServer(int pServerID, boolean bImmediate) throws SQLException,
      java.lang.Exception {
    PreparedStatement stmt1;
    boolean shutdown = false;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();
      stmt1 =
          this.metadataConnection
              .prepareStatement("UPDATE " + this.tablePrefix + "SERVER set SHUTDOWN_NOW = ? WHERE SERVER_ID = ?"); //$NON-NLS-1$

      if (bImmediate) {
        stmt1.setString(1, "K");
      } else {
        stmt1.setString(1, "Y");
      }

      stmt1.setInt(2, pServerID);

      if (stmt1.executeUpdate() > 0) {
        shutdown = true;
      }

      this.metadataConnection.commit();

      if (stmt1 != null) {
        stmt1.close();
      }
    }

    return (shutdown);
  }

  /**
   * Insert the method's description here. Creation date: (5/1/2002 7:56:40 PM)
   * 
   * @param pServerName the server name
   * @param bState the b state
   * @return boolean
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean pauseServer(String pServerName, boolean bState) throws SQLException,
      java.lang.Exception {
    PreparedStatement stmt1;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();
      stmt1 =
          this.metadataConnection
              .prepareStatement("UPDATE " + this.tablePrefix + "SERVER set STATUS_ID = ? WHERE SERVER_NAME = ?"); //$NON-NLS-1$

      if (bState) {
        stmt1.setInt(1, ETLServerStatus.PAUSED);
      } else {
        stmt1.setInt(1, ETLServerStatus.SERVER_ALIVE);
      }

      stmt1.setString(2, pServerName);

      stmt1.executeUpdate();

      this.metadataConnection.commit();

      if (stmt1 != null) {
        stmt1.close();
      }
    }

    return (true);
  }

  /**
   * Insert the method's description here. Creation date: (5/1/2002 7:56:40 PM)
   * 
   * @param pServerID int
   * @param bState the b state
   * @return boolean
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean pauseServer(int pServerID, boolean bState) throws SQLException,
      java.lang.Exception {
    PreparedStatement stmt1;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();
      stmt1 =
          this.metadataConnection
              .prepareStatement("UPDATE " + this.tablePrefix + "SERVER set STATUS_ID = ? WHERE SERVER_ID = ?"); //$NON-NLS-1$

      if (bState) {
        stmt1.setInt(1, ETLServerStatus.PAUSED);
      } else {
        stmt1.setInt(1, ETLServerStatus.SERVER_ALIVE);
      }

      stmt1.setInt(2, pServerID);

      stmt1.executeUpdate();

      this.metadataConnection.commit();

      if (stmt1 != null) {
        stmt1.close();
      }
    }

    return (true);
  }

  /**
   * Gets the KETL path.
   * 
   * @return Returns the mKETLPath.
   */
  public static final String getKETLPath() {
    if (Metadata.mKETLPath == null)
      return ".";
    return Metadata.mKETLPath;
  }

  /**
   * Schedule job.
   * 
   * @param pJobID the job ID
   * @param pSched the sched
   * @return Returns a new schedule id or -1 if failed.
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   * @author dnguyen 2006-07-27
   */
  public int scheduleJob(String pJobID, ETLJobSchedule pSched) throws SQLException,
      java.lang.Exception {

    // TODO create a class for the scheduler
    if (pJobID.length() == 0)
      return -1;
    if (!pSched.isScheduleValidated())
      return -1;
    PreparedStatement m_stmt_sel = null;
    PreparedStatement m_stmt_add = null;
    ResultSet m_rs = null;

    int pMonth = pSched.getMonth();
    int pMonthOfYear = pSched.getMonthOfYear();
    int pDay = pSched.getDay();
    int pDayOfWeek = pSched.getDayOfWeek();
    int pDayOfMonth = pSched.getDayOfMonth();
    int pHour = pSched.getHour();
    int pHourOfDay = pSched.getHourOfDay();
    int pMinute = pSched.getMinute();
    int pMinuteOfHour = pSched.getMinuteOfHour();
    String pDescription = pSched.getDescription();
    Date pOnceOnlyDate = pSched.getOnceOnlyDate();
    Date pEnableDate = pSched.getEnableDate();
    Date pNextRunDate = null;

    // TODO add pEnableDate & pDisableDate

    Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    Date pNow = cal.getTime();
    if (pOnceOnlyDate != null)
      pNextRunDate = pOnceOnlyDate;
    else if (pNextRunDate == null) {
      // if this value is not set, the job will never run. Default to
      // enable right now & run at next interval
      if (pEnableDate == null || pEnableDate.before(pNow))
        pEnableDate = pNow;
      // 1st, call this function w/o any increments (e.g. the schedule may
      // be later this year)
      pNextRunDate =
          Metadata.getNextDate(pEnableDate, -1, pMonthOfYear, -1, pDayOfWeek, pDayOfMonth, -1,
              pHourOfDay, -1, pMinuteOfHour);
      // 2nd, test if this date is in the past, then add the increments
      if (pNextRunDate == null || pNextRunDate.before(pNow))
        pNextRunDate =
            Metadata.getNextDate(pEnableDate, pMonth, pMonthOfYear, pDay, pDayOfWeek, pDayOfMonth,
                pHour, pHourOfDay, pMinute, pMinuteOfHour);
    }

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // A.make sure this exact schedule has not been created already
      String sql =
          "SELECT schedule_id FROM " + this.tablePrefix + "job_schedule WHERE job_id='" + pJobID
              + "'";
      if (pMonth > 0)
        sql += " AND month=" + pMonth;
      if (pMonthOfYear >= 0 && pMonthOfYear <= 11)
        sql += " AND month_of_year=" + pMonthOfYear; // in the xth
      // month of the
      // year 0-11
      if (pDay > 0)
        sql += " AND day=" + pDay; // every x days
      if (pDayOfWeek >= 0 && pDayOfWeek <= 6)
        sql += " AND day_of_week=" + pDayOfWeek; // 0-6 for Sun-Sat
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
      if (pOnceOnlyDate != null) {
        sql += " AND next_run_date=?";
      }

      m_stmt_sel = this.metadataConnection.prepareStatement(sql);
      if (pOnceOnlyDate != null)
        m_stmt_sel.setTimestamp(1, new Timestamp(pNextRunDate.getTime()));
      m_rs = m_stmt_sel.executeQuery();
      if (m_rs.next()) {
        int sched_id = m_rs.getInt(1);
        ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
            "Duplicate schedule found; existing schedule_id = " + sched_id);
        if (m_rs != null)
          m_rs.close();
        if (m_stmt_sel != null)
          m_stmt_sel.close();
        return -1;
      }

      // B.create a new schedule for this job
      m_stmt_add =
          this.metadataConnection
              .prepareStatement("INSERT INTO "
                  + this.tablePrefix
                  + "job_schedule (schedule_id, job_id, month, month_of_year, day, day_of_week, day_of_month,"
                  + "hour, hour_of_day, minute, minute_of_hour, next_run_date, schedule_desc) "
                  + "SELECT COALESCE(MAX(schedule_id)+1,1), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM "
                  + this.tablePrefix + "job_schedule ");
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
      if (pNextRunDate != null)
        m_stmt_add.setTimestamp(11, new Timestamp(pNextRunDate.getTime()));
      if (pDescription.length() > 0)
        m_stmt_add.setString(12, pDescription);
      m_stmt_add.executeUpdate();
      this.metadataConnection.commit();
      if (m_stmt_add != null)
        m_stmt_add.close();

      // C.get & return the new schedule_id
      try {
        if (pOnceOnlyDate != null)
          m_stmt_sel.setTimestamp(1, new Timestamp(pNextRunDate.getTime()));
        m_rs = m_stmt_sel.executeQuery();
        if (m_rs.next()) {
          int sched_id = m_rs.getInt(1);
          return sched_id;
        }
      } catch (Exception e) {
        ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
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

  /**
   * Sets the job schedule defaults.
   * 
   * @param pMonth the month
   * @param pMonthOfYear the month of year
   * @param pDay the day
   * @param pDayOfWeek the day of week
   * @param pDayOfMonth the day of month
   * @param pHour the hour
   * @param pHourOfDay the hour of day
   * @param pMinute the minute
   * @param pMinuteOfHour the minute of hour
   * @param pOnceOnlyDate the once only date
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  @SuppressWarnings("unused")
  private void setJobScheduleDefaults(int pMonth, int pMonthOfYear, int pDay, int pDayOfWeek,
      int pDayOfMonth, int pHour, int pHourOfDay, int pMinute, int pMinuteOfHour, Date pOnceOnlyDate)
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
        throw new Exception("Invalid job schedule value: Day-Of-Month values are 1-31.");
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
        throw new Exception("Invalid job schedule value: Day-Of-Week values are 1-7 for Sun-Sat.");
      else if ((pMonth <= 0))
        pDay = 7; // to set weekly job, set Day increment = 7
    }

    // if pHourOfDay is set and no day recurrance was set, then default to
    // every day
    if (pHourOfDay > 0) {
      if (pHourOfDay > 23)
        throw new Exception("Invalid job schedule value: Hour-Of-Day values are 0-23.");
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
   * Gets the execution errors.
   * 
   * @param pExecID int - returns all errors with this execution id
   * @param maxRows int - cap the number of rows to return; -1 is unlimited.
   * @param pLastModified the last modified
   * @return Returns a list of error objects.
   * @throws Exception the exception
   * @author dnguyen 2006-07-27
   */
  public ETLJobError[] getExecutionErrors(java.util.Date pLastModified, int pExecID, int maxRows)
      throws Exception {
    PreparedStatement m_stmt = null;
    ResultSet m_rs = null;
    ArrayList errors = new ArrayList();
    String sql;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      sql =
          "SELECT e.dm_load_id, e.job_id, e.message, e.code, e.error_datetime, "
              + "NULL as details, NULL as step_name FROM " + this.tablePrefix + "JOB_ERROR e"
              + " where e.error_datetime >= coalesce(?,e.error_datetime)"
              + " and e.dm_load_id = coalesce(?,e.dm_load_id)" + " union all "
              + "SELECT e.dm_load_id, e.job_id, e.message, e.code, e.error_datetime, "
              + "e.details, e.step_name FROM " + this.tablePrefix + "JOB_ERROR_HIST e"
              + " where e.error_datetime >= coalesce(?,e.error_datetime)"
              + " and e.dm_load_id = coalesce(?,e.dm_load_id)";

      m_stmt = this.metadataConnection.prepareStatement(sql);
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

  /**
   * Gets the current DB time stamp.
   * 
   * @return the current DB time stamp
   * @throws Exception the exception
   */
  public Date getCurrentDBTimeStamp() throws Exception {
    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      Date currDate = null;
      this.metadataConnection.commit();
      PreparedStatement stmt =
          this.metadataConnection.prepareStatement("SELECT " + this.currentTimeStampSyntax
              + " FROM " + this.tablePrefix + "SERVER_STATUS WHERE STATUS_ID = 1");

      ResultSet rs = stmt.executeQuery();
      if (rs.next())
        currDate = rs.getTimestamp(1);
      rs.close();

      if (stmt != null)
        stmt.close();
      return currDate;
    }
  }

  /**
   * Delete load.
   * 
   * @param pLoadIDs the load I ds
   * @return true, if successful
   * @throws SQLException the SQL exception
   * @throws Exception the exception
   */
  public boolean deleteLoad(String pLoadIDs) throws SQLException, java.lang.Exception {
    // pLoadIDs must be separated by commas
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // do not delete any loads that are currently executing (i.e. in
      // job_log or error_log)
      // Do the deletes in this order: 1st, delete the error history
      m_stmt =
          this.metadataConnection.prepareStatement("DELETE FROM " + this.tablePrefix
              + "job_error_hist WHERE dm_load_id IN (SELECT dm_load_id FROM " + this.tablePrefix
              + "job_log_hist WHERE load_id in (" + pLoadIDs + "))");
      m_stmt.executeUpdate();
      if (m_stmt != null)
        m_stmt.close();

      // 2nd, delete the log history
      m_stmt =
          this.metadataConnection.prepareStatement("DELETE FROM " + this.tablePrefix
              + "job_log_hist WHERE load_id in (" + pLoadIDs + ")");
      m_stmt.executeUpdate();
      if (m_stmt != null)
        m_stmt.close();

      // 3rd, delete the load if there's a load end date
      m_stmt =
          this.metadataConnection.prepareStatement("DELETE FROM " + this.tablePrefix
              + "load WHERE load_id in (" + pLoadIDs + ") and end_date is not null");
      m_stmt.executeUpdate();
      if (m_stmt != null)
        m_stmt.close();

      this.metadataConnection.commit();
    }

    return (true);
  }

  /**
   * Load table name.
   * 
   * @return the string
   */
  final protected String loadTableName() {
    return this.mResolvedLoadTableName == null ? "LOAD" : this.mResolvedLoadTableName;
  }

  public boolean executorAvailable(int jobTypeID, String pool) throws Exception {

    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // get executor info
      m_stmt =
          this.metadataConnection.prepareStatement("SELECT sum(THREADS) " + "	FROM "
              + this.tablePrefix + "SERVER_EXECUTOR A, " + this.tablePrefix + "JOB_EXECUTOR B, "
              + this.tablePrefix + "job_executor_job_type c, " + "	" + this.tablePrefix
              + "job_type d, " + this.tablePrefix + "server e "
              + " WHERE A.JOB_EXECUTOR_ID = B.JOB_EXECUTOR_ID "
              + " AND b.job_executor_id = c.job_executor_id "
              + " AND c.job_type_id = d.job_type_id  " + " AND e.server_id = a.server_id "
              + " and e.last_ping_time > sysdate - (1/1440) "
              + "  and d.job_type_id = ? and a.pool = ?");

      m_stmt.setInt(1, jobTypeID);
      m_stmt.setString(2, pool);

      ResultSet rs = m_stmt.executeQuery();
      int threads = 0;
      while (rs.next()) {
        threads = rs.getInt(1);
      }

      rs.close();

      if (m_stmt != null)
        m_stmt.close();

      if (threads == 0) {
        return false;
      }

      // get how many used
      m_stmt =
          this.metadataConnection
              .prepareStatement("select count(*) from "
                  + this.tablePrefix
                  + "job_log a join "
                  + this.tablePrefix
                  + "job b on (a.job_id = b.job_id) where status_id = 1 and b.job_type_id = ? and b.pool = ?");

      m_stmt.setInt(1, jobTypeID);
      m_stmt.setString(2, pool);
      rs = m_stmt.executeQuery();

      // reduce the number threads by the number used
      while (rs.next()) {
        threads = threads - rs.getInt(1);
      }

      rs.close();

      if (m_stmt != null)
        m_stmt.close();

      if (threads == 0) {
        return false;
      }

    }

    return true;
  }

  public boolean loadComplete(int loadId) throws SQLException, Exception {
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      // get executor info
      m_stmt =
          this.metadataConnection.prepareStatement("SELECT count(*)FROM " + this.tablePrefix
              + "JOB_LOG " + " WHERE LOAD_ID = ? ");

      m_stmt.setInt(1, loadId);

      ResultSet rs = m_stmt.executeQuery();
      int pendingJobs = 0;
      while (rs.next()) {
        pendingJobs = rs.getInt(1);
      }

      rs.close();

      if (m_stmt != null)
        m_stmt.close();

      if (pendingJobs > 0) {
        return false;
      }

    }

    return true;
  }

  public List<String[]> getObjectList(String domain, int loadId) throws SQLException, Exception {
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT object_id,object_name FROM "
              + this.tablePrefix + " EXT_OBJECT_LIST " + " WHERE DOMAIN_NAME = ? AND LOAD_ID = ? ");

      m_stmt.setString(1, domain.split("//")[1]);
      m_stmt.setInt(2, loadId);

      ResultSet rs = m_stmt.executeQuery();
      List<String[]> objects = new ArrayList<String[]>();
      while (rs.next()) {
        objects.add(new String[] {rs.getString(1), rs.getString(2)});
      }

      rs.close();

      if (m_stmt != null)
        m_stmt.close();
      return objects;
    }
  }

  public Map<String, JSONObject> getValueMapping(String arg0) throws SQLException, Exception {
    PreparedStatement m_stmt = null;

    synchronized (this.oLock) {
      // Make metadata connection alive.
      this.refreshMetadataConnection();

      m_stmt =
          this.metadataConnection.prepareStatement("SELECT field_name,config FROM "
              + this.tablePrefix + " FIELD_MAPPING " + " WHERE MAPPING_NAME = ? ");

      m_stmt.setString(1, arg0);

      ResultSet rs = m_stmt.executeQuery();
      Map<String, JSONObject> objects = new HashMap<String, JSONObject>();
      while (rs.next()) {
        objects.put(rs.getString(1), (JSONObject) JSONValue.parse(rs.getString(2)));
      }

      rs.close();

      if (m_stmt != null)
        m_stmt.close();
      return objects;
    }
  }
}
