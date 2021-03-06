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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.h2.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobExecutor;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.EngineConstants;
import com.kni.etl.Metadata;
import com.kni.etl.OSJobExecutor;
import com.kni.etl.SQLJobExecutor;
import com.kni.etl.TableauJobExecutor;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLCluster;
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;
import com.kni.util.ExternalJarLoader;
import com.kni.util.FileHelpers;

// TODO: Auto-generated Javadoc
/*
 * Created on Apr 4, 2005 To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
/**
 * The Class Console.
 */
public class Console {


  /**
   * The KETL Console allows for remote management of the KETL server Supported commands STATUS
   * SHUTDOWN {IMMEDIATE|NORMAL} STARTUP JOB <NAME> {DEFINITION|XMLDEFINITION|DEPENDENCIES} RESTART
   * {IMMEDIAITE|NORMAL} QUIT.
   */
  private enum COMMANDS {
    SHUTDOWN, STARTUP, STATUS, JOB, RESTART, QUIT, CONNECT, HELP, PARAMETERLIST, PARAMETER, PAUSE,
    RESUME, PROJECT, SERVER, RUN, REPEAT, INVALID, LIST, RESET, LOOKUPS, LOADID, ENABLETRIGGERS,
    FOREGROUND, XMLDEFINITION, DEFINITION, DEPENDENCIES, EXECUTE, IGNOREDEPENDENCIES, MULTI,
    IMPORT, EXPORT, DELETE, SKIP, EXECUTEDIRECT, KILL, NOTSPECIFIED, CANCEL_JOB;

    public String help() {
      switch (this) {
        case SHUTDOWN:
          return "SHUTDOWN <SERVERID> {IMMEDIATE|NORMAL}";
        case STATUS:
          return "STATUS {JOBS} {ALL}";
        case JOB:
          return "JOB <NAME> {DEFINITION|DELETE|KILL|XMLDEFINITION|RESTART|SKIP|EXPORT <FILENAME>|IMPORT <FILENAME>|DEPENDENCIES|EXECUTE <PROJECTID> {MULTI} {IGNOREDEPENDENCIES}}";
        case RESTART:
          return "RESTART {IMMEDIATE|NORMAL}";
        case CONNECT:
          return "CONNECT <SERVER|LOCALHOST> <USERNAME>";
        case PARAMETERLIST:
          return "PARAMETERLIST <NAME> <EXPORT|IMPORT|DEFINITION> {FILENAME}";
        case PARAMETER:
          return "PARAMETER SET|GET <PARAMETERLIST_NAME> <PARAMETER_NAME> {NEW_VALUE}";
        case QUIT:
          return "QUIT";
        case PAUSE:
          return "PAUSE <SERVERID>";
        case RESUME:
          return "RESUME <SERVERID>";
        case PROJECT:
          return "PROJECT LIST";
        case SERVER:
          return "SERVER LIST";
        case RUN:
          return "RUN {LIST|RESET|LOOKUPS|ENABLETRIGGERS <TRUE|FALSE>|<FILENAME>|FOREGROUND <JOBID>|LOADID <VALUE>}";
        case REPEAT:
          return "/ {<REPEAT>} {<SECONDS BETWEEN REPEAT>}";
        default:
          return null;
      }
    }


  };



  /**
   * Display version info.
   * 
   * @return the string
   */
  private static String displayVersionInfo() {
    EngineConstants.getVersion();
    return "KETL Console\n";
  }

  /**
   * The main method.
   * 
   * @param args the args
   */
  public static void main(String[] args) {
    String ketldir = System.getenv("KETLDIR");
    if (ketldir == null) {
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
          "KETLDIR not set, defaulting to working dir");
      ketldir = ".";
    }

    ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf" + File.separator
        + "Extra.Libraries"), "ketlextralibs", ";");
    ResourcePool.setCacheIndexPrefix("Console");
    // create console object
    Console console = new Console();

    Thread.currentThread().setName("KETL Console");

    // accept commands
    console.run(args);

    ResourcePool.releaseAllLookups();

  }

  /** The driver. */
  String driver;

  /** The input stream reader. */
  InputStreamReader inputStreamReader = new InputStreamReader(System.in);

  /** The k job exec. */
  ETLJobExecutor kJobExec = new KETLJobExecutor();

  /** The md. */
  Metadata md = null;

  /** The mdprefix. */
  String mdprefix;

  /** The previous commands. */
  ArrayList<String> mPreviousCommands = new ArrayList<String>();

  /** The n current server. */
  Node nCurrentServer;

  /** The os job exec. */
  ETLJobExecutor osJobExec = new OSJobExecutor();
  ETLJobExecutor tableauJobExec = new TableauJobExecutor();

  /** The password. */
  String password;

  /** The servername. */
  String servername = null;

  /** The sql job exec. */
  ETLJobExecutor sqlJobExec = new SQLJobExecutor();

  /** The stdin. */
  BufferedReader stdin = new BufferedReader(this.inputStreamReader);

  /** The url. */
  String url;

  /** The username. */
  String username;

  /** The xml config. */
  Document xmlConfig = null;

  private boolean enableTriggers;

  private int prevLoadId;

  /**
   * Connected.
   * 
   * @return true, if successful
   */
  private boolean connected() {
    if (this.md != null) {
      return true;
    }

    System.out.println("ERROR: currently disconnected!");

    return false;
  }

  /**
   * Connect to server.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private String connectToServer(String[] pCommands) throws IOException {
    if (this.md != null) {
      this.md.closeMetadata();
      this.md = null;
    }

    // if number of commands greater than 3 then syntax error
    if (pCommands.length > 3) {
      return this.syntaxError(COMMANDS.CONNECT);
    }

    // get server to connect to.
    if (pCommands.length > 1) // use named server
    {
      this.servername = pCommands[1];
    } else if (pCommands.length == 1) // use default server
    {
      Node n = XMLHelper.findElementByName(this.xmlConfig, "SERVERS", null, null);
      if (n == null) {
        ResourcePool
            .LogMessage("KETLServers.xml is missing the root node SERVERS, please review file");
        System.exit(-1);
      }
      this.servername = XMLHelper.getAttributeAsString(n.getAttributes(), "DEFAULTSERVER", "");
    }

    if (this.servername.equalsIgnoreCase("LOCALHOST")) {
      try {
        InetAddress thisIp = InetAddress.getLocalHost();
        this.servername = thisIp.getHostName();

        // try for localhost
        this.nCurrentServer =
            XMLHelper.findElementByName(this.xmlConfig, "SERVER", "NAME", "localhost");

        if (this.nCurrentServer == null) {
          this.nCurrentServer =
              XMLHelper.findElementByName(this.xmlConfig, "SERVER", "NAME", "LOCALHOST");
        }

        // try for explicit name
        if (this.nCurrentServer == null) {
          this.nCurrentServer =
              XMLHelper.findElementByName(this.xmlConfig, "SERVER", "NAME", this.servername);
        }
      } catch (UnknownHostException e) {
        return "Connection failure: Could not get system hostname please supply servername";
      }
    } else {
      this.nCurrentServer =
          XMLHelper.findElementByName(this.xmlConfig, "SERVER", "NAME", this.servername);
    }

    if (this.nCurrentServer == null) {
      return "ERROR: Server " + this.servername + " not found!";
    }

    if (pCommands.length == 3) {
      // use specified login and prompt for password
      System.out.print("Enter server password:");
      this.password = this.stdin.readLine();
      this.username = pCommands[2];
    } else {
      this.username =
          XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "USERNAME", null, null, null);
      this.password =
          XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "PASSWORD", null, null, null);
    }

    this.url = XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "URL", null, null, null);
    this.driver =
        XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "DRIVER", null, null, null);
    this.mdprefix =
        XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "MDPREFIX", null, null, null);
    String passphrase =
        XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "PASSPHRASE", null, null, null);

    // metadata object isn't set and login information found then connect to
    // metadata
    if ((this.md == null) && (this.username != null)) {

      try {
        Metadata mds = new Metadata(true, passphrase);
        mds.setRepository(this.username, this.password, this.url, this.driver, this.mdprefix);
        this.servername =
            XMLHelper.getAttributeAsString(this.nCurrentServer.getAttributes(), "NAME",
                this.servername);
        ResourcePool.setMetadata(mds);
        this.md = ResourcePool.getMetadata();

      } catch (Exception e1) {
        return ("ERROR: Connecting to metadata - " + e1.getMessage());
      }
    }

    return "Connected to " + this.servername;
  }

  /**
   * Help.
   * 
   * @param pCommands the commands
   * @return the string
   */
  String help(String[] pCommands) {
    StringBuffer sb = new StringBuffer();

    sb.append("KETL Console allows remote administration of a KETL\ncluster, the following commands are available.\n\n");

    for (COMMANDS element : COMMANDS.values()) {
      if (element.help() != null)
        sb.append("\t" + element.help() + "\n");
    }

    return sb.toString();
  }

  /**
   * Import jobs.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String importJobs(String[] pCommands) throws Exception {
    String[] files = FileHelpers.getFilenames(pCommands[3]);

    if (files == null || files.length == 0)
      return "ERROR: File not found - " + pCommands[3];

    for (String element : files) {

      Document jobNodes = null;
      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Reading file " + element);

      // turn file into readable nodes
      DocumentBuilder builder = null;

      // Build a DOM out of the XML string...
      try {
        DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
        builder = dmf.newDocumentBuilder();
        jobNodes = builder.parse(new InputSource(new FileReader(element)));

        NodeList nl = jobNodes.getElementsByTagName("JOB");

        for (int i = 0; i < nl.getLength(); i++) {
          Node job = nl.item(i);
          this.md.importJob(job);
        }
      } catch (org.xml.sax.SAXException e) {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Parsing XML document, " + e.toString());

        System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
      } catch (Exception e) {
        ResourcePool.LogException(e, this);

        System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
      }
    }
    return "Done";
  }

  /**
   * Import parameters.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String importParameters(String[] pCommands) throws Exception {
    String[] files = FileHelpers.getFilenames(pCommands[3]);

    for (String element : files) {

      StringBuffer sb = new StringBuffer();
      Document jobNodes = null;
      FileReader inputFileReader = null;

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Reading file " + element);
      inputFileReader = new FileReader(element);
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
        jobNodes = builder.parse(new InputSource(new StringReader(sb.toString())));

        NodeList nl = jobNodes.getElementsByTagName("PARAMETER_LIST");

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
            "WARNING: Any duplicate parameters will be given the last value found");

        for (int i = 0; i < nl.getLength(); i++) {
          Node parameterList = nl.item(i);

          this.md.importParameterList(parameterList);
        }
      } catch (org.xml.sax.SAXException e) {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Parsing XML document, " + e.toString());

        System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
      } catch (Exception e) {
        ResourcePool.LogException(e, this);

        System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
      }
    }
    return "Done";
  }

  /**
   * Job details.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String job(String[] pCommands) throws Exception {
    StringBuffer sb = new StringBuffer();
    StringBuffer sbJobList = new StringBuffer("Export Jobs:\n");
    boolean deleteAll = false;

    if (this.connected()) {
      // if its an export then load from file
      if (this.resolveCommand(pCommands[2]) == COMMANDS.IMPORT) {
        return this.importJobs(pCommands);
      }

      if (this.resolveCommand(pCommands[2]) == COMMANDS.LIST
          || this.resolveCommand(pCommands[2]) == COMMANDS.NOTSPECIFIED) {
        return this.listJobs(pCommands[1]);
      }
      /*
       * else if ((pCommands.length > 2) && (resolveCommand(pCommands[2], JOBDETAIL_TYPES) ==
       * EXECUTEDIRECT)) { return executeDirect(pCommands); }
       */
      else if (pCommands.length > 1) {
        try {
          ETLJob[] jobs = this.md.getJobDetails(pCommands[1].replaceAll("\\*", "%"));

          COMMANDS jobDetailType = COMMANDS.DEFINITION;

          if (pCommands.length > 2) {
            jobDetailType = this.resolveCommand(pCommands[2]);
          }

          if ((jobDetailType == COMMANDS.XMLDEFINITION) || (jobDetailType == COMMANDS.EXPORT)) {
            sb.append("<?xml version=\"1.0\"?>\n<ETL VERSION=\"" + this.md.getMetadataVersion()
                + "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">");
          }

          if ((jobDetailType == COMMANDS.DELETE)) {
            System.out
                .println("WARNING: Deleting jobs can result in lost dependencies\nIt is recommended that you export job definition first!");
          }

          if (jobs == null || jobs.length == 0) {
            if (this.md.getJob(pCommands[1]) == null) {
              sb.append("Job(s) do not exist");
            }
          }

          for (int i = 0; i < jobs.length; i++) {
            ETLJob eJob = jobs[i];

            // resolve output type
            switch (jobDetailType) {
              case XMLDEFINITION:
                sb.append("\n\n<!--- Job: " + eJob.getJobID() + "-->\n");
                sb.append(eJob.getXMLJobDefinition());

                break;

              case EXPORT:

                if (pCommands.length < 4) {
                  ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                      "ERROR: No export file given");

                  return this.syntaxError(COMMANDS.JOB);
                }

                sb.append("\n\n<!--- Job: " + eJob.getJobID() + "-->\n");
                sbJobList.append("  " + eJob.getJobID() + "\n");
                sb.append(eJob.getXMLJobDefinition());

                break;

              case DEFINITION:
                sb.append(eJob.getJobDefinition());

                break;

              case RESTART:

                String choice = "Y";
                this.md.getJobStatus(eJob);

                if (eJob.getStatus().getStatusCode() != ETLJobStatus.SCHEDULED) {
                  if (eJob.getStatus().getStatusCode() == ETLJobStatus.EXECUTING) {
                    System.out
                        .print("Job "
                            + eJob.getJobID()
                            + " is marked as executing, only do this if you are recovering from a server that has been killed! Y(Yes), N(No): ");
                    choice = this.stdin.readLine();
                  } else if ((eJob.getStatus().getStatusCode() != ETLJobStatus.FAILED)
                      && (eJob.getStatus().getStatusCode() != ETLJobStatus.PENDING_CLOSURE_FAILED)) {
                    System.out.print("Job " + eJob.getJobID()
                        + " has not failed do you really want to restart it Y(Yes), N(No): ");
                    choice = this.stdin.readLine();
                  }

                  if (choice.equalsIgnoreCase("Y")) {
                    eJob.getStatus().setStatusCode(ETLJobStatus.READY_TO_RUN);
                    this.md.setJobStatus(eJob);
                    sb.append("Job: " + eJob.getJobID() + " restarted");
                  }
                } else {
                  sb.append("Job: " + eJob.getJobID()
                      + " can not be restarted as it is running or not in the current run list");
                }

                break;
              case CANCEL_JOB:

                choice = "Y";
                this.md.getJobStatus(eJob);

                if ((eJob.getStatus().getStatusCode() != ETLJobStatus.EXECUTING)
                    && (eJob.getStatus().getStatusCode() != ETLJobStatus.SCHEDULED)) {
                  if ((eJob.getStatus().getStatusCode() != ETLJobStatus.PENDING_CLOSURE_FAILED)
                      && (eJob.getStatus().getStatusCode() != ETLJobStatus.FAILED)) {
                    System.out.print("Job " + eJob.getJobID()
                        + " has not failed do you really want to cancel it Y(Yes), N(No): ");
                    choice = this.stdin.readLine();
                  }

                  if (choice.equalsIgnoreCase("Y")) {
                    eJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_CANCELLED);
                    this.md.setJobStatus(eJob);
                    sb.append("Job: " + eJob.getJobID() + " cancelled");
                  }
                } else if (eJob.getStatus().getStatusCode() == ETLJobStatus.EXECUTING) {
                  System.out.print("Job " + eJob.getJobID()
                      + " is executing do you really want to cancel it Y(Yes), N(No): ");
                  choice = this.stdin.readLine();

                  if (choice.equalsIgnoreCase("Y")) {
                    eJob.getStatus().setStatusCode(ETLJobStatus.ATTEMPT_CANCEL);
                    this.md.setJobStatus(eJob);
                    sb.append("Job: " + eJob.getJobID() + " cancelled");
                  }
                } else {
                  sb.append("Job: " + eJob.getJobID()
                      + " can not be cancelled as it is not in the current run list");
                }

                break;

              case SKIP:
                this.md.getJobStatus(eJob);

                if ((eJob.getStatus().getStatusCode() != ETLJobStatus.EXECUTING)
                    && (eJob.getStatus().getStatusCode() != ETLJobStatus.SCHEDULED)) {
                  eJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL);
                  this.md.setJobStatus(eJob);
                  sb.append("Job: " + eJob.getJobID() + " skipped, it will appear as successful");
                } else {
                  sb.append("Job: " + eJob.getJobID()
                      + " can not be skipped as it is running or not in the current run list");
                }

                break;

              case DEPENDENCIES:
                sb.append("Job: " + eJob.getJobID() + "\n");
                sb.append(eJob.getDependencies() + "\n");

                break;

              case DELETE:

                String opt = "";

                while (opt != null) {
                  if (deleteAll == false) {
                    System.out.print("Delete job " + eJob.getJobID()
                        + " Y(Yes), N(No), S(Skip all other jobs), A(Yes for All): ");
                    opt = this.stdin.readLine();
                  } else {
                    opt = "Y";
                  }

                  switch (opt.toUpperCase().charAt(0)) {
                    case 'Y':
                      this.md.deleteJob(eJob.getJobID());
                      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deleted job: "
                          + eJob.getJobID());
                      opt = null;

                      break;

                    case 'N':
                      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Not deleting job: "
                          + eJob.getJobID());
                      opt = null;

                      break;

                    case 'A':
                      this.md.deleteJob(eJob.getJobID());
                      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deleted job: "
                          + eJob.getJobID());
                      opt = null;
                      deleteAll = true;

                      break;

                    case 'S':
                      sb.append("Skipping all other jobs.\n");
                      i = jobs.length;
                      opt = null;

                      break;

                    default:
                      System.out.println("Invalid option");

                      break;
                  }
                }

                break;

              case EXECUTE:

                boolean ignoreDeps = false;
                boolean allowMult = false;
                COMMANDS opt1 = COMMANDS.NOTSPECIFIED;
                COMMANDS opt2 = COMMANDS.NOTSPECIFIED;

                if (pCommands.length < 4) {
                  return this.syntaxError(COMMANDS.JOB);
                }

                if (pCommands.length > 4) {
                  opt1 = this.resolveCommand(pCommands[4]);
                }

                if (pCommands.length > 5) {
                  opt2 = this.resolveCommand(pCommands[5]);
                }

                if ((opt1 == COMMANDS.IGNOREDEPENDENCIES) || (opt2 == COMMANDS.IGNOREDEPENDENCIES)) {
                  ignoreDeps = true;
                }

                if ((opt1 == COMMANDS.MULTI) || (opt2 == COMMANDS.MULTI)) {
                  allowMult = true;
                }

                int pID;

                try {
                  pID = Integer.parseInt(pCommands[3]);

                  int loadId;
                  if ((loadId = this.md.executeJob(pID, pCommands[1], ignoreDeps, allowMult, null)) != -1) {
                    sb.append("Job submitted to server for direct execution, load id = " + loadId
                        + ".\n");
                  } else {
                    sb.append("Warning Job not submitted to server for execution.\n");
                  }
                } catch (Exception e) {
                  ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Invalid project id\n");
                  sb.append("Job not submitted");
                }

                break;
            }
          }

          if ((jobDetailType == COMMANDS.XMLDEFINITION) || (jobDetailType == COMMANDS.EXPORT)) {
            sb.append("\n</ETL>");
          }

          if (jobDetailType == COMMANDS.EXPORT) {
            BufferedWriter out = new BufferedWriter(new java.io.FileWriter(pCommands[3]));
            out.write(sb.toString());
            out.close();
            sb = sbJobList.append("Done.");
          }
        } catch (Exception e) {
          ResourcePool.LogException(e, this);
        }
      } else {
        return this.syntaxError(COMMANDS.JOB);
      }
    }

    return sb.toString();
  }

  private String listJobs(String pJobID) throws SQLException, Exception {

    List<ETLJob> jobs = this.md.listJobs(pJobID);
    StringBuffer sb = new StringBuffer();

    if (jobs.size() == 0)
      return "No jobs found";

    int[] len = new int[6];
    for (ETLJob j : jobs) {
      len[0] = Math.max(len[0], j.getJobID().length());
      len[1] = Math.max(len[1], Integer.toString(j.getProjectID()).length());
      len[2] = Math.max(len[2], j.getJobTypeName().length());
      len[3] = Math.max(len[3], j.getPool().length());
      len[4] = Math.max(len[4], Integer.toString(j.getPriority()).length());
    }

    String[] titles = new String[] {"Job", "Project", "Type", "Pool", "Priority"};

    for (int i = 0; i < titles.length; i++) {
      len[i] = Math.max(len[i], titles[i].length());
      sb.append(String.format("%-" + len[i] + "s ", titles[i]));
    }
    sb.append("\n");
    for (int i = 0; i < titles.length; i++) {
      sb.append(StringUtils.pad("-", len[i], "-", true));
      sb.append(" ");
    }
    sb.append("\n");
    for (ETLJob j : jobs) {
      sb.append(String.format("%-" + len[0] + "s ", j.getJobID()));
      sb.append(String.format("%-" + len[1] + "s ", j.getProjectID()));
      sb.append(String.format("%-" + len[2] + "s ", j.getJobTypeName()));
      sb.append(String.format("%-" + len[3] + "s ", j.getPool()));
      sb.append(String.format("%-" + len[4] + "s ", j.getPriority()));
      sb.append("\n");
    }

    return sb.toString();
  }

  /**
   * Job status table.
   * 
   * @param pJobs the jobs
   * @param pTableHeader the table header
   * @return the string
   */
  String jobStatusTable(Object[][] pJobs, String pTableHeader, boolean compact) {
    if (pJobs == null || pJobs.length == 0) {
      return "";
    }

    StringBuffer sb = new StringBuffer();
    sb.append(pTableHeader);
    sb.append("\n");

    for (int i = 0; i < pTableHeader.length(); i++) {
      sb.append("-");
    }

    sb.append("\n");

    int[] widths = new int[pJobs[0].length];
    for (Object[] element : pJobs) {
      for (int x = 0; x < element.length; x++) {
        if (element[x] != null) {
          String tmp;
          if (element[x] instanceof Date)
            tmp = dateFormatter.format(element[x]);
          else
            tmp = element[x].toString();
          widths[x] = widths[x] < tmp.length() ? tmp.length() : widths[x];

          if (compact && widths[x] > 128) {
            widths[x] = 128;
          }
        }
      }
    }

    for (Object[] element : pJobs) {

      for (int x = 0; x < element.length; x++) {
        sb.append(fixedWidth(element[x], widths[x], compact));

        if (x < (element.length - 1)) {
          sb.append(" | ");
        }
      }

      sb.append("\n");

    }

    sb.append("\n");

    return sb.toString();
  }

  private static SimpleDateFormat dateFormatter = new SimpleDateFormat("MM-dd-yy HH:mm:ss");

  private String fixedWidth(Object object, int len, boolean compact) {

    String tmp;
    if (object == null)
      tmp = "";
    else if (object instanceof Date) {
      tmp = dateFormatter.format(object);
    } else
      tmp = object.toString();

    if (compact) {
      tmp = tmp.replace("\n", " ").replace("\r", " ");
      if (tmp.length() > len)
        tmp = tmp.substring(0, len);
    }

    StringBuffer sb = new StringBuffer(tmp);

    while (sb.length() < len)
      sb.append(" ");

    return sb.toString();
  }

  /**
   * Parameter list.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String parameterList(String[] pCommands) throws Exception {

    if (this.connected()) {
      // if its an export then load from file
      if ((pCommands.length > 2) && (this.resolveCommand(pCommands[2]) == COMMANDS.IMPORT)) {
        return this.importParameters(pCommands);
      } else if ((pCommands.length > 2)
          && (this.resolveCommand(pCommands[2]) == COMMANDS.DEFINITION)) {

        return this.getXMLParameterlistDefinition(pCommands[1]);

      } else if ((pCommands.length > 2) && (this.resolveCommand(pCommands[2]) == COMMANDS.EXPORT)) {
        if (pCommands.length < 4) {
          ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "ERROR: No export file given");

          return this.syntaxError(COMMANDS.PARAMETERLIST);
        }

        BufferedWriter out = new BufferedWriter(new java.io.FileWriter(pCommands[3]));
        out.write(this.getXMLParameterlistDefinition(pCommands[1]));
        out.close();
      }
    }

    return "Done";
  }

  /**
   * Gets the XML parameterlist definition.
   * 
   * @param pListMatchString the list match string
   * @return the XML parameterlist definition
   */
  private String getXMLParameterlistDefinition(String pListMatchString) {
    StringBuffer sb = new StringBuffer();

    String[] pLists = this.md.getValidParameterListName(pListMatchString.replaceAll("\\*", "%"));

    sb.append("<?xml version=\"1.0\"?>\n<ETL VERSION=\"" + this.md.getMetadataVersion()
        + "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");

    if (pLists != null) {
      for (String element : pLists) {
        sb.append("  <PARAMETER_LIST NAME=\"" + element + "\">\n");

        Object[][] pList = this.md.getParameterList(element);

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Fetching: " + element);

        if (pList != null) {
          for (Object[] element0 : pList) {
            String sub = "";
            String val = "/>\n";

            if (element0[Metadata.SUB_PARAMETER_LIST_NAME] != null) {
              sub = " PARAMETER_LIST=\"" + element0[Metadata.SUB_PARAMETER_LIST_NAME] + "\" ";
            }

            if (element0[Metadata.PARAMETER_VALUE] != null) {
              val = " >" + element0[Metadata.PARAMETER_VALUE] + "</PARAMETER>\n";
            }

            sb.append("     <PARAMETER NAME=\"" + element0[Metadata.PARAMETER_NAME] + "\"" + sub
                + val);
          }
        }

        sb.append("  </PARAMETER_LIST>\n");
      }
    }

    sb.append("\n</ETL>");

    return sb.toString();
  }

  /**
   * Pause.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String pause(String[] pCommands) throws Exception {
    if (this.connected()) {

      if (pCommands.length < 2)
        return "ERROR: Server id is missing";

      int i;
      try {
        i = Integer.parseInt(pCommands[1]);
      } catch (NumberFormatException e) {
        return "Server ID not a valid number = " + pCommands[1];
      }
      if (this.md.pauseServer(i, true)) {
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Request to pause server made");
        return this.server(new String[] {"SERVER", "LIST"});
      }
    }

    return "";
  }

  /**
   * Project.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String project(String[] pCommands) throws Exception {
    StringBuffer sb = new StringBuffer("Project(s)\nID\tDescription\n--\t-----------\n");
    if (this.connected()) {
      Object[] result = this.md.getProjects();
      for (Object element : result) {
        Object[] tmp = (Object[]) element;
        sb.append(tmp[0]);
        sb.append('\t');
        sb.append(tmp[1]);
        sb.append('\n');
      }
    }

    return sb.toString();
  }

  /**
   * Resolve command.
   * 
   * @param pCommand the command
   * @param pCommandList the command list
   * @return the int
   */
  private COMMANDS resolveCommand(String pCommand) {
    if (pCommand.equals("/")) {
      return COMMANDS.REPEAT;
    }

    try {
      return COMMANDS.valueOf(pCommand.toUpperCase());
    } catch (Exception e) {
      return COMMANDS.NOTSPECIFIED;
    }
  }

  /**
   * Restart.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String restart(String[] pCommands) throws Exception {
    if (this.connected()) {
      this.shutdown(pCommands);
      this.startup(pCommands);
    }

    return "";
  }

  /**
   * Resume.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String resume(String[] pCommands) throws Exception {
    if (this.connected()) {

      if (pCommands.length < 2)
        return "ERROR: Server id is missing";

      int i;
      try {
        i = Integer.parseInt(pCommands[1]);
      } catch (NumberFormatException e) {
        return "Server ID not a valid number = " + pCommands[1];
      }
      if (this.md.pauseServer(i, false)) {
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Request to resume server made");
        return this.server(new String[] {"SERVER", "LIST"});
      }
    }

    return "";
  }

  /**
   * Run.
   * 
   * @param args the args
   */
  void run(String[] args) {
    try {
      String configFile = Metadata.CONFIG_FILE;

      if ((args != null) && (args.length > 0)) {
        configFile = args[0];
      }

      // EngineConstants.getSystemXML();
      this.xmlConfig = Metadata.LoadConfigFile(null, configFile);

      Console.displayVersionInfo();
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Console");

      String[] quickCommand = null;
      int command = 0;

      if (args.length > 1) {
        quickCommand = args[1].split(";");
      }

      // Pass each command
      boolean hasRequestedQuit = false;
      String line = null, last = null;
      int repeat = 0;
      long wait = 2000;

      while (!hasRequestedQuit) {
        try {
          if (quickCommand == null) {
            System.out.print("->");
            if (repeat-- > 0) {
              Thread.sleep(wait);
              line = last;
            } else
              line = this.stdin.readLine();

            if (last != null && line != null && line.startsWith("/")) {
              String[] commands = ArgumentParserUtil.splitQuoteAware(line);
              line = last;
              if (commands.length > 1) {
                repeat = Integer.parseInt(commands[1]);
                if (repeat < 1 || repeat > 25) {
                  throw new Exception("Repeat value must be between 1 and 25");
                }
              }

              if (commands.length > 2) {
                wait = Integer.parseInt(commands[2]) * 1000;
                if (wait < 1000 || wait > 60000) {
                  throw new Exception("Wait value must be between 1 and 60 seconds");
                }
              }
            } else
              last = line;
          } else {
            if (command < quickCommand.length) {
              line = quickCommand[command++];
            } else {
              line = COMMANDS.QUIT.name();
            }
          }

          if (line != null) {
            String[] commands = ArgumentParserUtil.splitQuoteAware(line);

            if ((commands != null) && (commands.length > 0)) {
              String res = "";

              switch (this.resolveCommand(commands[0])) {
                case SHUTDOWN:
                  res = this.shutdown(commands);

                  break;

                case RESTART:
                  res = this.restart(commands);

                  break;

                case STARTUP:
                  res = this.startup(commands);

                  break;
                case PROJECT:
                  res = this.project(commands);

                  break;

                case JOB:
                  res = this.job(commands);

                  break;

                case PARAMETERLIST:
                  res = this.parameterList(commands);

                  break;
                case PARAMETER:
                  res = this.parameter(commands);

                  break;
                case RUN:
                  res = this.runJob(commands);

                  break;

                case CONNECT:
                  res = this.connectToServer(commands);

                  break;

                case HELP:
                  res = this.help(commands);

                  break;
                case SERVER:
                  res = this.server(commands);

                  break;

                case QUIT:
                  hasRequestedQuit = true;

                  return;

                case STATUS:
                  res = this.status(commands);

                  break;

                case PAUSE:
                  res = this.pause(commands);

                  break;

                case RESUME:
                  res = this.resume(commands);

                  break;

                default:
                  res = this.unknownCommand(commands[0]);

                  break;
              }

              System.out.println(res);
            }
          }
        } catch (Exception ex) {
          ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error:"
              + (ex.getCause() != null && ex instanceof RuntimeException ? ex.getCause().toString()
                  : ex.toString()));
        }
      }
    } finally {
      if (this.md != null)
        this.md.closeMetadata();
    }

  }

  private String parameter(String[] commands) {
    if (this.connected()) {

      if (commands.length < 4)
        return "Syntax error:" + COMMANDS.PARAMETER.help();

      boolean set = commands[1].equalsIgnoreCase("SET");

      if (set && commands.length != 5) {
        return "Syntax error, set parameters:" + COMMANDS.PARAMETER.help();
      } else if (!set && commands.length != 4) {
        return "Syntax error, get parameters:" + COMMANDS.PARAMETER.help();
      }
      int id = this.md.getParameterListID(commands[2]);
      if (id < 0)
        return "Invalid parameter list:" + commands[2];

      if (set) {
        try {
          if (this.md.setParameterValue(id, commands[3], commands[4]))
            return "Parameter updated";
          return "Failed to update";
        } catch (Exception e) {
          return e.getMessage();
        }
      } else {
        return "Paramter value: " + this.md.getParameterValue(id, commands[3]);
      }
    }

    return "";
  }

  /**
   * Run job.
   * 
   * @param pCommands the commands
   * @return the string
   */
  private String runJob(String[] pCommands) throws Exception {
    StringBuffer sb = new StringBuffer();

    try {
      this.prevLoadId = this.iLoadID;

      COMMANDS jobDetailType = COMMANDS.DEFINITION;

      if (pCommands.length > 1) {
        jobDetailType = this.resolveCommand(pCommands[1]);
      } else {
        return this.syntaxError(COMMANDS.RUN);
      }

      // resolve output type
      switch (jobDetailType) {
        case LOADID:
          try {
            if (pCommands.length == 3)
              this.iLoadID = Integer.parseInt(pCommands[2]);
            else
              return "Invalid syntax";
          } catch (Exception e) {
            e.printStackTrace();
          }

          return "LoadID = " + this.iLoadID;
        case ENABLETRIGGERS:
          try {
            if (pCommands.length == 3)
              this.enableTriggers = Boolean.parseBoolean(pCommands[2]);
            else
              return "Invalid syntax - run enabletriggers true|false";
          } catch (Exception e) {
            e.printStackTrace();
          }

          return "Triggers: " + (this.enableTriggers ? "Enabled" : "Disabled");
        case RESET:
          if (pCommands.length == 3) {
            if (ResourcePool.releaseLookup(pCommands[2]))
              return "Released lookup " + pCommands[2];
            else
              return "Could not find lookup" + pCommands[2];
          } else {
            ResourcePool.releaseLoadLookups(this.iLoadID);
            return "All cached resources release";
          }

        case LIST:
          for (int i = 0; i < this.mPreviousCommands.size(); i++)
            sb.append("" + (i + 1) + ".\t" + this.mPreviousCommands.get(i) + "\n");
          return sb.toString();
        case LOOKUPS:
          System.gc();
          List<String> list = ResourcePool.getLookups(this.iLoadID);
          for (int i = 0; i < list.size(); i++)
            sb.append("" + (i + 1) + ".\t" + list.get(i) + "\n");
          return sb.toString();
        case FOREGROUND:
          if (pCommands.length < 3)
            return "Invalid syntax - run foreground <jobid> {load_id}";

          if (pCommands.length == 4)
            this.iLoadID = Integer.parseInt(pCommands[3]);

          ETLJob[] jobs = this.md.getJobDetails(pCommands[2].replaceAll("\\*", "%"));

          File f = File.createTempFile("ketl", ".tmp");
          BufferedWriter out = new BufferedWriter(new java.io.FileWriter(f));

          out.write("<?xml version=\"1.0\"?>\n<ETL VERSION=\"" + this.md.getMetadataVersion()
              + "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");

          for (ETLJob job : jobs) {
            out.write(job.getXMLJobDefinition());
          }

          out.write("\n</ETL>");
          out.close();
          f.deleteOnExit();
          pCommands = new String[] {"run", f.getAbsolutePath()};
        default:
          if (pCommands.length == 2) {

            String file = pCommands[1];
            try {
              file = this.mPreviousCommands.get(Integer.parseInt(pCommands[1]) - 1);
            } catch (Exception e) {
            }

            this.mPreviousCommands.remove(file);
            this.mPreviousCommands.add(0, file);

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Executing file " + file);
            Document doc;
            try {
              doc = XMLHelper.readXMLFromFile(file);
            } catch (Exception e) {
              ResourcePool.logException(e);
              return e.toString();
            }

            if (doc == null)
              return "File \'" + file + "\' not found";

            NodeList nl = doc.getElementsByTagName("JOB");

            for (int i = 0; i < nl.getLength(); i++) {
              Node nd = nl.item(i);

              String jobID = XMLHelper.getAttributeAsString(nd.getAttributes(), "ID", null);
              if (jobID == null)
                return "Job does not contain an ID attribute, aborting..";

              String type = XMLHelper.getAttributeAsString(nd.getAttributes(), "TYPE", null);
              ETLJobExecutor cur = null;
              if (type.startsWith("KETL")) {
                cur = this.kJobExec;
              } else if (type.equals("SQL")) {
                cur = this.sqlJobExec;
              } else if (type.equals("OSJOB")) {
                cur = this.osJobExec;
              } else if (type.equals("TABLEAU")) {
                cur = this.tableauJobExec;
              } else if (type.equals("XMLSESSIONIZER")) {
                throw new RuntimeException(
                    "The XMLSessionizer job type is no longer supported, please migrate to KETL job with Sessionizer step");
              } else if (type.equals("EMPTYJOB")) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Skipping empty job "
                    + jobID);
              }

              if (cur == null)
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                    "Unknown job type, skipping job " + jobID);
              else
                ETLJobExecutor._execute(new String[] {
                    "ENABLETRIGGERS=" + (this.enableTriggers ? "TRUE" : "FALSE"),
                    "LOADID=" + this.iLoadID, "FILE=" + file, "JOBID=" + jobID}, cur, false, 0);

            }

          } else
            return this.syntaxError(COMMANDS.RUN);
          break;

      }

    } finally {
      this.iLoadID = this.prevLoadId;

    }

    return sb.toString();
  }

  /** The load ID. */
  int iLoadID = (int) (System.currentTimeMillis() / 1000) / 2;

  /**
   * Server.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String server(String[] pCommands) throws Exception {
    StringBuilder sd = new StringBuilder();

    if (this.connected()) {
      KETLCluster kc = this.md.getClusterDetails();
      String[] res = kc.getServerList();
      for (String element : res) {
        sd.append(element);
        sd.append("\n");
      }
    }

    return sd.toString();
  }

  /**
   * Shutdown.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String shutdown(String[] pCommands) throws Exception {
    if (this.connected()) {

      if (pCommands.length < 2)
        return "ERROR: Server id is missing";

      boolean kill = false;

      if (pCommands.length == 3) {
        if (pCommands[2].equalsIgnoreCase("IMMEDIATE")) {
          kill = true;
        }
      }

      int i;
      try {
        i = Integer.parseInt(pCommands[1]);
      } catch (NumberFormatException e) {
        return "Server ID not a valid number = " + pCommands[1];
      }

      if (this.md.shutdownServer(i, kill)) {
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
            "Request to shutdown server made, waiting for confirmation");

        KETLCluster kc = this.md.getClusterDetails();
        int cnt = 0;

        while (kc.isServerAlive(i) && (cnt < 30)) {
          System.out.print(".");
          Thread.sleep(2000);
          cnt++;
          kc = this.md.getClusterDetails();
          if (cnt % 10 == 0 && cnt > 1 && kc.isServerAlive(i)) {
            this.md.shutdownServer(i, kill);
          }
        }

        System.out.println("");

        if (kc.isServerAlive(i)) {
          ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
              "Server has not yet shutdown, check status");
        } else {
          ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Server shutdown");
        }

        return "";
      }

      return "ERROR: Server ID might not be valid";

    }

    return "Not connected";
  }

  /**
   * Startup.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String startup(String[] pCommands) throws Exception {
    if (this.connected()) {
      KETLCluster kc = this.md.getClusterDetails();

      if (kc.isServerAlive(this.servername)) {
        ResourcePool
            .LogMessage(
                this,
                ResourcePool.INFO_MESSAGE,
                "WARNING: Server is registered and current status is alive\nIf a server has crashed then forcing startup might be ok");

        System.out.print("Force startup Y/N: ");

        String res = this.stdin.readLine();

        if (res.startsWith("N")) {
          return "Startup aborted";
        }
      }

      // server can be started up remotely
      // unix nohup java -cp KETL.jar;ojdbc14.jar
      String remoteStart =
          XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "REMOTESTART", null, null, null);

      if (remoteStart == null) {
        return "No remote start tag defined in server xml";
      }

      String[] params = EngineConstants.getParametersFromText(remoteStart);

      if ((params != null) && (params.length > 0)) {
        String pwd = null;

        for (String element : params) {
          String res = null;
          String paramName = element;

          if (paramName.equalsIgnoreCase("NETWORKNAME")) {
            res =
                XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "NETWORKNAME", null, null,
                    null);

            if (res == null) {
              return "No network name tag defined in server xml";
            }

            remoteStart = EngineConstants.replaceParameter(remoteStart, paramName, res);
          } else if (element.equalsIgnoreCase("USERNAME")) {
            System.out.print("Enter username to start server: ");
            res = this.stdin.readLine();

            remoteStart = EngineConstants.replaceParameter(remoteStart, paramName, res);

            System.out.print("Enter password for username: ");
            pwd = this.stdin.readLine();

            remoteStart = EngineConstants.replaceParameter(remoteStart, "PASSWORD", pwd);
          }
        }
      }

      KETLBootStrap.startProcess(null, remoteStart + " " + this.username + " " + this.password
          + " " + this.url + " " + this.driver, false);

      return "Started";
    }

    return null;
  }

  /**
   * Status.
   * 
   * @param pCommands the commands
   * @return the string
   * @throws Exception the exception
   */
  private String status(String[] pCommands) throws Exception {
    // Show how many servers in cluster, how many running
    if (this.connected()) {
      if ((pCommands.length > 1) && pCommands[1].equalsIgnoreCase("JOBS")) {
        // list all running jobs, start time, server
        StringBuffer sb = new StringBuffer();

        boolean all = pCommands.length == 3 && pCommands[2].equalsIgnoreCase("ALL");
        boolean compact = !all;
        if (all) {
          sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.WAITING_FOR_CHILDREN),
              "Waiting for Children", compact));
          sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.REJECTED), "Rejected",
              compact));
          sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.CANCELLED),
              "Cancelled", compact));
          sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.QUEUED_FOR_EXECUTION),
              "Queued For Execution", compact));
          sb.append(this.jobStatusTable(
              this.md.getJobsByStatus(ETLJobStatus.PENDING_CLOSURE_CANCELLED), "Just Cancelled",
              compact));
          sb.append(this.jobStatusTable(
              this.md.getJobsByStatus(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL), "Just Finished",
              compact));
        }

        sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.PAUSED), "Paused",
            compact));
        sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.READY_TO_RUN),
            "Ready To Run", compact));
        sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.WAITING_TO_BE_RETRIED),
            "Waiting To Retry", compact));
        sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.FAILED), "Failed",
            compact));
        sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.PENDING_CLOSURE_FAILED),
            "Just Failed", compact));
        sb.append(this.jobStatusTable(this.md.getJobsByStatus(ETLJobStatus.EXECUTING), "Executing",
            compact));

        return sb.toString();
      }

      KETLCluster kc = this.md.getClusterDetails();

      return kc.toString();
    }

    return null;
  }

  /*
   * private String executeDirect(String[] pCommands) throws Exception { StringBuffer sb = new
   * StringBuffer(); String className; if(pCommands.length != 4) { return
   * "Syntax: job <ID> EXECUTEDIRECT <XMLFILENAME>"; } String xmlFile = pCommands[4]; if(md != null)
   * { // get job executor class int typeID; className = md.getJobExecutorClassForTypeID(-1); } else
   * { ResourcePool.LogMessage(this,"Not connected to metadata, please specify job execution
   * class:"); className = stdin.readLine(); // request class } return "Done"; }
   */

  /*
   * private String executeDirect(String[] pCommands) throws Exception { StringBuffer sb = new
   * StringBuffer(); String className; if(pCommands.length != 4) { return
   * "Syntax: job <ID> EXECUTEDIRECT <XMLFILENAME>"; } String xmlFile = pCommands[4]; if(md != null)
   * { // get job executor class int typeID; className = md.getJobExecutorClassForTypeID(-1); } else
   * { ResourcePool.LogMessage(this,"Not connected to metadata, please specify job execution
   * class:"); className = stdin.readLine(); // request class } return "Done"; }
   */

  /**
   * Syntax error.
   * 
   * @param pCommand the command
   * @return the string
   */
  private String syntaxError(COMMANDS pCommand) {
    return "Syntax Error: Wrong syntax for " + pCommand + "\n" + pCommand.help();
  }

  /**
   * Unknown command.
   * 
   * @param pCommand the command
   * @return the string
   */
  private String unknownCommand(String pCommand) {
    return ("Error unknown command: " + pCommand);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Console";
  }
}
