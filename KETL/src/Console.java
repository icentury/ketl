/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLCluster;
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.sessionizer.XMLSessionizeJobExecutor;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;
import com.kni.util.FileHelpers;

/*
 * Created on Apr 4, 2005 To change the template for this generated file go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class Console {

    static final int CANCEL_JOB = 12;
    static final int CONNECT = 6;
    static final int DEFINITION = 1;
    static final int DELETE = 8;
    static final int DEPENDENCIES = 2;
    static final int EXECUTE = 3;
    static final int EXECUTEDIRECT = 11;
    static final int EXPORT = 7;
    static final int HELP = 7;
    static final int IGNOREDEPENDENCIES = 4;
    static final int IMPORT = 6;
    static final int JOB = 3;
    static final String[] JOBDETAIL_TYPES = { "XMLDEFINITION", "DEFINITION", "DEPENDENCIES", "EXECUTE",
            "IGNOREDEPENDENCIES", "MULTI", "IMPORT", "EXPORT", "DELETE", "RESTART", "SKIP", "EXECUTEDIRECT", "CANCEL" };
    static final int LIST = 0;
    static final int MULTI = 5;
    static final int PARAMETERLIST = 8;

    static final int PAUSE = 9;

    static final int PROJECT = 11;

    static final int QUIT = 5;
    static final int RESET = 1;
    static final int LOOKUPS = 2;
    static final int RESTART = 4;
    static final int LOADID = 3;

    static final int RESTART_JOB = 9;

    static final int RESUME = 10;
    static final int RUN = 13;
    static final int LAST = 14;
    static final String[] RUN_TYPES = { "LIST", "RESET", "LOOKUPS", "LOADID" };

    static final int SERVER = 12;

    static final int SHUTDOWN = 0;
    static final int SKIP = 10;
    static final int STARTUP = 1;
    static final int STATUS = 2;

    /**
     * The KETL Console allows for remote management of the KETL server Supported commands STATUS SHUTDOWN
     * {IMMEDIATE|NORMAL} STARTUP JOB <NAME> {DEFINITION|XMLDEFINITION|DEPENDENCIES} RESTART {IMMEDIAITE|NORMAL} QUIT
     */
    static final String[] strCommands = { "SHUTDOWN", "STARTUP", "STATUS", "JOB", "RESTART", "QUIT", "CONNECT", "HELP",
            "PARAMETERLIST", "PAUSE", "RESUME", "PROJECT", "SERVER", "RUN", "/" };

    static final String[] strSyntax = {
            "SHUTDOWN <SERVERID> {IMMEDIATE|NORMAL}",
            "STARTUP",
            "STATUS {CLUSTER|JOBS}",
            "JOB <NAME> {DEFINITION|DELETE|KILL|XMLDEFINITION|RESTART|SKIP|EXPORT <FILENAME>|IMPORT <FILENAME>|DEPENDENCIES|EXECUTE <PROJECTID> {MULTI} {IGNOREDEPENDENCIES}}",
            "RESTART {IMMEDIATE|NORMAL}", "QUIT", "CONNECT <SERVER|LOCALHOST> <USERNAME>", "HELP",
            "PARAMETERLIST <NAME> <EXPORT|IMPORT|DEFINITION> {FILENAME}", "PAUSE <SERVERID>", "RESUME <SERVERID>", "PROJECT LIST",
            "SERVER LIST", "RUN {LIST|RESET|<FILENAME>|LOADID <VALUE>}", "/ {<REPEAT>} {<SECONDS BETWEEN REPEAT>}" };

    static final int XMLDEFINITION = 0;

    private static String displayVersionInfo() {
        EngineConstants.getVersion();
        return "KETL Console\n";
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // create console object
        Console console = new Console();

        Thread.currentThread().setName("KETL Console");

        // accept commands
        console.run(args);

        ResourcePool.releaseAllLookups();
    }

    String driver;
    InputStreamReader inputStreamReader = new InputStreamReader(System.in);
    ETLJobExecutor kJobExec = new KETLJobExecutor();
    Metadata md = null;
    String mdprefix;
    ArrayList mPreviousCommands = new ArrayList();
    Node nCurrentServer;

    ETLJobExecutor osJobExec = new OSJobExecutor();

    String password;

    String servername = null;

    ETLJobExecutor sessionJobExec = new XMLSessionizeJobExecutor();

    ETLJobExecutor sqlJobExec = new SQLJobExecutor();

    BufferedReader stdin = new BufferedReader(inputStreamReader);

    String url;

    String username;

    Document xmlConfig = null;

    private boolean connected() {
        if (md != null) {
            return true;
        }

        System.out.println("ERROR: currently disconnected!");

        return false;
    }

    private String connectToServer(String[] pCommands) throws IOException {
        if (md != null) {
            md.closeMetadata();
            md = null;
        }

        // if number of commands greater than 3 then syntax error
        if (pCommands.length > 3) {
            return syntaxError(CONNECT);
        }

        // get server to connect to.
        if (pCommands.length > 1) // use named server
        {
            servername = pCommands[1];
        }
        else if (pCommands.length == 1) // use default server
        {
            servername = XMLHelper.getAttributeAsString(xmlConfig.getFirstChild().getAttributes(), "DEFAULTSERVER", "");
        }

        if (servername.equalsIgnoreCase("LOCALHOST")) {
            try {
                InetAddress thisIp = InetAddress.getLocalHost();
                servername = thisIp.getHostName();

                // try for localhost
                nCurrentServer = XMLHelper.findElementByName(xmlConfig, "SERVER", "NAME", "localhost");

                if (nCurrentServer == null) {
                    nCurrentServer = XMLHelper.findElementByName(xmlConfig, "SERVER", "NAME", "LOCALHOST");
                }

                // try for explicit name
                if (nCurrentServer == null) {
                    nCurrentServer = XMLHelper.findElementByName(xmlConfig, "SERVER", "NAME", servername);
                }
            } catch (UnknownHostException e) {
                return "Connection failure: Could not get system hostname please supply servername";
            }
        }
        else {
            nCurrentServer = XMLHelper.findElementByName(xmlConfig, "SERVER", "NAME", servername);
        }

        if (nCurrentServer == null) {
            return "ERROR: Server " + servername + " not found!";
        }

        if (pCommands.length == 3) {
            // use specified login and prompt for password
            System.out.print("Enter server password:");
            password = stdin.readLine();
            username = pCommands[2];
        }
        else {
            username = XMLHelper.getChildNodeValueAsString(nCurrentServer, "USERNAME", null, null, null);
            password = XMLHelper.getChildNodeValueAsString(nCurrentServer, "PASSWORD", null, null, null);
        }

        url = XMLHelper.getChildNodeValueAsString(nCurrentServer, "URL", null, null, null);
        driver = XMLHelper.getChildNodeValueAsString(nCurrentServer, "DRIVER", null, null, null);
        mdprefix = XMLHelper.getChildNodeValueAsString(nCurrentServer, "MDPREFIX", null, null, null);
        String passphrase = XMLHelper.getChildNodeValueAsString(nCurrentServer, "PASSPHRASE", null, null, null);

        // metadata object isn't set and login information found then connect to metadata
        if ((md == null) && (username != null)) {

            try {
                Metadata mds = new Metadata(true, passphrase);
                mds.setRepository(username, password, url, driver, mdprefix);
                servername = XMLHelper.getAttributeAsString(nCurrentServer.getAttributes(), "NAME", servername);
                ResourcePool.setMetadata(mds);
                md = ResourcePool.getMetadata();

            } catch (Exception e1) {
                return ("ERROR: Connecting to metadata - " + e1.getMessage());
            }
        }

        return "Connected to " + servername;
    }

    String help(String[] pCommands) {
        StringBuffer sb = new StringBuffer();

        sb
                .append("KETL Console allows remote administration of a KETL\ncluster, the following commands are available.\n\n");

        for (int i = 0; i < strSyntax.length; i++) {
            sb.append("\t" + strSyntax[i] + "\n");
        }

        return sb.toString();
    }

    private String importJobs(String[] pCommands) throws Exception {
        String[] files = FileHelpers.getFilenames(pCommands[3]);

        if (files == null || files.length == 0)
            return "ERROR: File not found - " + pCommands[3];

        for (int f = 0; f < files.length; f++) {

            Document jobNodes = null;
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Reading file " + files[f]);

            // turn file into readable nodes
            DocumentBuilder builder = null;

            // Build a DOM out of the XML string...
            try {
                DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
                builder = dmf.newDocumentBuilder();
                jobNodes = builder.parse(new InputSource(new FileReader(files[f])));

                NodeList nl = jobNodes.getElementsByTagName("JOB");

                for (int i = 0; i < nl.getLength(); i++) {
                    Node job = nl.item(i);
                    md.importJob(job);
                }
            } catch (org.xml.sax.SAXException e) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Parsing XML document, "
                        + e.toString());

                System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
            } catch (Exception e) {
                ResourcePool.LogException(e, this);

                System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
            }
        }
        return "Done";
    }

    private String importParameters(String[] pCommands) throws Exception {
        String[] files = FileHelpers.getFilenames(pCommands[3]);

        for (int f = 0; f < files.length; f++) {

            StringBuffer sb = new StringBuffer();
            Document jobNodes = null;
            FileReader inputFileReader = null;

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Reading file " + files[f]);
            inputFileReader = new FileReader(files[f]);
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

                    md.importParameterList(parameterList);
                }
            } catch (org.xml.sax.SAXException e) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Parsing XML document, "
                        + e.toString());

                System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
            } catch (Exception e) {
                ResourcePool.LogException(e, this);

                System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
            }
        }
        return "Done";
    }

    private String jobDetails(String[] pCommands) throws Exception {
        StringBuffer sb = new StringBuffer();
        StringBuffer sbJobList = new StringBuffer("Export Jobs:\n");
        boolean deleteAll = false;

        if (connected()) {
            // if its an export then load from file
            if (resolveCommand(pCommands[2], JOBDETAIL_TYPES) == IMPORT) {
                return importJobs(pCommands);
            }
            /*
             * else if ((pCommands.length > 2) && (resolveCommand(pCommands[2], JOBDETAIL_TYPES) == EXECUTEDIRECT)) {
             * return executeDirect(pCommands); }
             */
            else if (pCommands.length > 1) {
                try {
                    ETLJob[] jobs = md.getJobDetails(pCommands[1].replaceAll("\\*", "%"));

                    int jobDetailType = DEFINITION;

                    if (pCommands.length > 2) {
                        jobDetailType = resolveCommand(pCommands[2], JOBDETAIL_TYPES);
                    }

                    if ((jobDetailType == XMLDEFINITION) || (jobDetailType == EXPORT)) {
                        sb.append("<?xml version=\"1.0\"?>\n<ETL VERSION=\"" + md.getMetadataVersion()
                                + "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">");
                    }

                    if ((jobDetailType == DELETE)) {
                        System.out
                                .println("WARNING: Deleting jobs can result in lost dependencies\nIt is recommended that you export job definition first!");
                    }

                    if (jobs == null || jobs.length == 0) {
                        if (md.getJob(pCommands[1]) == null) {
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
                                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "ERROR: No export file given");

                                return this.syntaxError(JOB);
                            }

                            sb.append("\n\n<!--- Job: " + eJob.getJobID() + "-->\n");
                            sbJobList.append("  " + eJob.getJobID() + "\n");
                            sb.append(eJob.getXMLJobDefinition());

                            break;

                        case DEFINITION:
                            sb.append(eJob.getJobDefinition());

                            break;

                        case RESTART_JOB:

                            String choice = "Y";
                            md.getJobStatus(eJob);

                            if (eJob.getStatus().getStatusCode() != ETLJobStatus.SCHEDULED) {
                                if (eJob.getStatus().getStatusCode() == ETLJobStatus.EXECUTING) {
                                    System.out
                                            .print("Job "
                                                    + eJob.getJobID()
                                                    + " is marked as executing, only do this if you are recovering from a server that has been killed! Y(Yes), N(No): ");
                                    choice = stdin.readLine();
                                }
                                else if ((eJob.getStatus().getStatusCode() != ETLJobStatus.FAILED)
                                        && (eJob.getStatus().getStatusCode() != ETLJobStatus.PENDING_CLOSURE_FAILED)) {
                                    System.out.print("Job " + eJob.getJobID()
                                            + " has not failed do you really want to restart it Y(Yes), N(No): ");
                                    choice = stdin.readLine();
                                }

                                if (choice.equalsIgnoreCase("Y")) {
                                    eJob.getStatus().setStatusCode(ETLJobStatus.READY_TO_RUN);
                                    md.setJobStatus(eJob);
                                    sb.append("Job: " + eJob.getJobID() + " restarted");
                                }
                            }
                            else {
                                sb.append("Job: " + eJob.getJobID()
                                        + " can not be restarted as it is running or not in the current run list");
                            }

                            break;
                        case CANCEL_JOB:

                            choice = "Y";
                            md.getJobStatus(eJob);

                            if ((eJob.getStatus().getStatusCode() != ETLJobStatus.EXECUTING)
                                    && (eJob.getStatus().getStatusCode() != ETLJobStatus.SCHEDULED)) {
                                if ((eJob.getStatus().getStatusCode() != ETLJobStatus.PENDING_CLOSURE_FAILED)
                                        && (eJob.getStatus().getStatusCode() != ETLJobStatus.FAILED)) {
                                    System.out.print("Job " + eJob.getJobID()
                                            + " has not failed do you really want to cancel it Y(Yes), N(No): ");
                                    choice = stdin.readLine();
                                }

                                if (choice.equalsIgnoreCase("Y")) {
                                    eJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_CANCELLED);
                                    md.setJobStatus(eJob);
                                    sb.append("Job: " + eJob.getJobID() + " cancelled");
                                }
                            }
                            else {
                                sb
                                        .append("Job: "
                                                + eJob.getJobID()
                                                + " can not be cancelled as it is already running or not in the current run list");
                            }

                            break;

                        case SKIP:
                            md.getJobStatus(eJob);

                            if ((eJob.getStatus().getStatusCode() != ETLJobStatus.EXECUTING)
                                    && (eJob.getStatus().getStatusCode() != ETLJobStatus.SCHEDULED)) {
                                eJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL);
                                md.setJobStatus(eJob);
                                sb.append("Job: " + eJob.getJobID() + " skipped, it will appear as successful");
                            }
                            else {
                                sb.append("Job: " + eJob.getJobID()
                                        + " can not be skipped as it is running or not in the current run list");
                            }

                            break;

                        case DEPENDENCIES:
                            sb.append("Job: " + eJob.getJobID() + "\n");
                            sb.append(eJob.getDepedencies() + "\n");

                            break;

                        case DELETE:

                            String opt = "";

                            while (opt != null) {
                                if (deleteAll == false) {
                                    System.out.print("Delete job " + eJob.getJobID()
                                            + " Y(Yes), N(No), S(Skip all other jobs), A(Yes for All): ");
                                    opt = stdin.readLine();
                                }
                                else {
                                    opt = "Y";
                                }

                                switch (opt.toUpperCase().charAt(0)) {
                                case 'Y':
                                    md.deleteJob(eJob.getJobID());
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
                                    md.deleteJob(eJob.getJobID());
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
                            int opt1 = -1;
                            int opt2 = -1;

                            if (pCommands.length < 4) {
                                return this.syntaxError(JOB);
                            }

                            if (pCommands.length > 4) {
                                opt1 = resolveCommand(pCommands[4], JOBDETAIL_TYPES);
                            }

                            if (pCommands.length > 5) {
                                opt2 = resolveCommand(pCommands[5], JOBDETAIL_TYPES);
                            }

                            if ((opt1 == IGNOREDEPENDENCIES) || (opt2 == IGNOREDEPENDENCIES)) {
                                ignoreDeps = true;
                            }

                            if ((opt1 == MULTI) || (opt2 == MULTI)) {
                                allowMult = true;
                            }

                            int pID;

                            try {
                                pID = Integer.parseInt(pCommands[3]);

                                if (md.executeJob(pID, pCommands[1], ignoreDeps, allowMult)) {
                                    sb.append("Job submitted to server for direct execution.\n");
                                }
                                else {
                                    sb.append("Warning Job not submitted to server for execution.\n");
                                }
                            } catch (Exception e) {
                                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Invalid project id\n");
                                sb.append("Job not submitted");
                            }

                            break;
                        }
                    }

                    if ((jobDetailType == XMLDEFINITION) || (jobDetailType == EXPORT)) {
                        sb.append("\n</ETL>");
                    }

                    if (jobDetailType == EXPORT) {
                        BufferedWriter out = new BufferedWriter(new java.io.FileWriter(pCommands[3]));
                        out.write(sb.toString());
                        out.close();
                        sb = sbJobList.append("Done.");
                    }
                } catch (Exception e) {
                    ResourcePool.LogException(e, this);
                }
            }
            else {
                return syntaxError(JOB);
            }
        }

        return sb.toString();
    }

    String jobStatusTable(Object[][] pJobs, String pTableHeader) {
        if (pJobs == null) {
            return "";
        }

        StringBuffer sb = new StringBuffer();
        sb.append(pTableHeader);
        sb.append("\n");

        for (int i = 0; i < pTableHeader.length(); i++) {
            sb.append("-");
        }

        sb.append("\n");

        for (int i = 0; i < pJobs.length; i++) {
            for (int x = 0; x < pJobs[i].length; x++) {
                if (pJobs[i][x] != null) {
                    sb.append(pJobs[i][x]);
                }

                if (x < (pJobs[i].length - 1)) {
                    sb.append(" - ");
                }
            }

            sb.append("\n");
        }

        sb.append("\n");

        return sb.toString();
    }

    private String parameterList(String[] pCommands) throws Exception {

        if (connected()) {
            // if its an export then load from file
            if ((pCommands.length > 2) && (resolveCommand(pCommands[2], JOBDETAIL_TYPES) == IMPORT)) {
                return importParameters(pCommands);
            }
            else if ((pCommands.length > 2) && (resolveCommand(pCommands[2], JOBDETAIL_TYPES) == DEFINITION)) {

                return getXMLParameterlistDefinition(pCommands[1]);

            }
            else if ((pCommands.length > 2) && (resolveCommand(pCommands[2], JOBDETAIL_TYPES) == EXPORT)) {
                if (pCommands.length < 4) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "ERROR: No export file given");

                    return this.syntaxError(PARAMETERLIST);
                }

                BufferedWriter out = new BufferedWriter(new java.io.FileWriter(pCommands[3]));
                out.write(getXMLParameterlistDefinition(pCommands[1]));
                out.close();
            }
        }

        return "Done";
    }

    private String getXMLParameterlistDefinition(String pListMatchString) {
        StringBuffer sb = new StringBuffer();

        String[] pLists = md.getValidParameterListName(pListMatchString.replaceAll("\\*", "%"));

        sb.append("<?xml version=\"1.0\"?>\n<ETL VERSION=\"" + md.getMetadataVersion()
                + "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");

        if (pLists != null) {
            for (int i = 0; i < pLists.length; i++) {
                sb.append("  <PARAMETER_LIST NAME=\"" + pLists[i] + "\">\n");

                Object[][] pList = md.getParameterList(pLists[i]);

                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Fetching: " + pLists[i]);

                if (pList != null) {
                    for (int x = 0; x < pList.length; x++) {
                        String sub = "";
                        String val = "/>\n";

                        if (pList[x][Metadata.SUB_PARAMETER_LIST_NAME] != null) {
                            sub = " PARAMETER_LIST=\"" + pList[x][Metadata.SUB_PARAMETER_LIST_NAME] + "\" ";
                        }

                        if (pList[x][Metadata.PARAMETER_VALUE] != null) {
                            val = " >" + pList[x][Metadata.PARAMETER_VALUE] + "</PARAMETER>\n";
                        }

                        sb.append("     <PARAMETER NAME=\"" + pList[x][Metadata.PARAMETER_NAME] + "\"" + sub + val);
                    }
                }

                sb.append("  </PARAMETER_LIST>\n");
            }
        }

        sb.append("\n</ETL>");

        return sb.toString();
    }

    private String pause(String[] pCommands) throws Exception {
        if (connected()) {

            if (pCommands.length < 2)
                return "ERROR: Server id is missing";

            int i;
            try {
                i = Integer.parseInt(pCommands[1]);
            } catch (NumberFormatException e) {
                return "Server ID not a valid number = " + pCommands[1];
            }
            if (md.pauseServer(i, true)) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Request to pause server made");
                return this.server(new String[] { "SERVER", "LIST" });
            }
        }

        return "";
    }

    private String project(String[] pCommands) throws Exception {
        StringBuffer sb = new StringBuffer("Project(s)\nID\tDescription\n--\t-----------\n");
        if (connected()) {
            Object[] result = md.getProjects();
            for (int i = 0; i < result.length; i++) {
                Object[] tmp = (Object[]) result[i];
                sb.append(tmp[0]);
                sb.append('\t');
                sb.append(tmp[1]);
                sb.append('\n');
            }
        }

        return sb.toString();
    }

    private int resolveCommand(String pCommand, String[] pCommandList) {
        for (int i = 0; i < pCommandList.length; i++) {
            if (pCommandList[i].equalsIgnoreCase(pCommand)) {
                return i;
            }
        }

        return -1;
    }

    private String restart(String[] pCommands) throws Exception {
        if (connected()) {
            this.shutdown(pCommands);
            this.startup(pCommands);
        }

        return "";
    }

    private String resume(String[] pCommands) throws Exception {
        if (connected()) {

            if (pCommands.length < 2)
                return "ERROR: Server id is missing";

            int i;
            try {
                i = Integer.parseInt(pCommands[1]);
            } catch (NumberFormatException e) {
                return "Server ID not a valid number = " + pCommands[1];
            }
            if (md.pauseServer(i, false)) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Request to resume server made");
                return this.server(new String[] { "SERVER", "LIST" });
            }
        }

        return "";
    }

    void run(String[] args) {
        String configFile = Metadata.CONFIG_FILE;

        if ((args != null) && (args.length > 0)) {
            configFile = args[0];
        }

        // EngineConstants.getSystemXML();
        xmlConfig = Metadata.LoadConfigFile(null, configFile);

        System.out.println(displayVersionInfo());

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
                    }
                    else
                        line = stdin.readLine();

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
                    }
                    else
                        last = line;
                }
                else {
                    if (command < quickCommand.length) {
                        line = quickCommand[command++];
                    }
                    else {
                        line = strCommands[QUIT];
                    }
                }

                if (line != null) {
                    String[] commands = ArgumentParserUtil.splitQuoteAware(line);

                    if ((commands != null) && (commands.length > 0)) {
                        String res = "";

                        switch (resolveCommand(commands[0], strCommands)) {
                        case SHUTDOWN:
                            res = shutdown(commands);

                            break;

                        case RESTART:
                            res = restart(commands);

                            break;

                        case STARTUP:
                            res = startup(commands);

                            break;
                        case PROJECT:
                            res = project(commands);

                            break;

                        case JOB:
                            res = jobDetails(commands);

                            break;

                        case PARAMETERLIST:
                            res = parameterList(commands);

                            break;
                        case RUN:
                            res = runJob(commands);

                            break;

                        case CONNECT:
                            res = connectToServer(commands);

                            break;

                        case HELP:
                            res = help(commands);

                            break;
                        case SERVER:
                            res = server(commands);

                            break;

                        case QUIT:
                            hasRequestedQuit = true;

                            return;

                        case STATUS:
                            res = status(commands);

                            break;

                        case PAUSE:
                            res = pause(commands);

                            break;

                        case RESUME:
                            res = resume(commands);

                            break;

                        default:
                            res = unknownCommand(commands[0]);

                            break;
                        }

                        System.out.println(res);
                    }
                }
            } catch (Exception ex) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error:"
                        + (ex.getCause() != null && ex instanceof RuntimeException ? ex.getCause().toString() : ex
                                .toString()));
            }
        }

    }

    private String runJob(String[] pCommands) {
        StringBuffer sb = new StringBuffer();

        {

            int jobDetailType = DEFINITION;

            if (pCommands.length > 1) {
                jobDetailType = resolveCommand(pCommands[1], RUN_TYPES);
            }
            else {
                return syntaxError(RUN);
            }

            // resolve output type
            switch (jobDetailType) {
            case LOADID:
                try {
                    if (pCommands.length == 3)
                        iLoadID = Integer.parseInt(pCommands[2]);
                    else
                        return "Invalid syntax";
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return "LoadID = " + this.iLoadID;
            case RESET:
                if (pCommands.length == 3) {
                    if (ResourcePool.releaseLookup(pCommands[2]))
                        return "Released lookup " + pCommands[2];
                    else
                        return "Could not find lookup" + pCommands[2];
                }
                else {
                    ResourcePool.releaseLoadLookups(this.iLoadID);
                    return "All cached resources release";
                }

            case LIST:
                for (int i = 0; i < this.mPreviousCommands.size(); i++)
                    sb.append("" + (i + 1) + ".\t" + this.mPreviousCommands.get(i) + "\n");
                return sb.toString();
            case LOOKUPS:
                System.gc();
                ArrayList list = ResourcePool.getLookups(this.iLoadID);
                for (int i = 0; i < list.size(); i++)
                    sb.append("" + (i + 1) + ".\t" + list.get(i) + "\n");
                return sb.toString();

            default:
                if (pCommands.length == 2) {

                    String file = pCommands[1];
                    try {
                        file = (String) this.mPreviousCommands.get(Integer.parseInt(pCommands[1]) - 1);
                    } catch (Exception e) {
                    }

                    this.mPreviousCommands.remove(file);
                    this.mPreviousCommands.add(0, file);

                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Executing file " + file);
                    Document doc;
                    try {
                        doc = XMLHelper.readXMLFromFile(file);
                    } catch (Exception e) {
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
                        if (type.equals("KETL")) {
                            cur = this.kJobExec;
                        }
                        else if (type.equals("SQL")) {
                            cur = this.sqlJobExec;
                        }
                        else if (type.equals("OSJOB")) {
                            cur = this.osJobExec;
                        }
                        else if (type.equals("XMLSESSIONIZER")) {
                            cur = this.sessionJobExec;
                        }
                        else if (type.equals("EMPTYJOB")) {
                            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Skipping empty job " + jobID);
                        }

                        if (cur == null)
                            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Unknown job type, skipping job "
                                    + jobID);
                        else
                            ETLJobExecutor._execute(new String[] { "LOADID=" + iLoadID, "FILE=" + file,
                                    "JOBID=" + jobID }, cur, false, 0);

                    }

                }
                else
                    return syntaxError(RUN);
                break;

            }

        }

        return sb.toString();
    }

    int iLoadID = (int) System.currentTimeMillis() / 2;

    private String server(String[] pCommands) throws Exception {
        StringBuilder sd = new StringBuilder();

        if (connected()) {
            KETLCluster kc = md.getClusterDetails();
            String[] res = kc.getServerList();
            for (int i = 0; i < res.length; i++) {
                sd.append(res[i]);
                sd.append("\n");
            }
        }

        return sd.toString();
    }

    private String shutdown(String[] pCommands) throws Exception {
        if (connected()) {

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

            if (md.shutdownServer(i, kill)) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                        "Request to shutdown server made, waiting for confirmation");

                KETLCluster kc = md.getClusterDetails();
                int cnt = 0;

                while (kc.isServerAlive(i) && (cnt < 30)) {
                    System.out.print(".");
                    Thread.sleep(2000);
                    cnt++;
                    kc = md.getClusterDetails();
                    if (cnt % 10 == 0 && cnt > 1 && kc.isServerAlive(i)) {
                        md.shutdownServer(i, kill);
                    }
                }

                System.out.println("");

                if (kc.isServerAlive(i)) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                            "Server has not yet shutdown, check status");
                }
                else {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Server shutdown");
                }

                return "";
            }

            return "ERROR: Server ID might not be valid";

        }

        return "Not connected";
    }

    private String startup(String[] pCommands) throws Exception {
        if (connected()) {
            KETLCluster kc = md.getClusterDetails();

            if (kc.isServerAlive(this.servername)) {
                ResourcePool
                        .LogMessage(this, ResourcePool.INFO_MESSAGE,
                                "WARNING: Server is registered and current status is alive\nIf a server has crashed then forcing startup might be ok");

                System.out.print("Force startup Y/N: ");

                String res = stdin.readLine();

                if (res.startsWith("N")) {
                    return "Startup aborted";
                }
            }

            // server can be started up remotely
            // unix nohup java -cp KETL.jar;ojdbc14.jar
            String remoteStart = XMLHelper.getChildNodeValueAsString(nCurrentServer, "REMOTESTART", null, null, null);

            if (remoteStart == null) {
                return "No remote start tag defined in server xml";
            }

            String[] params = EngineConstants.getParametersFromText(remoteStart);

            if ((params != null) && (params.length > 0)) {
                String pwd = null;

                // resolve parameter values
                for (int i = 0; i < params.length; i++) {
                    String res = null;
                    String paramName = params[i];

                    if (paramName.equalsIgnoreCase("NETWORKNAME")) {
                        res = XMLHelper.getChildNodeValueAsString(this.nCurrentServer, "NETWORKNAME", null, null, null);

                        if (res == null) {
                            return "No network name tag defined in server xml";
                        }

                        remoteStart = EngineConstants.replaceParameter(remoteStart, paramName, res);
                    }
                    else if (params[i].equalsIgnoreCase("USERNAME")) {
                        System.out.print("Enter username to start server: ");
                        res = stdin.readLine();

                        remoteStart = EngineConstants.replaceParameter(remoteStart, paramName, res);

                        System.out.print("Enter password for username: ");
                        pwd = stdin.readLine();

                        remoteStart = EngineConstants.replaceParameter(remoteStart, "PASSWORD", pwd);
                    }
                }
            }

            KETLBootStrap.startProcess(null, remoteStart + " " + username + " " + password + " " + url + " " + driver,
                    false);

            return "Started";
        }

        return null;
    }

    private String status(String[] pCommands) throws Exception {
        // Show how many servers in cluster, how many running
        if (connected()) {
            if ((pCommands.length > 1) && pCommands[1].equalsIgnoreCase("JOBS")) {
                // list all running jobs, start time, server
                StringBuffer sb = new StringBuffer();

                if (pCommands.length == 3 && pCommands[2].equalsIgnoreCase("ALL")) {
                    sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.REJECTED), "Rejected"));
                    sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.CANCELLED), "Cancelled"));
                    sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.QUEUED_FOR_EXECUTION),
                            "Queued For Execution"));
                    sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.PENDING_CLOSURE_CANCELLED),
                            "Just Cancelled"));
                    sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL),
                            "Just Finished"));
                }

                sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.EXECUTING), "Executing"));
                sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.FAILED), "Failed"));
                sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.PENDING_CLOSURE_FAILED), "Just Failed"));
                sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.PAUSED), "Paused"));
                sb.append(jobStatusTable(md.getJobsByStatus(ETLJobStatus.READY_TO_RUN), "Ready To Run"));

                return sb.toString();
            }

            KETLCluster kc = md.getClusterDetails();

            return kc.toString();
        }

        return null;
    }

    /*
     * private String executeDirect(String[] pCommands) throws Exception { StringBuffer sb = new StringBuffer(); String
     * className; if(pCommands.length != 4) { return "Syntax: job <ID> EXECUTEDIRECT <XMLFILENAME>"; } String xmlFile =
     * pCommands[4]; if(md != null) { // get job executor class int typeID; className =
     * md.getJobExecutorClassForTypeID(-1); } else { ResourcePool.LogMessage(this,"Not connected to metadata, please
     * specify job execution class:"); className = stdin.readLine(); // request class } return "Done"; }
     */

    /*
     * private String executeDirect(String[] pCommands) throws Exception { StringBuffer sb = new StringBuffer(); String
     * className; if(pCommands.length != 4) { return "Syntax: job <ID> EXECUTEDIRECT <XMLFILENAME>"; } String xmlFile =
     * pCommands[4]; if(md != null) { // get job executor class int typeID; className =
     * md.getJobExecutorClassForTypeID(-1); } else { ResourcePool.LogMessage(this,"Not connected to metadata, please
     * specify job execution class:"); className = stdin.readLine(); // request class } return "Done"; }
     */

    private String syntaxError(int pCommand) {
        return "Syntax Error: Wrong syntax for " + pCommand + "\n" + strSyntax[pCommand];
    }

    private String unknownCommand(String pCommand) {
        return ("Error unknown command: " + pCommand);
    }
}
