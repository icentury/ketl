/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

import java.sql.SQLException;
import java.util.StringTokenizer;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;

class ExecuteJob {

    /**
     * ExecuteJob constructor comment.
     */
    public ExecuteJob() {
        super();
    }

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
            }
            else if (fields.length() > 0) {
                result = new String[1];
                result[0] = fields;
            }
        }

        return (result);
    }

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

        // metadata object isn't set and login information found then connect to metadata

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
     * Starts the application.
     * 
     * @param args an array of command-line arguments
     */
    public static void main(java.lang.String[] args) {

        // String mdServer = null;
        String jobID = null;

        String server = null;

        // String mdServer = null;
        String projectID = null;

        // String mdServer = null;
        String ignoreDependencies = null;

        // String mdServer = null;
        String allowMultiple = null;

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

            if ((ignoreDependencies == null) && (element.indexOf("IGNORE_DEPENDENCIES=") != -1)) {
                ignoreDependencies = ExecuteJob.extractArguments(element, "IGNORE_DEPENDENCIES=");
            }

            if ((allowMultiple == null) && (element.indexOf("ALLOW_MULTIPLE=") != -1)) {
                allowMultiple = ExecuteJob.extractArguments(element, "ALLOW_MULTIPLE=");
            }
        }

        if (allowMultiple == null) {
            allowMultiple = "FALSE";
        }

        if ((server == null) || (ignoreDependencies == null) || (projectID == null) || (jobID == null)) {
            System.out
                    .println("Wrong arguments:  SERVER=TEST PROJECT_ID=1 JOB_ID=TEST_SCRIPT IGNORE_DEPENDENCIES=FALSE [ALLOW_MULTIPLE=FALSE]");
            System.out
                    .println("example:  SERVER=TEST PROJECT_ID=1 JOB_ID=TEST_SCRIPT IGNORE_DEPENDENCIES=FALSE ALLOW_MULTIPLE=FALSE");

            return;
        }
        Metadata md = null;

        try {
            md = ExecuteJob.connectToServer(Metadata.LoadConfigFile(null, Metadata.CONFIG_FILE), server);
        } catch (Exception e1) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Connecting to metadata - "
                    + e1.getMessage());
            System.exit(com.kni.etl.EngineConstants.METADATA_ERROR_EXIT_CODE);
        }

        boolean ignoreDeps = false;
        boolean allowMult = false;

        if (ignoreDependencies.compareToIgnoreCase("true") == 0) {
            ignoreDeps = true;
        }

        if (allowMultiple.compareToIgnoreCase("true") == 0) {
            allowMult = true;
        }

        int pID;

        try {
            pID = Integer.parseInt(projectID);
        } catch (Exception e) {
            System.out.println("Invalid project id");

            return;
        }

        try {
            com.kni.etl.ETLJob[] e = md.getJobDetails(jobID);

            if (e == null || e.length == 0) {
                System.err.println("Job " + jobID + " not found in metadata.");
                System.exit(-1);
            }
            if (md.executeJob(pID, jobID, ignoreDeps, allowMult)) {
                System.out.println("Job submitted to server for direct execution.");
            }
            else {
                System.out.println("Warning Job not submitted to server for execution.");

                md.closeMetadata();

                System.exit(1);
            }
        } catch (SQLException e) {
            System.out.println(e);
            md.closeMetadata();
        } catch (Exception e) {
            System.out.println(e);
        }

        md.closeMetadata();
    }
}
