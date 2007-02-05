/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;

/**
 * Insert the type's description here. Creation date: (5/3/2002 5:39:29 PM)
 * 
 * @author: Administrator
 */
public abstract class ETLJobExecutor extends Thread {

    protected boolean bShutdown = false;
    protected LinkedList llPendingQueue = null;
    protected int iSleepPeriod = 100;
    protected ETLJobExecutorStatus jesStatus = new ETLJobExecutorStatus();
    protected DocumentBuilderFactory dmfFactory;
    protected ArrayList aesOverrideParameters = null;
    protected String[] aesIgnoreQAs = null;
    protected String msXMLOverride = "";
    protected boolean mbCommandLine = true;

    /**
     * ETLJobExecutorThread constructor comment.
     */
    public ETLJobExecutor() {
        super();
        dmfFactory = DocumentBuilderFactory.newInstance();
    }

    /**
     * ETLJobExecutorThread constructor comment.
     * 
     * @param target java.lang.Runnable
     */
    public ETLJobExecutor(Runnable target) {
        super(target);
    }

    /**
     * ETLJobExecutorThread constructor comment.
     * 
     * @param target java.lang.Runnable
     * @param name java.lang.String
     */
    public ETLJobExecutor(Runnable target, String name) {
        super(target, name);
    }

    /**
     * ETLJobExecutorThread constructor comment.
     * 
     * @param name java.lang.String
     */
    public ETLJobExecutor(String name) {
        super(name);
    }

    /**
     * ETLJobExecutorThread constructor comment.
     * 
     * @param group java.lang.ThreadGroup
     * @param target java.lang.Runnable
     */
    public ETLJobExecutor(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    /**
     * ETLJobExecutorThread constructor comment.
     * 
     * @param group java.lang.ThreadGroup
     * @param target java.lang.Runnable
     * @param name java.lang.String
     */
    public ETLJobExecutor(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    /**
     * ETLJobExecutorThread constructor comment.
     * 
     * @param group java.lang.ThreadGroup
     * @param name java.lang.String
     */
    public ETLJobExecutor(ThreadGroup group, String name) {
        super(group, name);
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 6:49:24 PM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    protected abstract boolean executeJob(ETLJob jCurrentJob);

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:34:09 PM)
     * 
     * @return int
     */
    public int getSleepPeriod() {
        return iSleepPeriod;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:23:02 AM)
     * 
     * @return com.kni.etl.ETLJobExecutorStatus
     */
    public ETLJobExecutorStatus getStatus() {
        return jesStatus;
    }

    private static int exit(int code, Throwable e, boolean pExitCleanly) {
        if (pExitCleanly)
            return code;

        throw e == null ? new RuntimeException("Exit code: " + code) : (e instanceof RuntimeException? (RuntimeException)e:new RuntimeException(e));
    }

    public static void execute(String[] args, ETLJobExecutor pETLJobExecutor, boolean pExitCleanly) {
        int res = _execute(args, pETLJobExecutor, pExitCleanly, 0);
        ResourcePool.releaseLoadLookups(0);

        if (pExitCleanly)
            System.exit(res);
    }

    public static int _execute(String[] args, ETLJobExecutor pETLJobExecutor, boolean pExitCleanly, int iLoadID) {
        long lStartTime;
        long lEndTime;

        // declare metadata object
        Metadata md = null;

        // declare XML filename
        String fileName = null;

        // declare job name override
        String jobName = null, jobID = null;

        ArrayList overrideParameters = new ArrayList();
        String[] ignoreQAs = null;
        String server = null;

        // extract login information for metadata and xml filename
        for (int index = 0; index < args.length; index++) {
            if ((server == null) && (args[index].indexOf("SERVER=") != -1)) {
                server = ArgumentParserUtil.extractArguments(args[index], "SERVER=");
            }

            if ((ignoreQAs == null) && (args[index].indexOf("IGNOREQA=[") != -1)) {
                ignoreQAs = ArgumentParserUtil.extractMultipleArguments(args[index], "IGNOREQA=[");
            }

            if ((fileName == null) && (args[index].indexOf("FILE=") != -1)) {
                fileName = ArgumentParserUtil.extractArguments(args[index], "FILE=");
            }

            if ((jobName == null) && (args[index].indexOf("JOB_NAME=") != -1)) {
                jobName = ArgumentParserUtil.extractArguments(args[index], "JOB_NAME=");
            }
            if ((jobID == null) && (args[index].indexOf("JOBID=") != -1)) {
                jobID = ArgumentParserUtil.extractArguments(args[index], "JOBID=");
            }

            if ((args[index].indexOf("LOADID=") != -1)) {
                iLoadID = Integer.parseInt(ArgumentParserUtil.extractArguments(args[index], "LOADID="));
            }

            if (args[index].indexOf("PARAMETER=[") != -1) {
                String[] param = ArgumentParserUtil.extractMultipleArguments(args[index], "PARAMETER=[");
                overrideParameters.add(param);
            }
        }

        // if filename is null report error
        if (fileName == null) {
            System.out
                    .println("Wrong arguments:  FILE=<XML_FILE> (SERVER=localhost) (JOB_NAME=<NAME>) (PARAMETER=[(TestList),PATH,/u01]) (IGNOREQA=[FileTest,SizeTest])");
            System.out.println("example:  FILE=c:\\transform.xml JOB_NAME=Transform SERVER=localhost");

            return exit(com.kni.etl.EngineConstants.WRONG_ARGUMENT_EXIT_CODE, null, pExitCleanly);
        }

        // metadata object isn't set and login information found then connect to metadata

        try {
            Document doc;
            if (ResourcePool.getMetadata() == null) {
                if ((doc = Metadata.LoadConfigFile(null, Metadata.CONFIG_FILE)) != null) {
                    md = connectToServer(doc, server);
                    ResourcePool.setMetadata(md);
                }
            }
        } catch (Exception e1) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE, "Metadata not available, check KETLServers.xml - "
                    + e1.getMessage() + ", ");
            ResourcePool.setMetadata(null);
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

            return exit(com.kni.etl.EngineConstants.READXML_ERROR_EXIT_CODE, e, pExitCleanly);
        }

        String strJobXML = sb.toString();

        DocumentBuilder builder = null;
        Document xmlDOM = null;
        NodeList nl;
        Node node;

        // Build a DOM out of the XML string...
        try {
            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            builder = dmf.newDocumentBuilder();
            xmlDOM = builder.parse(new InputSource(new StringReader(strJobXML)));

            nl = xmlDOM.getElementsByTagName("JOB");

            NodeList nlp = xmlDOM.getElementsByTagName("PARAMETER_LIST");
            NodeList nlq = xmlDOM.getElementsByTagName("QA");

            if ((nl.getLength() > 1) && (jobName != null)) {
                System.out.println("ERROR: JOB_NAME argument not applicable to XML file with multiple jobs");

                return exit(com.kni.etl.EngineConstants.MULTIJOB_JOB_OVERRIDE_ERROR_EXIT_CODE, null, pExitCleanly);
            }

            for (int i = 0; i < nl.getLength(); i++) {
                node = nl.item(i);

                boolean execute = (jobID == null || XMLHelper.getAttributeAsString(node.getAttributes(), "ID", "")
                        .equals(jobID));

                if (execute && node.getNodeType() == Node.ELEMENT_NODE) {

                    ETLJobExecutor je = pETLJobExecutor;
                    ETLJob kj = je.getNewJob();

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
                                    return exit(com.kni.etl.EngineConstants.BADLY_FORMED_ARGUMENT_EXIT_CODE, null,
                                            pExitCleanly);
                                }
                            }

                            if (str.length == 3) {
                                if ((str[0] == null) || (str[1] == null) || (str[2] == null)) {
                                    System.out.println("ERROR: Badly formed parameter override ParameterListName = "
                                            + str[0] + " ParameterName=" + str[1] + ", ParameterValue=" + str[2]);
                                    return exit(com.kni.etl.EngineConstants.BADLY_FORMED_ARGUMENT_EXIT_CODE, null,
                                            pExitCleanly);
                                }
                            }

                            if (str.length == 2) {
                                je.msXMLOverride = je.msXMLOverride + "\t<" + XMLHelper.PARAMETER_TAG + " "
                                        + XMLHelper.NAME_TAG + "=\"" + str[0] + "\">" + str[1] + "</"
                                        + XMLHelper.PARAMETER_TAG + ">\n";
                            }
                            else if (str.length == 3) {
                                je.msXMLOverride = je.msXMLOverride + "\t<" + XMLHelper.PARAMETER_TAG + " "
                                        + XMLHelper.PARAMETER_LIST_TAG + "=\"" + str[0] + "\" " + XMLHelper.NAME_TAG
                                        + "=\"" + str[1] + "\">" + str[2] + "</" + XMLHelper.PARAMETER_TAG + ">\n";
                            }
                        }
                    }

                    // close parameter override tag
                    je.msXMLOverride = je.msXMLOverride + "</" + XMLHelper.PARAMETER_LIST_TAG + ">\n";

                    // build temp DOM to generate nodes
                    Document tmpXMLDOM = builder.parse(new InputSource(new StringReader(je.msXMLOverride)));

                    // import nodes into main document
                    Node newNode = xmlDOM.importNode(tmpXMLDOM.getFirstChild(), true);

                    // append new nodes to the job
                    node.appendChild(newNode);

                    kj.setAction(XMLHelper.outputXML(node));

                    if (jobName == null) {
                        kj.setJobID(XMLHelper.getAttributeAsString(node.getAttributes(), "ID", XMLHelper.getAttributeAsString(node.getAttributes(), "NAME", null)));
                    }
                    else {
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

                        return exit(kj.getStatus().getErrorCode(), kj.getStatus().getException(), pExitCleanly);

                    }

                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Total execution time: "
                            + ((lEndTime - lStartTime) / 1000.0) + " seconds");
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Job complete.");
                }
            }

        } catch (org.xml.sax.SAXException e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "ERROR: parsing XML document, "
                    + e.toString());
            return (exit(EngineConstants.INVALID_XML_EXIT_CODE, e, pExitCleanly));
        } catch (RuntimeException e) {
            
            ResourcePool.LogException(e.getCause()==null?e:(Exception) e.getCause(), null);
            return (exit(EngineConstants.OTHER_ERROR_EXIT_CODE, e, pExitCleanly));
        } catch (Exception e) {
            ResourcePool.LogException(e, null);
            return (exit(EngineConstants.OTHER_ERROR_EXIT_CODE, e, pExitCleanly));
        }

        return 0;
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
     * Insert the method's description here. Creation date: (5/7/2002 11:52:15 AM)
     */
    protected abstract boolean initialize();

    /**
     * Loops on the job queue, taking each job and running with it. Creation date: (5/3/2002 5:43:04 PM)
     */
    public void run() {
        ETLJob jCurrentJob;
        boolean bSuccess;

        mbCommandLine = false;

        String orginalName = this.getName();

        this.setName(orginalName + "(" + this.getClass().getName() + ") - Starting");

        // Run any initialization code that the subclasses will need...
        if (initialize() == false) {
            jesStatus.setStatusCode(ETLJobExecutorStatus.ERROR);

            return;
        }

        while (bShutdown == false) {
            this.setName(orginalName + "(" + this.getClass().getName() + ") - Ready");
            // We're ready to get the next job, so set our status to READY...
            if (jesStatus.getStatusCode() != ETLJobExecutorStatus.READY) {
                jesStatus.setStatusCode(ETLJobExecutorStatus.READY);
            }

            jCurrentJob = null;

            synchronized (llPendingQueue) {
                if (llPendingQueue.size() > 0) {
                    jCurrentJob = (ETLJob) llPendingQueue.removeLast();
                }
            }

            // If there was no job for us, sleep for a bit and check again...
            if (jCurrentJob == null) {
                try {
                    if (this.getSleepPeriod() < 2000) {
                        this.setSleepPeriod(this.getSleepPeriod() + 500);
                    }

                    sleep(this.getSleepPeriod());
                } catch (Exception e) {
                }

                continue;
            }

            this.setSleepPeriod(100);

            // We've got a job, so set our executor's status to WORKING...
            jesStatus.setStatusCode(ETLJobExecutorStatus.WORKING);
            this.setName(this.getName() + "(" + this.getClass().getName() + ") - Executing");
            // Make sure that this job wasn't cancelled while it was in the queue...
            if (jCurrentJob.isCancelled()) {
                // Set the status and drop the job...
                jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_CANCELLED);
                this.setName(orginalName + "(" + this.getClass().getName() + ") - Cancelled");
                continue;
            }

            // Update the status of the job and let's go!
            jCurrentJob.getStatus().setStatusCode(ETLJobStatus.EXECUTING);
            bSuccess = executeJob(jCurrentJob);

            // Update the status to done if the subclass forgot to...
            if (jCurrentJob.getStatus().getStatusCode() == ETLJobStatus.EXECUTING) {
                if (bSuccess) {
                    jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL);
                }
                else {
                    jCurrentJob.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_FAILED);
                }
            }
        }

        // Run any initialization code that the subclasses will need...but set the shutdown status regardless...
        jesStatus.setStatusCode(ETLJobExecutorStatus.SHUTTING_DOWN);
        terminate();
        jesStatus.setStatusCode(ETLJobExecutorStatus.TERMINATED);

    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 8:01:20 PM)
     * 
     * @param param java.util.LinkedList
     */
    public void setPendingQueue(LinkedList llQueue) {
        llPendingQueue = llQueue;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:34:09 PM)
     * 
     * @param newSleepPeriod int
     */
    public void setSleepPeriod(int newSleepPeriod) {
        iSleepPeriod = newSleepPeriod;
    }

    /**
     * This is the publicly accessible function to set the "shutdown" flag for the thread. It will no longer process any
     * new jobs, but finish what it's working on. It will then call the internal terminate() function to close down
     * anything it needs to. BRIAN: should we make this final? Creation date: (5/3/2002 6:50:09 PM)
     */
    public void shutdown() {
        bShutdown = true;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 2:49:24 PM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    public abstract boolean supportsJobType(ETLJob jJob);

    protected abstract ETLJob getNewJob() throws Exception;

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:52:41 AM)
     */
    protected boolean terminate() {
        return true;
    }

}
