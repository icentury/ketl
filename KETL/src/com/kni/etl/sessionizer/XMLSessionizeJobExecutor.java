/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobExecutor;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.KETLJobStatus;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ParallelInlineSortFileReader;
import com.kni.etl.ketl_v1.ResultRecord;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;

public class XMLSessionizeJobExecutor extends ETLJobExecutor {

    boolean multipleSequencesAllowed = true;
    private static boolean showProgress = false;

    /*
     * This main method allows the user to directly execute the sessionizer basically you create an XMLSessionizerJob
     * and XMLSessionizerJobExecutor thread and pass the XML file contents to it and execute.
     */
    public static void main(String[] args) {
        // declare simple statistics variables
        long lStartTime;
        long lEndTime;

        // declare metadata object
        Metadata md = null;

        // declare login metadata array
        String[] mdUser = null;

        // declare XML filename
        String fileName = null;

        // enable progress reporting, e.g 3000 inserts a second etc.
        showProgress = true;

        // extract login information for metadata and xml filename
        for (int index = 0; index < args.length; index++) {
            if ((mdUser == null) && (args[index].indexOf("MD_USER=[") != -1)) {
                mdUser = ArgumentParserUtil.extractMultipleArguments(args[index], "MD_USER=[");
            }

            if ((fileName == null) && (args[index].indexOf("FILE=") != -1)) {
                fileName = ArgumentParserUtil.extractArguments(args[index], "FILE=");
            }
        }

        // if filename is null report error
        if (fileName == null) {
            System.out.println("Wrong arguments:  FILE=<XML_FILE> MD_USER=[USER,PWD,JDBCURL,JDBCDriver,MDPRefix]");
            System.out
                    .println("example:  FILE=c:\\sessionconfig.xml MD_USER=[ETLUSER,ETLPWD,jdbc:oracle:oci8:@DEV3ORA,oracle.jdbc.driver.OracleDriver,KETL]");

            return;
        }

        // metadata object isn't set and login information found then connect to metadata
        if ((md == null) && (mdUser != null)) {
            Metadata mds = null;

            String mdPrefix = null;

            if ((mdUser != null) && (mdUser.length == 5)) {
                mdPrefix = mdUser[4];
            }

            try {
                mds = new Metadata(true);
                mds.setRepository(mdUser[0], mdUser[1], mdUser[2], mdUser[3], mdPrefix);
            } catch (Exception e1) {
                e1.printStackTrace();
                return;
            }

            ResourcePool.setMetadata(mds);
            md = ResourcePool.getMetadata();
        }

        // Read XML from file and set as action for job executor
        StringBuffer sb = new StringBuffer();

        try {
            FileReader inputFileReader = new FileReader(fileName);
            int c;

            while ((c = inputFileReader.read()) != -1) {
                sb.append((char) c);
            }
        } catch (Exception e) {
            System.out.println("Error reading file '" + args[0] + "': " + e.getMessage());

            return;
        }

        // convert to pure string - B. Sullivan does this for some reason
        String strJobXML = sb.toString();

        // declare xml manipulation objects
        DocumentBuilder builder = null;
        Document xmlDOM = null;
        NodeList nl;
        Node node;

        // Build a DOM out of the XML string...
        try {
            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            builder = dmf.newDocumentBuilder();
            xmlDOM = builder.parse(new InputSource(new StringReader(strJobXML)));

            // get job tag, only 1 should exist, multiple job support not implemented yet
            // TODO: add multiple job support
            nl = xmlDOM.getElementsByTagName("JOB");

            // get parameter list
            NodeList nlp = xmlDOM.getElementsByTagName("PARAMETER_LIST");

            // get QA tags
            NodeList nlq = xmlDOM.getElementsByTagName("QA");

            for (int i = 0; i < nl.getLength(); i++) {
                node = nl.item(i);

                // if element node then handle
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    XMLSessionizeJobExecutor je = new XMLSessionizeJobExecutor();
                    KETLJobStatus jobStatus = new KETLJobStatus();
                    XMLSessionizeJob kj = new XMLSessionizeJob(jobStatus);

                    ArrayList ar = new ArrayList();

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

                    // set action of job - VERY IMPORTANT
                    kj.setAction(XMLHelper.outputXML(node));

                    // set jobID
                    kj.setJobID(XMLHelper.getAttributeAsString(node.getAttributes(), "NAME", null));

                    // get start time
                    lStartTime = System.currentTimeMillis();

                    // execute job
                    je.executeJob(kj);

                    // get end time
                    lEndTime = System.currentTimeMillis();

                    // get status object details
                    // the status object can be extracted earlier before execution
                    // and will allow external monitoring of the job in a different thread
                    if (kj.getStatus().getErrorCode() != 0) {
                        ResourcePool.LogMessage(Thread.currentThread(), "Job failed (" + kj.getStatus().getErrorCode()
                                + ") : " + kj.getStatus().getErrorMessage());

                        return;
                    }

                    // output end stats, as this is direct execution.
                    ResourcePool.LogMessage(Thread.currentThread(), "Total execution time: "
                            + ((lEndTime - lStartTime) / 1000.0) + " seconds");
                    ResourcePool.LogMessage(Thread.currentThread(), "Job complete.");
                }
            }
        }

        catch (org.xml.sax.SAXException e) {
            ResourcePool.LogMessage(Thread.currentThread(), "ERROR: parsing XML document, " + e.toString());

            return;
        } catch (Exception e) {
            ResourcePool.LogException(e, null);

            return;
        }
    }

    protected HashMap hmConnections = new HashMap();

    /**
     * SessionizerJobExecutor constructor comment.
     */
    public XMLSessionizeJobExecutor() {
        super();
    }

    public XMLSessionizeJobExecutor(Runnable target) {
        super(target);
    }

    /**
     * SessionizerJobExecutor constructor comment.
     * 
     * @param target java.lang.Runnable
     * @param name java.lang.String
     */
    public XMLSessionizeJobExecutor(Runnable target, String name) {
        super(target, name);
    }

    /**
     * SessionizerJobExecutor constructor comment.
     * 
     * @param name java.lang.String
     */
    public XMLSessionizeJobExecutor(String name) {
        super(name);
    }

    /**
     * SessionizerJobExecutor constructor comment.
     * 
     * @param group java.lang.ThreadGroup
     * @param target java.lang.Runnable
     */
    public XMLSessionizeJobExecutor(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    /**
     * SessionizerJobExecutor constructor comment.
     * 
     * @param group java.lang.ThreadGroup
     * @param target java.lang.Runnable
     * @param name java.lang.String
     */
    public XMLSessionizeJobExecutor(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    /**
     * SessionizerJobExecutor constructor comment.
     * 
     * @param group java.lang.ThreadGroup
     * @param name java.lang.String
     */
    public XMLSessionizeJobExecutor(ThreadGroup group, String name) {
        super(group, name);
    }

    protected AnalyzeSessionQueuedThread restartJob(XMLSessionizeJob sjJob, int pExpectedParallism) {
        AnalyzeSessionQueuedThread res = null;
        FileInputStream in;

        try {
            in = new FileInputStream(this.getRestartFilenaname(sjJob));
        } catch (FileNotFoundException e) {
            return null;
        }

        ObjectInputStream s;

        try {
            s = new ObjectInputStream(in);
            res = (AnalyzeSessionQueuedThread) s.readObject();
        } catch (Exception e) {
            ResourcePool.LogMessage("WARNING: Restart file contains errors, file will be ignored " + e.toString());
        }

        return res;
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 7:59:26 AM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    protected boolean executeJob(ETLJob ejJob) {
        XMLSessionizeJob sjJob;
        ETLJobStatus jsJobStatus;

        // Only accept SQL jobs...
        if ((ejJob instanceof XMLSessionizeJob) == false) {
            return false;
        }

        sjJob = (XMLSessionizeJob) ejJob;
        jsJobStatus = sjJob.getStatus();

        if (jsJobStatus.getErrorCode() > 0) {
            return false;
        }

        AnalyzeSessionQueuedThread destAnalyzeSessionThread = null;

        // Run the SQL job
        try {
            sjJob.loadDestinationSettings();
            ParallelInlineSortFileReader pFileReader = sjJob.createFileChannelParsers();

            ResultRecord record = null;

            int recordCount = 0;
            java.util.Date startDate = null;
            int parallelSessionizers = 1;
            IDCounter iBadSessions = new IDCounter();
            int waitQueueSize = sjJob.mWaitQueueSize;

            // create metadata managed id counter
            IDCounter idCounter = new IDCounter("TEMP_SESSION_ID", 10000);

            BlockingQueue destDataQueue = null;

            // check for existance of restart file if reload sessions enabled
            if (sjJob.restartSessions()
                    && ((destAnalyzeSessionThread = restartJob(sjJob, parallelSessionizers)) != null)) {
                jsJobStatus.setExtendedMessage("Reloading non-final sessions from last run.");
            }
            else {
                destAnalyzeSessionThread = null;
            }

            if (destAnalyzeSessionThread == null) {
                destAnalyzeSessionThread = new AnalyzeSessionQueuedThread(jsJobStatus, 0);
                destDataQueue = new LinkedBlockingQueue(waitQueueSize);
                destAnalyzeSessionThread.setDataQueue(destDataQueue);
            }
            else {
                destAnalyzeSessionThread.setJobStatus(jsJobStatus);
                destAnalyzeSessionThread.setPartitionIdentifier(0);
                destAnalyzeSessionThread.setName();
                destDataQueue = destAnalyzeSessionThread.getDataQueue();
                destAnalyzeSessionThread.shutdown = false;
            }

            destAnalyzeSessionThread.setJob(sjJob);
            destAnalyzeSessionThread.idCounter = idCounter;
            destAnalyzeSessionThread.waitQueueSize = waitQueueSize;
            destAnalyzeSessionThread.iBadSessions = iBadSessions;
            destAnalyzeSessionThread.ParentThread = Thread.currentThread();

            destAnalyzeSessionThread.start();

            // Pageview queue for pageview output writes
            if (startDate == null) {
                startDate = new java.util.Date();
            }

            do {
                record = pFileReader.getNextLine();

                if ((record != null)) {
                    recordCount++;
                    record = (ResultRecord) record.clone();
                    record.OverallLine = recordCount;

                    // put the record on the queue
                    destDataQueue.put(record);
                }

                try {
                    // check for errors in threads
                    // if a queue occurs then there maybe a pageview writer error
                    if (destAnalyzeSessionThread.fatalError) {
                        ResourcePool.LogMessage(this, "ERROR: Fatal error analyzing hit, see previous errors.");

                        // shutdown the threads
                        this.forceShutdown(destAnalyzeSessionThread, destDataQueue);
                        this.waitForShutdown(destAnalyzeSessionThread);

                        jsJobStatus.setErrorCode(4);
                        jsJobStatus.setErrorMessage("Fatal error analyzing data, see log");

                        return false;
                    }
                } catch (Exception e2) {
                    ResourcePool.LogException(e2, this);
                }

                if (((recordCount != 0) && ((recordCount % 50000) == 0)) || (record == null)) {
                    logStatus(jsJobStatus, pFileReader, recordCount, startDate, iBadSessions);
                }
            } while (record != null);

            // close all still running sessions as end of web log has been reached
            // set close out session type
            destAnalyzeSessionThread.closeOutMode = sjJob.closeOutSessionModeType();

            // put end marker on the queue
            destDataQueue.put(AnalyzeSessionQueuedThread.END_MARKER);

            // serialize job restart sessions allowed
            if (sjJob.restartSessions()) {
                serializeSessions(sjJob, destAnalyzeSessionThread);
            }

            // sleep whilst threads shutting down
            waitForShutdown(destAnalyzeSessionThread);

            // check for fatal errors
            if (destAnalyzeSessionThread.fatalError) {
                ResourcePool.LogMessage(this, "ERROR: Fatal error analyzing hit, see previous errors.");

                jsJobStatus.setErrorCode(4);
                jsJobStatus.setErrorMessage("Fatal error analyzing data, see log");

                return false;
            }

            idCounter.setToCurrentID();

            if (recordCount == 0) {
                jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES
                jsJobStatus.setErrorMessage("No data found and loaded, check data source");

                return false;
            }
        } catch (Exception e) {
            if (destAnalyzeSessionThread != null)
                destAnalyzeSessionThread.interrupt();

            jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES
            jsJobStatus.setErrorMessage("Error running sessionize job: " + e);

            return false;
        }

        return true;
    }

    private boolean serializeSessions(ETLJob pJob, AnalyzeSessionQueuedThread pAnalyzeSessionThread)
            throws IOException, InterruptedException {
        boolean threadsNotReady = true;
        boolean res = true;

        do {
            threadsNotReady = false;

            if (pAnalyzeSessionThread.fatalError) {
                res = false;
            }

            if (pAnalyzeSessionThread.readyToSerialize() == false) {
                threadsNotReady = true;
                Thread.sleep(25);
            }
        } while (threadsNotReady);

        if (res) {
            FileOutputStream out = new FileOutputStream(getRestartFilenaname(pJob));
            ObjectOutputStream s = new ObjectOutputStream(out);
            s.writeObject(pAnalyzeSessionThread);
            s.flush();
            ResourcePool.LogMessage("Restart file will be written to " + this.getRestartFilenaname(pJob));
        }

        synchronized (pAnalyzeSessionThread) {
            pAnalyzeSessionThread.notify();
        }

        return res;
    }

    /**
     * @param jsJobStatus
     * @param pFileReader
     * @param recordCount
     * @param startDate
     * @param iBadSessions
     */
    private void logStatus(ETLJobStatus jsJobStatus, ParallelInlineSortFileReader pFileReader, int recordCount,
            java.util.Date startDate, IDCounter iBadSessions) {
        java.util.Date currDate = new java.util.Date();
        long diff = currDate.getTime() - startDate.getTime();

        // don't want to divide by 0!
        if (diff < 1000) {
            diff = 1000;
        }

        String msg = "Records Processed: " + String.valueOf(recordCount) + " at " + (recordCount / (diff / 1000))
                + " lines a second, in " + (diff / 1000)
                + " seconds\n\t\tStatistics\n\t\t----------\n\t\tBad Sessions: " + iBadSessions.getID()
                + "\n\t\tRead rate: " + (NumberFormatter.format((pFileReader.getBytesRead() / (diff / 1000))) + "/sec");

        if (showProgress) {
            ResourcePool.LogMessage(msg);
        }

        jsJobStatus.setExtendedMessage(msg);
        jsJobStatus.messageChanged = true;

    }

    /**
     * @param parallelSessionizers
     * @param destAnalyzeSessionThreads
     * @throws InterruptedException
     */
    private void waitForShutdown(Thread pThread) throws InterruptedException {
        boolean threadsAlive = true;

        do {
            threadsAlive = false;

            if (pThread.isAlive()) {
                threadsAlive = true;
                Thread.sleep(25);
            }
        } while (threadsAlive);
    }

    protected String getRestartFilenaname(ETLJob sjJob) {
        String tempdir = System.getProperty("java.io.tmpdir");

        if ((tempdir != null) && (tempdir.endsWith("/") == false) && (tempdir.endsWith("\\") == false)) {
            tempdir = tempdir + "/";
        }
        else if (tempdir == null) {
            tempdir = "";
        }

        return tempdir + "KETL." + sjJob.getJobID() + ".restart";
    }

    protected Connection getConnection(String strDriverClass, String strURL, String strUserName, String strPassword)
            throws Exception {
        String strConnectionKey;
        Connection cConnection;

        // Build the key that we will use to refer to this database connection...
        strConnectionKey = strDriverClass + strURL + strUserName + strPassword;

        // See if we already have a connection to this database, and if so return that one...
        if ((cConnection = (Connection) hmConnections.get(strConnectionKey)) != null) {
            return cConnection;
        }

        // Load the JDBC driver
        try {
            Class.forName(strDriverClass);
        } catch (Exception e) {
            throw e; // Throw it back to calling class
        }

        // Connect to the database...
        cConnection = DriverManager.getConnection(strURL, strUserName, strPassword);

        // Add this connection to our database...
        if (cConnection != null) {
            hmConnections.put(strConnectionKey, cConnection);
        }

        return cConnection;
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 7:59:26 AM)
     */
    protected boolean initialize() {
        return true;
    }

    /**
     * SessionizerJobExecutor constructor comment.
     * 
     * @param target java.lang.Runnable
     */
    /**
     * Insert the method's description here. Creation date: (5/13/2002 7:59:26 AM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    public boolean supportsJobType(ETLJob jJob) {
        // Only accept SQL jobs...
        return (jJob instanceof XMLSessionizeJob);
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 7:59:26 AM)
     */
    protected boolean terminate() {
        return true;
    }

    /**
     * @param parallelSessionizers
     * @param destAnalyzeSessionThreads
     * @throws InterruptedException
     */
    private void forceShutdown(AnalyzeSessionQueuedThread pThread, BlockingQueue pQueue) throws InterruptedException {
        if (pThread.isAlive() && (pThread.shutdown == false)) {
            pThread.fatalError = true;
            pThread.shutdown = true;
            pQueue.clear();
            pQueue.put(AnalyzeSessionQueuedThread.END_MARKER);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLJobExecutor#getNewJob()
     */
    @Override
    protected ETLJob getNewJob() throws Exception {
        return null;
    }
}
