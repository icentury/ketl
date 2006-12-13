/*
 * Created on Apr 22, 2006
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLCluster;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class XMLMetadataBridge implements XMLMetadataCalls {

    private static Document xmlConfig = null;
    private static Object mLock = new Object();

    private static DocumentBuilder mDocBuilder;
    private final static String ROOTNODE_TAG = "ETL";

    private SimpleDateFormat mDefaultDateFormat = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss Z");

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getServerList()
     */
    private static synchronized Element getNewDocument() {
        Document doc = mDocBuilder.newDocument();
        Element e = doc.createElement(ROOTNODE_TAG);
        doc.appendChild(e);
        doc.setXmlStandalone(true);
        return e;
    }

    private static Element addChildElement(Node target, String newTag) {
        Document doc = target.getOwnerDocument();
        Element e = doc.createElement(newTag);
        target.appendChild(e);
        return e;
    }

    private static HashMap mdCache = new HashMap();

    private Metadata getMetadataByServer(String pServerID) throws Exception {

        if (pServerID == null) {
            throw new KETLException("Server ID cannot be null");
        }

        if (mdCache.containsKey(pServerID)) {
            ResourcePool.setMetadata((Metadata) mdCache.get(pServerID));
            return ResourcePool.getMetadata();
        }

        if (xmlConfig == null) {
            throw new KETLException("KETL path or KETL servers file not found");
        }

        // try for localhost
        Node serverNode = XMLHelper.findElementByName(xmlConfig, "SERVER", "NAME", pServerID);

        if (serverNode == null) {
            throw new KETLException("ERROR: Problems getting server name, check config file");
        }

        String[] mdUser = new String[5];

        mdUser[0] = XMLHelper.getChildNodeValueAsString(serverNode, "USERNAME", null, null, null);
        mdUser[1] = XMLHelper.getChildNodeValueAsString(serverNode, "PASSWORD", null, null, null);
        mdUser[2] = XMLHelper.getChildNodeValueAsString(serverNode, "URL", null, null, null);
        mdUser[3] = XMLHelper.getChildNodeValueAsString(serverNode, "DRIVER", null, null, null);
        mdUser[4] = XMLHelper.getChildNodeValueAsString(serverNode, "MDPREFIX", null, null, "");
        String passphrase = XMLHelper.getChildNodeValueAsString(serverNode, "PASSPHRASE", null, null, null);

        Metadata md = null;

        String mdPrefix = null;

        if ((mdUser != null) && (mdUser.length == 5)) {
            mdPrefix = mdUser[4];
        }

        // use metadata repository now
        md = new Metadata(true,passphrase);

        md.setRepository(mdUser[0], mdUser[1], mdUser[2], mdUser[3], mdPrefix);

        // cache the connection
        mdCache.put(pServerID, md);

        ResourcePool.setMetadata(md);

        return ResourcePool.getMetadata();
    }

    public String handleError(Exception e) {
        Element e1 = getNewDocument();
        addChildElement(e1, "ERROR").setTextContent(e.getMessage());
        if (e instanceof PassphraseException) {
            System.out.println(e.getMessage());
            System.out.println("Passphrase location: " + ((PassphraseException) e).getPassphraseFilePath());
        }
        else
            e.printStackTrace();
        return XMLHelper.outputXML(e1, true);
    }

    public String getServerList() {

        Element root = getNewDocument();

        NodeList nl = xmlConfig.getElementsByTagName("SERVER");
        String defaultServername = XMLHelper.getAttributeAsString(xmlConfig.getFirstChild().getAttributes(),
                "DEFAULTSERVER", "");

        Element servers = addChildElement(root, "SERVERS");

        for (int i = 0; i < nl.getLength(); i++) {
            String nm = XMLHelper.getAttributeAsString(nl.item(i).getAttributes(), "NAME", null);
            if (nm != null) {
                Element server = addChildElement(servers, "SERVER");
                server.setTextContent(nm);
                if (defaultServername != null && defaultServername.equalsIgnoreCase(nm))
                    server.setAttribute("DEFAULT", "TRUE");
            }
        }

        return XMLHelper.outputXML(root, true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getServerClusterDetails()
     */
    public String getServerClusterDetails(String pRootServerID) {

        try {

            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pRootServerID);

                KETLCluster kc = md.getClusterDetails();

                return kc.getAsXML();

            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getLoads(int, java.util.Date)
     */
    public String getLoads(String pServerID, Date pLastModified) {
        Element e = getNewDocument();

        try {

            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);

                ETLLoad[] loads = md.getLoads(pLastModified, -1);

                Element loadsRoot = addChildElement(e, "LOADS");

                for (int i = 0; i < loads.length; i++) {
                    Element ld = addChildElement(loadsRoot, "LOAD");
                    ld.setAttribute("START_JOB_ID", loads[i].start_job_id);
                    ld.setAttribute("LOAD_ID", Integer.toString(loads[i].LoadID));
                    ld.setAttribute("FAILED", loads[i].failed ? "TRUE" : "FALSE");
                    ld.setAttribute("IGNORED_PARENTS", loads[i].ignored_parents ? "TRUE" : "FALSE");
                    ld.setAttribute("PROJECT_ID", Integer.toString(loads[i].project_id));
                    ld.setAttribute("RUNNING", loads[i].running ? "TRUE" : "FALSE");
                    ld.setAttribute("START_DATE", this.mDefaultDateFormat.format(loads[i].start_date));
                    ld.setAttribute("END_DATE", loads[i].end_date == null ? "" : this.mDefaultDateFormat
                            .format(loads[i].end_date));
                }
            }

            return XMLHelper.outputXML(e, true);
        } catch (Exception e1) {
            return this.handleError(e1);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getLoadJobs(int, int, java.util.Date)
     */
    public String getLoadJobs(String pServerID, int pLoadID, Date pLastModified) {
        Element root = getNewDocument();

        try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);

                ETLJob[] jobs = md.getLoadJobs(pLastModified, pLoadID);

                getJobsAsXML(root, jobs, true);
            }
            return XMLHelper.outputXML(root, true);
        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }

    /**
     * @param root
     * @param jobs
     */
    private void getJobsAsXML(Element root, ETLJob[] jobs, boolean pGetStatus) {
        for (int x = 0; x < jobs.length; x++) {
            ETLJob j = jobs[x];
            Element e = addChildElement(root, "JOB");
            e.setAttribute("ID", j.getJobID());
            e.setAttribute("NAME", j.getName());
            e.setAttribute("PROJECT", j.getProjectName());
            e.setAttribute("TYPE", j.getJobTypeName());
            if (pGetStatus) {
                Element status = addChildElement(e, "STATUS");
                status.setAttribute("START_DATE", this.mDefaultDateFormat.format(j.getStatus().getStartDate()));
                status.setAttribute("END_DATE", j.getStatus().getEndDate() == null ? "" : this.mDefaultDateFormat
                        .format(j.getStatus().getEndDate()));
                status.setAttribute("EXECUTION_DATE", j.getStatus().getExecutionDate() == null ? ""
                        : this.mDefaultDateFormat.format(j.getStatus().getExecutionDate()));
                status.setAttribute("EXECUTION_ID",	String.valueOf(j.getJobExecutionID()));
                status.setTextContent(j.getStatus().getStatusMessage());
                status.setAttribute("STATUS_ID", Integer.toString(j.getStatus().getStatusCode()));
            }
            e.setAttribute("SECONDS_BEFORE_RETRY", Integer.toString(j.getSecondsBeforeRetry()));
            e.setAttribute("RETRY_ATTEMPTS", Integer.toString(j.getMaxRetries()));

            e.setAttribute("DESCRIPTION", j.getDescription());

            if (j.isAlertingDisabled()) {
                e.setAttribute("DISABLE_ALERTING", "Y");
            } else {
            	e.setAttribute("DISABLE_ALERTING", "N");
            }  

            for (int i = 0; i < j.dependencies.length; i++) {
                Element deps;

                if (j.dependencies[i][1] == Metadata.DEPENDS_ON) {
                    deps = addChildElement(e, "DEPENDS_ON");
                }
                else {
                    deps = addChildElement(e, "WAITS_ON");
                }

                deps.setTextContent(j.dependencies[i][0]);
            }

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getProjectJobs(int, int, java.util.Date)
     */
    public String getProjectJobs(String pServerID, int pProjectID, Date pLastModified) {
        Element root = getNewDocument();

        try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);

                ETLJob[] jobs = md.getProjectJobs(pLastModified, pProjectID);

                getJobsAsXML(root, jobs, false);
            }
            return XMLHelper.outputXML(root, true);
        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }

    /**
     * 
     */
    public XMLMetadataBridge() {
        super();
        // TODO Auto-generated constructor stub
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getProjects(int, java.util.Date)
     */
    public String getProjects(String pServerID, Date pLastModified) {
        try {
            synchronized (mLock) {
                Element root = getNewDocument();

                Metadata md = this.getMetadataByServer(pServerID);

                Element e = addChildElement(root, "PROJECTS");
                Object[] result = md.getProjects();

                for (int i = 0; i < result.length; i++) {
                    Element p = addChildElement(e, "PROJECT");
                    Object[] tmp = (Object[]) result[i];
                    p.setAttribute("ID", ((Integer) tmp[0]).toString());
                    p.setAttribute("NAME", (String) tmp[1]);
                }

                return XMLHelper.outputXML(root, true);
            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLBridge#getJob(int, int, java.lang.String)
     */
    public String getJob(String pServerID, int pProjectID, String pJobID) {

        try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);

                ETLJob j = md.getJob(pJobID);
                if (j == null)
                    return null;

                Element e = getNewDocument();

                j.getXMLJobDefinition(e);

                return XMLHelper.outputXML(e, true);
            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }

    }

    public static void configure(String pKETLPath, String pKETLConfigFile) throws Exception {
        
        if (pKETLConfigFile == null)
            throw new Exception("Connection to KETL metadata not possible, as KETL path is null");
        if (pKETLConfigFile == null)
            throw new Exception("Connection to KETL metadata not possible, as configuration file is null");

        System.out.println("Using the following KETL config file: " + pKETLPath + File.separator + pKETLConfigFile);

        xmlConfig = Metadata.LoadConfigFile(pKETLPath, pKETLPath + File.separator + pKETLConfigFile);

        mDocBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLMetadataCalls#getJobStatus(java.lang.String, int)
     */
    public String getJobStatus(String pServerID, int pJobExecutionID) {
        try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);

                /*
                 * ETLJob j = md.getJob(pJobID); Element e = getNewDocument(); j.getXMLJobDefinition(e); return
                 * XMLHelper.outputXML(e,true);
                 */
                return "";
            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.XMLMetadataCalls#setJobStatus(java.lang.String, java.lang.String, java.lang.String)
     */
    public String setJobStatus(String pServerID, String pProjectID, String pJobID, int pLoadID, int pJobExecutionID, String pState) {
        try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);

                ETLJob j = md.getJob(pJobID, pLoadID, pJobExecutionID);

                if (j == null)
                    return "Job could not be found";

                if (j.getStatus() == null)
                    return "Job is not current in the job queue";

                int status_id = -1;
                if (pState.equalsIgnoreCase("PAUSE"))
                    status_id = ETLJobStatus.PAUSED;
                else if (pState.equalsIgnoreCase("RESUME"))
                    status_id = ETLJobStatus.QUEUED_FOR_EXECUTION;
                else if (pState.equalsIgnoreCase("SKIP"))
                    status_id = ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL;
                else if (pState.equalsIgnoreCase("CANCEL"))
                    status_id = ETLJobStatus.PENDING_CLOSURE_CANCELLED;
                else if (pState.equalsIgnoreCase("RESTART"))
                    status_id = ETLJobStatus.READY_TO_RUN;

                if (status_id == -1)
                    return "Invalid status";

                if (j.getStatus() == null)
                    return "Job is not current in the job queue";

                j.getStatus().setStatusCode(status_id);
                md.setJobStatus(j);
                return "Status changed";

            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }


    /**
     * Updates the metdata associated with pServerID, pProjectID
     * with the new pJobXML. The existing pJobXML is compared against
     * the supplied if the modification date of the destination is greater
     * than the one supplied then an exception is raised
     * Returns an XML document as string confirming change.
     *  <ETL><JOB ID="?" PROJECT="?" SUCCESS="TRUE|FALSE"/></ETL>
     */
    public String updateJob(String pServerID, String pProjectID, String pJobXML, boolean pForceOverwrite) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Updates the KETLServers.xml file on the application server to include
     * another server entry the XML in the file looks like
     * <SERVERS DEFAULTSERVER="test">
     *     <SERVER NAME="test">
     *         <USERNAME>ketlmd</USERNAME>
     *         <PASSWORD>ketlmd</PASSWORD>
     *         <NETWORKNAME>localhost</NETWORKNAME>
     *         <DRIVER>org.postgresql.Driver</DRIVER>
     *         <PRESQL>set search_path = 'ketlmd'</PRESQL>
     *         <URL>jdbc:postgresql://localhost/postgres?prepareThreshold=1</URL>
     *         <MDPREFIX></MDPREFIX>
     *         <PASSPHRASE>ZATXO+7vBD7k9uicS/JOlBtsuscFIA8bpWBHZcHYNrc=</PASSPHRASE>       
     *     </SERVER>
     * </SERVERS>
     * 
     * Return true for success else false
     */
    public boolean addServer(String pUsername, String pPassword, String pJDBCDriver, String pURL, String pMDPrefix, String pPassphrase) {
        // TODO Auto-generated method stub
        return false;
    }


    /**
     * Removes the specified server id from the KETLServers.xml file
     * on the applications server
     */
    public boolean removeServer(String pServerID) {
        // TODO Auto-generated method stub
        return false;
    }


    /**
     * Call the metadata library to add an entry to the schedule table
     * return the new schedule id
     */
    public String scheduleJob(String pServerID, int pProjectID, String pJobID, 
    		int pMonth, int pMonthOfYear, int pDay, int pDayOfWeek, int pDayOfMonth, 
    		int pHour, int pHourOfDay, int pMinute, int pMinuteOfHour, 
    		String pDescription, Date pOnceOnlyDate, Date pEnableDate, Date pDisableDate) {
        //NOTE: pProjectID is currently not used.
    	try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);
                int sched_id = md.scheduleJob(pJobID, pMonth, pMonthOfYear, 
                		pDay, pDayOfWeek, pDayOfMonth, pHour, pHourOfDay, pMinute, pMinuteOfHour, 
                		pDescription, pOnceOnlyDate, pEnableDate, pDisableDate);
                return Integer.toString(sched_id);
            }

        } catch (Exception e1) {
            this.handleError(e1);
            return "-1";
        }
    }

    /**
     * Delete the specified load from the current job_error_hist, job_log_hist and load table
     * return true for success else false
     */
    public boolean deleteLoad(String pServerID, String pLoadID) {
        // TODO Auto-generated method stub
        return false;
    }


    /**
     * Create a project lock in the metadata with a timeout according to the value
     * in the system.xml for the object specified
     * return an alphanumeric string as the lockid or -1 if lock not available
     */
    public int getLock(String pServerID, String pProjectID, boolean pForceOverwrite) {
        // TODO Auto-generated method stub
        return -1;
    }

    /**
     * Release a project lock in the metadata
     */
    public void releaseLock(String pServerID, int pLockID) {
        return;
    }
    
    /**
     * Refresh a project lock to extend the lock timeout
     */
    public boolean refreshLock(String pServerID, int pLockID) {
        return false;
    }

    /**
     * Get a summary of load status changes for jobs after the last refresh date
     *  <ETL><JOB ID="?" STATUS="">
     *       <JOB ID="?" STATUS="">
     *  </ETL>
     */
    public String refreshLoadStatus(String pServerID, int pLoadID, Date pLastRefreshDate) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Get a summary of new or modified jobs after the last refresh date
     *  <ETL><JOB ID="?" STATUS="">
     *       <JOB ID="?" STATUS="">
     *  </ETL>
     */
    public String refreshProjectStatus(String pServerID, String pProjectID, Date pLastRefreshDate) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Execute a job immediately
     *   Returns "success" for "failure" 
     */
    public String executeJob(String pServerID, int pProjectID, String pJobID, boolean pIgnoreDependencies, boolean pAllowMultiple)
    {
        try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);
                boolean isSuccessful = md.executeJob(pProjectID, pJobID, pIgnoreDependencies, pAllowMultiple);
                if (isSuccessful)
                	return "success";
                else
                	return "failure";
            }
        } catch (Exception e1) {
            this.handleError(e1);
            return "failure";
        }
    }
    
    /**
     * Get job errors if any as XML from the metadata
     *  <ETL><JOB ID="?" PROJECT="?" STATUS=">
     *       <ERRORS>
     *          <ERROR TIMESTAMP="" CODE="">Message</ERROR>
     *       </ERRORS>
     *       </JOB>
     *  </ETL>
     */
    private static int maxRows = 500;
    // TODO: implement paging
    public String getLoadErrors(String pServerID, int pLoadID, Date pLastModified) {
    	Element root = getNewDocument();
    	try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);
                
                // get all jobs within this load
                ETLJob[] jobs = md.getLoadJobs(pLastModified, pLoadID);
                for (int x = 0; x < jobs.length; x++) {
                    ETLJob job = jobs[x];                    
                    Element e1 = addXMLJobExecutionNode(root, job);
                    int execID = job.getJobExecutionID();

                    // for each job, get all errors via the execution id
                    ETLJobError[] errors = md.getExecutionErrors(pLastModified, execID, maxRows);
                    for (int y = 0; y < errors.length; y++) {
                        ETLJobError err = errors[y];
                        addXMLErrorNode(e1, err);
                    }
                }
                return XMLHelper.outputXML(root,true);
            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }
    
    public String getJobErrors(String pServerID, String pJobName, Date pLastModified) {
    	Element root = getNewDocument();
    	try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);
                
                // get all loads that contain this job
                ETLLoad[] loads = md.getJobLoads(pLastModified, pJobName);
                for (int w = 0; w < loads.length; w++) {
                	ETLLoad load = loads[w];
                	Element e0 = addXMLLoadNode(root, load);                	
                
	                // get the job execution details (single job execution)
	                ETLJob[] jobs = md.getExecutionDetails(pLastModified, load.LoadID, load.jobExecutionID);
	                for (int x = 0; x < jobs.length; x++) {
	                    ETLJob job = jobs[x];                    
	                    Element e1 = addXMLJobExecutionNode(e0, job);
	                    int execID = job.getJobExecutionID();
	
	                    // for each job, get all errors via the execution id
	                    ETLJobError[] errors = md.getExecutionErrors(pLastModified, execID, maxRows);
	                    for (int y = 0; y < errors.length; y++) {
	                        ETLJobError err = errors[y];
	                        addXMLErrorNode(e1, err);
	                    }
	                }
                }
                return XMLHelper.outputXML(root,true);
            }

        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }
    public String getExecutionErrors(String pServerID, int pLoadID, int pExecID, Date pLastModified) {
    	Element root = getNewDocument();
    	try {
            synchronized (mLock) {
                Metadata md = this.getMetadataByServer(pServerID);
                
                // get the job execution details (single job execution)
                ETLJob[] jobs = md.getExecutionDetails(pLastModified, pLoadID, pExecID);
                for (int x = 0; x < jobs.length; x++) {
                    ETLJob job = jobs[x];                    
                    Element e1 = addXMLJobExecutionNode(root, job);
                    int execID = job.getJobExecutionID();

                    // for each job, get all errors via the execution id
                    ETLJobError[] errors = md.getExecutionErrors(pLastModified, execID, maxRows);
                    for (int y = 0; y < errors.length; y++) {
                        ETLJobError err = errors[y];
                        addXMLErrorNode(e1, err);
                    }
                }
                return XMLHelper.outputXML(root,true);
            }
        } catch (Exception e1) {
            return this.handleError(e1);
        }
    }
    
    private Element addXMLLoadNode(Element parent, ETLLoad load) {
        Element ld = addChildElement(parent, "LOAD");
        ld.setAttribute("START_JOB_ID", load.start_job_id);
        ld.setAttribute("LOAD_ID", Integer.toString(load.LoadID));
        ld.setAttribute("FAILED", load.failed ? "TRUE" : "FALSE");
        ld.setAttribute("IGNORED_PARENTS", load.ignored_parents ? "TRUE" : "FALSE");
        ld.setAttribute("PROJECT_ID", Integer.toString(load.project_id));
        ld.setAttribute("RUNNING", load.running ? "TRUE" : "FALSE");
        ld.setAttribute("START_DATE", load.start_date == null ? ""
        		: this.mDefaultDateFormat.format(load.start_date));
        ld.setAttribute("END_DATE", load.end_date == null ? "" 
        		: this.mDefaultDateFormat.format(load.end_date));
        return ld;
    }
    private Element addXMLJobExecutionNode(Element parent, ETLJob j) {
    	Element e = addChildElement(parent, "JOB");
        e.setAttribute("ID", j.getJobID());
        e.setAttribute("NAME", j.getName());
        e.setAttribute("TYPE", j.getJobTypeName());
        e.setAttribute("EXECUTION_ID", String.valueOf(j.getJobExecutionID()));
        e.setAttribute("EXECUTION_DATE", j.getStatus().getExecutionDate() == null ? ""
                : this.mDefaultDateFormat.format(j.getStatus().getExecutionDate()));
        e.setAttribute("END_DATE", j.getStatus().getEndDate() == null ? "" 
        		: this.mDefaultDateFormat.format(j.getStatus().getEndDate()));
        e.setAttribute("STATUS_ID", Integer.toString(j.getStatus().getStatusCode()));
        e.setAttribute("STATUS_TEXT", j.jsStatus.getStatusMessage().toString());
        e.setAttribute("SERVER_ID", String.valueOf(j.jsStatus.getServerID()));
        return e;
    }
    private Element addXMLErrorNode(Element parent, ETLJobError err) {
    	Element e = addChildElement(parent, "ERROR");
    	e.setAttribute("DATETIME", err.getDate() == null ? ""
        		: this.mDefaultDateFormat.format(err.getDate()));
    	e.setAttribute("CODE", err.getCode());
    	e.setAttribute("MESSAGE", err.getMessage());
    	e.setAttribute("DETAILS", err.getDetails());
    	e.setAttribute("STEP_NAME", err.getStepName());
    	return e;
    }
}
