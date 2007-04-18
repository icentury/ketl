/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;

/**
 * Insert the type's description here. Creation date: (5/3/2002 1:41:54 PM)
 * 
 * @author: Administrator
 */
public class ETLJob {

    transient protected ETLJobStatus jsStatus;
    protected boolean bCancelJob = false;
    protected int iLoadID;
    protected String sJobID;
    protected int iJobExecutionID;
    protected java.lang.String strAction;
    transient protected Object moAction;
    HashMap mTagLevelParameterListCache = null;
    protected java.util.Date mdCreationDate = new java.util.Date();
    private int SecondsBeforeRetry = 0;
    private String msGlobalParameterListName = null;
    private int miGlobalParameterListID = -1;
    private int RetryAttempts = 0;
    private int MaxRetries = 0;
    private int JobTypeID = -1;
    private int ProjectID = -1;
    private String JobTypeName;
    private String Name = "";
    private String Description = "";
    String jobDefinition = null;
    boolean DisableAlerting = false;

    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
        s.writeObject(moAction.toString());
    }

    private void readObject(ObjectInputStream s) throws IOException {
        try {
            s.defaultReadObject();
            moAction = s.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void setJobTypeName(String pName) {
        this.JobTypeName = pName;
    }

    public String getJobTypeName() {
        return (this.JobTypeName);
    }

    public String getProjectName() {
        try {
            return ResourcePool.getMetadata().getProjectName(this.ProjectID);
        } catch (Exception e) {
            return "";
        }
    }

    public ArrayList getParameterLists(String pParameterListName) {
        return (ArrayList) this.getParameterListCache().get(pParameterListName);
    }

    /**
     * @return Returns the parameterListCache.
     */
    public final HashMap getParameterListCache() {
        return mTagLevelParameterListCache;
    }

    /**
     * @param pParameterListCache The parameterListCache to set.
     */
    public final void setParameterListCache(HashMap pParameterListCache) {
        mTagLevelParameterListCache = pParameterListCache;
    }

    /**
     * @return Returns the description.
     */
    public String getDescription() {
        return Description;
    }

    /**
     * @param description The description to set.
     */
    public void setDescription(String description) {
        Description = description;
    }

    public String getXMLJobDefinition(Element rootNode) {
        try {
            Element e = getJobAsXMLElement(rootNode);

            return XMLHelper.outputXML(e);
        } catch (Exception e) {
            ResourcePool.LogException(e, this);
        }

        return null;
    }

    public String getXMLJobDefinition() {
        try {
            Element e = getJobAsXMLElement(null);

            return XMLHelper.outputXML(e);
        } catch (Exception e) {
            ResourcePool.LogException(e, this);
        }

        return null;
    }

    /**
     * @return
     * @throws ParserConfigurationException
     * @throws SQLException
     * @throws Exception
     */
    public Element getJobAsXMLElement(Node storeRootNode) throws ParserConfigurationException, SQLException, Exception {
        Document documentRoot;
        Element e = null;
        if (storeRootNode == null) {
            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dmf.newDocumentBuilder();
            documentRoot = builder.newDocument();
            e = documentRoot.createElement("JOB");
            documentRoot.appendChild(e);
        }
        else {
            // get owner document
            if (storeRootNode.getOwnerDocument() == null)
                documentRoot = (Document) storeRootNode;
            else {
                documentRoot = storeRootNode.getOwnerDocument();
            }
            // create child node
            e = documentRoot.createElement("JOB");
            // append child node to correct position
            storeRootNode.appendChild(e);
        }

        e.appendChild(documentRoot.createTextNode("\n"));
        e.setAttribute("ID", this.getJobID());
        e.setAttribute("NAME", this.getName());
        e.setAttribute("PROJECT", this.getProjectName());
        e.setAttribute("TYPE", this.getJobTypeName());

        String parameterListName = ResourcePool.getMetadata().getParameterListName(this.getGlobalParameterListID());

        if (parameterListName != null) {
            e.setAttribute("PARAMETER_LIST", parameterListName);
        }

        e.setAttribute("SECONDS_BEFORE_RETRY", Integer.toString(this.getSecondsBeforeRetry()));
        e.setAttribute("RETRY_ATTEMPTS", Integer.toString(this.getMaxRetries()));
        e.setAttribute("DESCRIPTION", this.getDescription());

        if (this.isAlertingDisabled()) {
            e.setAttribute("DISABLE_ALERTING", "Y");
        }
        else
            e.setAttribute("DISABLE_ALERTING", "N");

        if (dependencies == null) {
            dependencies = ResourcePool.getMetadata().getJobDependencies(this.getJobID());
        }

        for (int i = 0; i < dependencies.length; i++) {
            Element deps;

            if (dependencies[i][1] == Metadata.DEPENDS_ON) {
                deps = documentRoot.createElement("DEPENDS_ON");
            }
            else {
                deps = documentRoot.createElement("WAITS_ON");
            }

            deps.appendChild(documentRoot.createTextNode(dependencies[i][0]));
            e.appendChild(deps);
            e.appendChild(documentRoot.createTextNode("\n"));
        }

        setChildNodes(e);
        e.appendChild(documentRoot.createTextNode("\n"));
        return e;
    }

    protected Node setChildNodes(Node pParentNode) {
        Element e = pParentNode.getOwnerDocument().createElement("EMPTY");

        // XMLHelper.outputXML(this.)
        Object action = this.getAction();

        if (action != null) {
            e.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction().toString()));
        }

        pParentNode.appendChild(e);

        return e;
    }

    String[][] dependencies = null;

    public String getDepedencies() throws Exception {
        String strWaitsOn = null;
        String strDependsOn = null;

        if (dependencies == null) {
            dependencies = ResourcePool.getMetadata().getJobDependencies(this.getJobID());
        }

        StringBuffer sb = new StringBuffer();

        for (int x = 0; x < dependencies.length; x++) {
            if (dependencies[x][1] == Metadata.DEPENDS_ON) {
                if (strDependsOn == null) {
                    strDependsOn = "DEPENDS_ON (Critical dependencies)\n\t" + dependencies[x][0];
                }
                else {
                    strDependsOn = strDependsOn + "\n\t" + dependencies[x][0];
                }
            }
            else {
                if (strWaitsOn == null) {
                    strWaitsOn = "WAITS_ON (Non-critical dependencies)\n\t" + dependencies[x][0];
                }
                else {
                    strWaitsOn = strWaitsOn + "\n\t" + dependencies[x][0];
                }
            }
        }

        if (strDependsOn != null) {
            sb.append(strDependsOn + "\n");
        }

        if (strWaitsOn != null) {
            sb.append(strWaitsOn + "\n");
        }

        return sb.toString();
    }

    public String getJobDefinition() throws Exception {
        if (jobDefinition == null) {
            if (dependencies == null) {
                dependencies = ResourcePool.getMetadata().getJobDependencies(this.getJobID());
            }

            String strWaitsOn = null;
            String strDependsOn = null;

            StringBuffer sb = new StringBuffer();

            for (int x = 0; x < dependencies.length; x++) {
                if (dependencies[x][1] == Metadata.DEPENDS_ON) {
                    if (strDependsOn == null) {
                        strDependsOn = "--   DEPENDS_ON = " + dependencies[x][0];
                    }
                    else {
                        strDependsOn = strDependsOn + ", " + dependencies[x][0];
                    }
                }
                else {
                    if (strWaitsOn == null) {
                        strWaitsOn = "--   WAITS_ON = " + dependencies[x][0];
                    }
                    else {
                        strWaitsOn = strWaitsOn + ", " + dependencies[x][0];
                    }
                }
            }

            sb.append("-- BEGIN_SQL_JOB(" + this.getJobID() + ")\n");
            sb.append("-- {\n");
            sb.append("--   JOB_TYPE_ID = " + this.getJobTypeID() + ";\n");
            sb.append("--   PARAMETER_LIST_ID = " + this.getGlobalParameterListID() + ";\n");
            sb.append("--   PROJECT_ID = " + this.getProjectID() + ";\n");
            sb.append("--   NAME = '" + this.getName() + "';\n");
            sb.append("--   DESCRIPTION = '" + this.getDescription() + "';\n");
            sb.append("--   RETRY_ATTEMPTS = " + this.getMaxRetries() + ";\n");
            sb.append("--   SECONDS_BEFORE_RETRY = " + this.getSecondsBeforeRetry() + ";\n");

            if (strDependsOn != null) {
                sb.append(strDependsOn + ";\n");
            }

            if (strWaitsOn != null) {
                sb.append(strWaitsOn + ";\n");
            }

            sb.append("-- }\n");
            sb.append(this.getAction() + "\n");
            sb.append("-- END_SQL_JOB\n");
            jobDefinition = sb.toString();
        }

        return (jobDefinition);
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return Name;
    }

    /**
     * @param name The name to set.
     */
    public void setName(String name) {
        Name = name;
    }

    /**
     * ETLJob constructor comment.
     * 
     * @throws Exception TODO
     */
    public ETLJob() throws Exception {
        super();

        if (jsStatus == null) {
            jsStatus = new ETLJobStatus();
        }
    }

    public int getJobTypeID() {
        return this.JobTypeID;
    }

    public void setJobTypeID(int pJobTypeID) {
        this.JobTypeID = pJobTypeID;
    }

    public ETLJob(ETLJobStatus pjsStatus) throws Exception {
        this();
        jsStatus = pjsStatus;
    }

    /**
     * Simplified version of setCancelJob() - used strictly for cancelling jobs. Creation date: (5/3/2002 5:36:11 PM)
     * 
     * @return boolean
     */
    public boolean cancelJob() {
        return setCancelJob(true);
    }

    /**
     * To be used by jobs that have connections to close, etc. Creation date: (5/9/2002 2:27:36 PM)
     */
    public void cleanup() {
        closeLog();
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 3:32:26 PM)
     * 
     * @return java.lang.String
     */
    public Object getAction() {
        if (moAction instanceof String) {
            String strAction = (String) moAction;

            if (strAction != null) {
                strAction = getInternalConstants(strAction);
            }

            return strAction;
        }

        return moAction;
    }

    public String getInternalConstants(String strAction) {
        // replace any load id variables with real load id's
        for (int i = 0; i < EngineConstants.PARAMETER_LOAD_ID.length; i++) {
            strAction = EngineConstants.replaceParameter(strAction, EngineConstants.PARAMETER_LOAD_ID[i], new Integer(
                    this.getLoadID()).toString());
        }

        // replace any date variables with real load id's
        java.text.SimpleDateFormat dmf = new java.text.SimpleDateFormat();

        for (int i = 0; i < EngineConstants.PARAMETER_DATE.length; i++) {
            String[] params = EngineConstants.getParameterParameters(strAction, EngineConstants.PARAMETER_DATE[i]);

            if ((params == null) || (params.length == 0)) {
                dmf.applyPattern(EngineConstants.PARAMETER_DATE_FORMAT[i]);
                strAction = EngineConstants.replaceParameter(strAction, EngineConstants.PARAMETER_DATE[i], dmf
                        .format(this.getCreationDate()));
            }
            else {
                String[] values = new String[params.length];

                for (int x = 0; x < params.length; x++) {
                    dmf.applyPattern(params[x]);
                    values[x] = dmf.format(this.getCreationDate());
                }

                dmf.applyPattern(EngineConstants.PARAMETER_DATE_FORMAT[i]);
                strAction = EngineConstants.replaceParameter(strAction, EngineConstants.PARAMETER_DATE[i], params,
                        values, this.getCreationDate().toString());
            }
        }

        // replace any job execution id variables with real job execution id's
        for (int i = 0; i < EngineConstants.PARAMETER_JOB_EXECUTION_ID.length; i++) {
            strAction = EngineConstants.replaceParameter(strAction, EngineConstants.PARAMETER_JOB_EXECUTION_ID[i],
                    new Integer(this.getJobExecutionID()).toString());
        }
        return strAction;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:43 AM)
     * 
     * @return int
     */
    public int getJobExecutionID() {
        return iJobExecutionID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:31 AM)
     * 
     * @return int
     */
    public String getJobID() {
        return sJobID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:17 AM)
     * 
     * @return int
     */
    public int getLoadID() {
        return iLoadID;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:12:54 PM)
     * 
     * @return int
     */
    public int getMaxRetries() {
        return MaxRetries;
    }

    /**
     * This method returns the top level parameters in a given list by name. NOTE: This will NOT return any lower level
     * parameters. To obtain these, you must retrieve the sub parameter list. Creation date: (5/8/2002 4:31:46 PM)
     * 
     * @return java.lang.Object
     * @param oKey java.lang.Object
     */
    public Object getGlobalParameter(Object oName) {
        return this.getParameterValue(this.msGlobalParameterListName, (String) oName, null);
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:08:13 PM)
     * 
     * @return int
     */
    public int getRetryAttempts() {
        return RetryAttempts;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:07:52 PM)
     * 
     * @return int
     */
    public int getSecondsBeforeRetry() {
        return SecondsBeforeRetry;
    }

    /**
     * NOTE: By returning the status object, we allow the caller to change the status without the job's knowing. Option:
     * to merge the ETLJobStatus object with ETLJob, and implement an IStatus interface. Problem is, the interface
     * mechanism won't allow us to have the message bound checking and synchronization behavior built in. Creation date:
     * (5/3/2002 2:08:59 PM)
     * 
     * @return com.kni.etl.ETLJobStatus
     */
    public ETLJobStatus getStatus() {
        return jsStatus;
    }

    
    private boolean bCancelSuccessfull = false;
    
    public void cancelSuccessfull(boolean arg0){
        bCancelSuccessfull = arg0;
    }
    
    /**
     * Insert the method's description here. Creation date: (5/3/2002 5:28:54 PM)
     * 
     * @return boolean
     */
    public boolean isCancelSuccessfull() {
        return bCancelSuccessfull;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 5:28:54 PM)
     * 
     * @return boolean
     */
    public synchronized boolean isCancelled() {
        return bCancelJob;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 2:55:31 PM)
     * 
     * @return boolean
     */
    public synchronized boolean isCompleted() {
        switch (jsStatus.getStatusCode()) {
        case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
        case ETLJobStatus.PENDING_CLOSURE_FAILED:
        case ETLJobStatus.PENDING_CLOSURE_CANCELLED:
            return true;
        }

        return false;
    }

    public synchronized boolean isSuccessful() {
        switch (jsStatus.getStatusCode()) {
        case ETLJobStatus.SUCCESSFUL:
        case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
            return true;
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 3:32:26 PM)
     * 
     * @param newAction java.lang.String
     * @throws Exception TODO
     */
    public void setAction(Object oAction) throws Exception {
        moAction = oAction;
    }

    /**
     * Sets cancel flag for job if possible. NOTE: this does not guarantee that the job will be cancelled, or that a
     * previous cancellation request will not be removed. It all depends on whether the worker thread will see this flag
     * before completion. Returns: true if successful in changing the flag, false otherwise (if already set as
     * requested, or if in invalid status)
     */
    public boolean setCancelJob(boolean bCancel) {
        // Cannot change the cancel flag if we're already completed...
        if (isCompleted() == true) {
            return false;
        }

        // Only set the flag if it's a new value...
        if (bCancel != bCancelJob) {
            bCancelJob = bCancel;

            return true;
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:43 AM)
     * 
     * @param newJobExecutionID int
     */
    public void setJobExecutionID(int newJobExecutionID) {
        iJobExecutionID = newJobExecutionID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:31 AM)
     * 
     * @param newJobID int
     */
    public void setJobID(String newJobID) {
        sJobID = newJobID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:17 AM)
     * 
     * @param newLoadID int
     */
    public void setLoadID(int newLoadID) {
        iLoadID = newLoadID;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:12:54 PM)
     * 
     * @param newMaxRetries int
     */
    public void setMaxRetries(int newMaxRetries) {
        MaxRetries = newMaxRetries;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 4:33:44 PM)
     * 
     * @param oName java.lang.Object
     * @param oValue java.lang.Object
     */
    public void setGlobalParameter(Object oName, Object oValue) {

        ArrayList ar = (ArrayList) this.mTagLevelParameterListCache.get(this.msGlobalParameterListName);
        for (Object o : ar) {
            ((ParameterList) o).setParameter(oName, oValue);
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:08:13 PM)
     * 
     * @param newRetryAttempts int
     */
    public void setRetryAttempts(int newRetryAttempts) {
        RetryAttempts = newRetryAttempts;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:07:52 PM)
     * 
     * @param newSecondsBeforeRetry int
     */
    public void setSecondsBeforeRetry(int newSecondsBeforeRetry) {
        if (newSecondsBeforeRetry < 0) {
            this.SecondsBeforeRetry = 0;
        }
        else {
            SecondsBeforeRetry = newSecondsBeforeRetry;
        }
    }

    /**
     * Returns a String that represents the value of this object.
     * 
     * @return a string representation of the receiver
     */
    public String toString() {
        // Insert code to print the receiver here.
        // This implementation forwards the message to super. You may replace or supplement this.
        return super.toString();
    }

    /**
     * @return
     */
    public java.util.Date getCreationDate() {
        return mdCreationDate;
    }

    /**
     * @return Returns the projectID.
     */
    public int getProjectID() {
        return ProjectID;
    }

    /**
     * @param projectID The projectID to set.
     */
    public void setProjectID(int projectID) {
        ProjectID = projectID;
    }

    /**
     * @return Returns the parameterListID.
     */
    public int getGlobalParameterListID() {
        return miGlobalParameterListID;
    }

    /**
     * @param parameterListID The parameterListID to set.
     */
    public void setGlobalParameterListID(int parameterListID) {
        this.miGlobalParameterListID = parameterListID;
        this.msGlobalParameterListName = ResourcePool.getMetadata().getParameterListName(parameterListID);
        this.setGlobalParameterListName(null, this.msGlobalParameterListName);
    }

    public void setGlobalParameterListName(Node node, String parameterList) {

        if (parameterList == null)
            return;

        this.msGlobalParameterListName = parameterList;

        ArrayList ar;
        if (node != null) {
            ar = ParameterList.recurseParameterList(node, parameterList);
        }
        else {
            ar = ParameterList.recurseParameterList(parameterList);

        }

        if (ar != null) {
            if (this.mTagLevelParameterListCache == null)
                this.mTagLevelParameterListCache = new HashMap();
            this.mTagLevelParameterListCache.put(parameterList, ar);
        }

        if (ResourcePool.getMetadata() != null) {
            this.miGlobalParameterListID = ResourcePool.getMetadata()
                    .getParameterListID(this.msGlobalParameterListName);
        }
    }

    /**
     * @return Returns the disableAlerting.
     */
    public boolean isAlertingDisabled() {
        return DisableAlerting;
    }

    /**
     * @param disableAlerting The disableAlerting to set.
     */
    public void setDisableAlerting(boolean disableAlerting) {
        DisableAlerting = disableAlerting;
    }

    public String getParameterValue(String parameterListName, String parameterName, String defaultValue) {
        ArrayList res = (ArrayList) this.mTagLevelParameterListCache.get(parameterListName);

        ArrayList values = new ArrayList();
        ArrayList paths = new ArrayList();
        if (res == null)
            return defaultValue;

        for (Object o : res) {
            ParameterList pl = (ParameterList) o;
            String val = (String) pl.getParameter(parameterName);

            if (val != null && values.contains(val) == false) {
                values.add(val);
            }

            if (val != null)
                paths.add(pl.path());

        }

        if (values.size() == 0)
            return defaultValue;

        if (values.size() > 1)
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Multiple values found for the parameter "
                    + parameterName + " in parameter list " + parameterListName
                    + ", first value will be used. Parameter list path(s): " + paths.toString());

        return (String) values.get(0);

    }

    private HashMap mCounters = new HashMap();
    private HashMap mLogs = new HashMap();
    private boolean mLoggerFailed = false;
    private OutputStream moDump;
    private String mDumpFile;
    private BufferedOutputStream moDumpBuffer;
    private PrintWriter mDumpWriter;

    final public synchronized SharedCounter getErrorCounter(String name2) {
        return this.getCounter("{${ERR}" + name2);
    }

    final public synchronized SharedCounter getCounter(String pName, Class pClass) {

        SharedCounter res = (SharedCounter) this.mCounters.get(pName);

        if (res == null) {
            if (pClass == null || pClass == Integer.class || pClass == int.class)
                res = new SharedCounter();
            else {
                throw new RuntimeException("Invalid class requested for sequence generator "
                        + pClass.getCanonicalName() + " please use Integer only");
            }

            res = new SharedCounter();
            this.mCounters.put(pName, res);
        }
        return res;

    }

    final public SharedCounter getCounter(String name2) {
        return this.getCounter(name2, Integer.class);
    }

    public void writeLog() {
        if (this.mLoggerFailed == false && this.moDump != null) {
            try {
                this.mDumpWriter.flush();
                this.mDumpWriter.close();
                this.moDumpBuffer.close();
                this.moDump.close();
                this.moDump = null;
            } catch (IOException e) {
                this.mLoggerFailed = true;
                System.err.println("Job logging failed: " + e.toString());
                e.printStackTrace();
            }
        }
    }

    private void flushLog() throws IOException {
        if (moDump != null) {
            this.mDumpWriter.flush();
            this.moDumpBuffer.flush();
            this.moDump.flush();
        }
    }

    private void closeLog() {
        if (this.mLoggerFailed == false && this.moDump != null) {
            try {
                if (moDump != null) {
                    this.mDumpWriter.flush();
                    this.mDumpWriter.close();
                    this.moDumpBuffer.close();
                    this.moDump.close();
                    this.moDump = null;
                }
                if (this.isSuccessful()) {
                    File fDel = new File(this.mDumpFile);
                    fDel.delete();
                }
                else
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Job details logged to "
                            + this.mDumpFile);
            } catch (IOException e) {
                this.mLoggerFailed = true;
                System.err.println("Job logging failed: " + e.toString());
                e.printStackTrace();
            }
        }
    }

    public String getDumpFile() throws IOException {
        this.flushLog();
        return this.mDumpFile;
    }

    public void logJobMessage(Object obj) {
        if (this.mLoggerFailed)
            return;
        try {
            if (moDump == null) {
                mDumpFile = this.getLoggingPath() + File.separator + this.getJobID() + "."
                        + this.getJobExecutionID() + ".joblog";
                moDump = new FileOutputStream(mDumpFile);
                moDumpBuffer = new BufferedOutputStream(moDump);
                mDumpWriter = new PrintWriter(moDumpBuffer);
            }

            if (obj instanceof Error)
                ((Error) obj).printStackTrace(mDumpWriter);
            else
                mDumpWriter.println(obj.toString());

        } catch (IOException e) {
            this.mLoggerFailed = true;
            System.err.println("Job logging failed: " + e.toString());
            e.printStackTrace();
        }

    }

    public String getLoggingPath() {
        String rootPath = EngineConstants.BAD_RECORD_PATH;

        rootPath = this.getInternalConstants(rootPath);
        try {
            File fl = new File(rootPath);
            fl.mkdirs();
        } catch (Exception e) {
            ResourcePool.LogException(e, this);
            rootPath = "log";
        }
        
        return rootPath;
    }

    final public synchronized ArrayList getLog(String name2) {
        ArrayList res = (ArrayList) this.mLogs.get(name2);

        if (res == null) {
            res = new ArrayList();
            this.mLogs.put(name2, res);
        }
        return res;
    }

    final public boolean isKilled() {
        return false;
    }

    final public boolean isPaused() {
        return false;
    }

}