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

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/3/2002 1:41:54 PM)
 * 
 * @author: Administrator
 */
public class ETLJob {

    /** The js status. */
    transient protected ETLJobStatus jsStatus;
    
    /** The b cancel job. */
    protected boolean bCancelJob = false;
    
    /** The load ID. */
    protected int iLoadID;
    
    /** The s job ID. */
    protected String sJobID;
    
    /** The job execution ID. */
    protected int iJobExecutionID;
    
    /** The str action. */
    protected java.lang.String strAction;
    
    /** The mo action. */
    transient protected Object moAction;
    
    /** The tag level parameter list cache. */
    HashMap mTagLevelParameterListCache = null;
    
    /** The md creation date. */
    protected java.util.Date mdCreationDate = new java.util.Date();
    
    /** The Seconds before retry. */
    private int SecondsBeforeRetry = 0;
    
    /** The ms global parameter list name. */
    private String msGlobalParameterListName = null;
    
    /** The mi global parameter list ID. */
    private int miGlobalParameterListID = -1;
    
    /** The Retry attempts. */
    private int RetryAttempts = 0;
    
    /** The Max retries. */
    private int MaxRetries = 0;
    
    /** The Job type ID. */
    private int JobTypeID = -1;
    
    /** The Project ID. */
    private int ProjectID = -1;
    
    /** The Job type name. */
    private String JobTypeName;
    
    /** The Name. */
    private String Name = "";
    
    /** The Description. */
    private String Description = "";
    
    /** The job definition. */
    String jobDefinition = null;
    
    /** The Disable alerting. */
    boolean DisableAlerting = false;

    /**
     * Write object.
     * 
     * @param s the s
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
        s.writeObject(this.moAction.toString());
    }

    /**
     * Read object.
     * 
     * @param s the s
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void readObject(ObjectInputStream s) throws IOException {
        try {
            s.defaultReadObject();
            this.moAction = s.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sets the job type name.
     * 
     * @param pName the new job type name
     */
    public void setJobTypeName(String pName) {
        this.JobTypeName = pName;
    }

    /**
     * Gets the job type name.
     * 
     * @return the job type name
     */
    public String getJobTypeName() {
        return (this.JobTypeName);
    }

    /**
     * Gets the project name.
     * 
     * @return the project name
     */
    public String getProjectName() {
        try {
            return ResourcePool.getMetadata().getProjectName(this.ProjectID);
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Gets the parameter lists.
     * 
     * @param pParameterListName the parameter list name
     * 
     * @return the parameter lists
     */
    public ArrayList getParameterLists(String pParameterListName) {
        return (ArrayList) this.getParameterListCache().get(pParameterListName);
    }

    /**
     * Gets the parameter list cache.
     * 
     * @return Returns the parameterListCache.
     */
    public final HashMap getParameterListCache() {
        return this.mTagLevelParameterListCache;
    }

    /**
     * Sets the parameter list cache.
     * 
     * @param pParameterListCache The parameterListCache to set.
     */
    public final void setParameterListCache(HashMap pParameterListCache) {
        this.mTagLevelParameterListCache = pParameterListCache;
    }

    /**
     * Gets the description.
     * 
     * @return Returns the description.
     */
    public String getDescription() {
        return this.Description;
    }

    /**
     * Sets the description.
     * 
     * @param description The description to set.
     */
    public void setDescription(String description) {
        this.Description = description;
    }

    /**
     * Gets the XML job definition.
     * 
     * @param rootNode the root node
     * 
     * @return the XML job definition
     */
    public String getXMLJobDefinition(Element rootNode) {
        try {
            Element e = this.getJobAsXMLElement(rootNode);

            return XMLHelper.outputXML(e);
        } catch (Exception e) {
            ResourcePool.LogException(e, this);
        }

        return null;
    }

    /**
     * Gets the XML job definition.
     * 
     * @return the XML job definition
     */
    public String getXMLJobDefinition() {
        try {
            Element e = this.getJobAsXMLElement(null);

            return XMLHelper.outputXML(e);
        } catch (Exception e) {
            ResourcePool.LogException(e, this);
        }

        return null;
    }

    /**
     * Gets the job as XML element.
     * 
     * @param storeRootNode the store root node
     * 
     * @return the job as XML element
     * 
     * @throws ParserConfigurationException the parser configuration exception
     * @throws SQLException the SQL exception
     * @throws Exception the exception
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

        if (this.dependencies == null) {
            this.dependencies = ResourcePool.getMetadata().getJobDependencies(this.getJobID());
        }

        for (String[] element : this.dependencies) {
            Element deps;

            if (element[1] == Metadata.DEPENDS_ON) {
                deps = documentRoot.createElement("DEPENDS_ON");
            }
            else {
                deps = documentRoot.createElement("WAITS_ON");
            }

            deps.appendChild(documentRoot.createTextNode(element[0]));
            e.appendChild(deps);
            e.appendChild(documentRoot.createTextNode("\n"));
        }

        this.setChildNodes(e);
        e.appendChild(documentRoot.createTextNode("\n"));
        return e;
    }

    /**
     * Sets the child nodes.
     * 
     * @param pParentNode the parent node
     * 
     * @return the node
     */
    protected Node setChildNodes(Node pParentNode) {
        Element e = pParentNode.getOwnerDocument().createElement("EMPTY");

        // XMLHelper.outputXML(this.)
        Object action = this.getAction(false);

        if (action != null) {
            e.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction(false).toString()));
        }

        pParentNode.appendChild(e);

        return e;
    }

    /** The dependencies. */
    String[][] dependencies = null;

    /**
     * Gets the depedencies.
     * 
     * @return the depedencies
     * 
     * @throws Exception the exception
     */
    public String getDepedencies() throws Exception {
        String strWaitsOn = null;
        String strDependsOn = null;

        if (this.dependencies == null) {
            this.dependencies = ResourcePool.getMetadata().getJobDependencies(this.getJobID());
        }

        StringBuffer sb = new StringBuffer();

        for (String[] element : this.dependencies) {
            if (element[1] == Metadata.DEPENDS_ON) {
                if (strDependsOn == null) {
                    strDependsOn = "DEPENDS_ON (Critical dependencies)\n\t" + element[0];
                }
                else {
                    strDependsOn = strDependsOn + "\n\t" + element[0];
                }
            }
            else {
                if (strWaitsOn == null) {
                    strWaitsOn = "WAITS_ON (Non-critical dependencies)\n\t" + element[0];
                }
                else {
                    strWaitsOn = strWaitsOn + "\n\t" + element[0];
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

    /**
     * Gets the job definition.
     * 
     * @return the job definition
     * 
     * @throws Exception the exception
     */
    public String getJobDefinition() throws Exception {
        if (this.jobDefinition == null) {
            if (this.dependencies == null) {
                this.dependencies = ResourcePool.getMetadata().getJobDependencies(this.getJobID());
            }

            String strWaitsOn = null;
            String strDependsOn = null;

            StringBuffer sb = new StringBuffer();

            for (String[] element : this.dependencies) {
                if (element[1] == Metadata.DEPENDS_ON) {
                    if (strDependsOn == null) {
                        strDependsOn = "--   DEPENDS_ON = " + element[0];
                    }
                    else {
                        strDependsOn = strDependsOn + ", " + element[0];
                    }
                }
                else {
                    if (strWaitsOn == null) {
                        strWaitsOn = "--   WAITS_ON = " + element[0];
                    }
                    else {
                        strWaitsOn = strWaitsOn + ", " + element[0];
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
            sb.append(this.getAction(false) + "\n");
            sb.append("-- END_SQL_JOB\n");
            this.jobDefinition = sb.toString();
        }

        return (this.jobDefinition);
    }

    /**
     * Gets the name.
     * 
     * @return Returns the name.
     */
    public String getName() {
        return this.Name;
    }

    /**
     * Sets the name.
     * 
     * @param name The name to set.
     */
    public void setName(String name) {
        this.Name = name;
    }

    /**
     * ETLJob constructor comment.
     * 
     * @throws Exception TODO
     */
    public ETLJob() throws Exception {
        super();

        if (this.jsStatus == null) {
            this.jsStatus = new ETLJobStatus();
        }
    }

    /**
     * Gets the job type ID.
     * 
     * @return the job type ID
     */
    public int getJobTypeID() {
        return this.JobTypeID;
    }

    /**
     * Sets the job type ID.
     * 
     * @param pJobTypeID the new job type ID
     */
    public void setJobTypeID(int pJobTypeID) {
        this.JobTypeID = pJobTypeID;
    }

    /**
     * Instantiates a new ETL job.
     * 
     * @param pjsStatus the pjs status
     * 
     * @throws Exception the exception
     */
    public ETLJob(ETLJobStatus pjsStatus) throws Exception {
        this();
        this.jsStatus = pjsStatus;
    }

    /**
     * Simplified version of setCancelJob() - used strictly for cancelling jobs. Creation date: (5/3/2002 5:36:11 PM)
     * 
     * @return boolean
     */
    public boolean cancelJob() {
        return this.setCancelJob(true);
    }

    /**
     * To be used by jobs that have connections to close, etc. Creation date: (5/9/2002 2:27:36 PM)
     */
    public void cleanup() {
        this.closeLog();
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 3:32:26 PM)
     * 
     * @param resolveConstants TODO
     * 
     * @return java.lang.String
     */
    public Object getAction(boolean resolveConstants) {
        if (this.moAction instanceof String) {
            String strAction = (String) this.moAction;

            if (strAction != null && resolveConstants) {
                strAction = this.getInternalConstants(strAction);
            }

            return strAction;
        }

        return this.moAction;
    }

    /**
     * Gets the internal constants.
     * 
     * @param strAction the str action
     * 
     * @return the internal constants
     */
    public String getInternalConstants(String strAction) {
        for (String element : EngineConstants.PARAMETER_LOAD_ID) {
            strAction = EngineConstants.replaceParameter(strAction, element, new Integer(this.getLoadID()).toString());
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

        for (String element : EngineConstants.PARAMETER_JOB_EXECUTION_ID) {
            strAction = EngineConstants.replaceParameter(strAction, element, new Integer(this.getJobExecutionID())
                    .toString());
        }
        return strAction;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:43 AM)
     * 
     * @return int
     */
    public int getJobExecutionID() {
        return this.iJobExecutionID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:31 AM)
     * 
     * @return int
     */
    public String getJobID() {
        return this.sJobID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:17 AM)
     * 
     * @return int
     */
    public int getLoadID() {
        return this.iLoadID;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:12:54 PM)
     * 
     * @return int
     */
    public int getMaxRetries() {
        return this.MaxRetries;
    }

    /**
     * This method returns the top level parameters in a given list by name. NOTE: This will NOT return any lower level
     * parameters. To obtain these, you must retrieve the sub parameter list. Creation date: (5/8/2002 4:31:46 PM)
     * 
     * @param oName the o name
     * 
     * @return java.lang.Object
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
        return this.RetryAttempts;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:07:52 PM)
     * 
     * @return int
     */
    public int getSecondsBeforeRetry() {
        return this.SecondsBeforeRetry;
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
        return this.jsStatus;
    }

    /** The b cancel successfull. */
    private boolean bCancelSuccessfull = false;

    /**
     * Cancel successfull.
     * 
     * @param arg0 the arg0
     */
    public void cancelSuccessfull(boolean arg0) {
        this.bCancelSuccessfull = arg0;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 5:28:54 PM)
     * 
     * @return boolean
     */
    public boolean isCancelSuccessfull() {
        return this.bCancelSuccessfull;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 5:28:54 PM)
     * 
     * @return boolean
     */
    public synchronized boolean isCancelled() {
        return this.bCancelJob;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 2:55:31 PM)
     * 
     * @return boolean
     */
    public synchronized boolean isCompleted() {
        switch (this.jsStatus.getStatusCode()) {
        case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
        case ETLJobStatus.PENDING_CLOSURE_FAILED:
        case ETLJobStatus.PENDING_CLOSURE_CANCELLED:
            return true;
        }

        return false;
    }

    /**
     * Checks if is successful.
     * 
     * @return true, if is successful
     */
    public synchronized boolean isSuccessful() {
        switch (this.jsStatus.getStatusCode()) {
        case ETLJobStatus.SUCCESSFUL:
        case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
            return true;
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 3:32:26 PM)
     * 
     * @param oAction the o action
     * 
     * @throws Exception TODO
     */
    public void setAction(Object oAction) throws Exception {
        this.moAction = oAction;
    }

    /**
     * Sets cancel flag for job if possible. NOTE: this does not guarantee that the job will be cancelled, or that a
     * previous cancellation request will not be removed. It all depends on whether the worker thread will see this flag
     * before completion. Returns: true if successful in changing the flag, false otherwise (if already set as
     * requested, or if in invalid status)
     * 
     * @param bCancel the b cancel
     * 
     * @return true, if set cancel job
     */
    public boolean setCancelJob(boolean bCancel) {
        // Cannot change the cancel flag if we're already completed...
        if (this.isCompleted() == true) {
            return false;
        }

        // Only set the flag if it's a new value...
        if (bCancel != this.bCancelJob) {
            ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Cancelling job " + this.getJobID());
            this.bCancelJob = bCancel;

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
        this.iJobExecutionID = newJobExecutionID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:31 AM)
     * 
     * @param newJobID int
     */
    public void setJobID(String newJobID) {
        this.sJobID = newJobID;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 9:57:17 AM)
     * 
     * @param newLoadID int
     */
    public void setLoadID(int newLoadID) {
        this.iLoadID = newLoadID;
    }

    /**
     * Insert the method's description here. Creation date: (5/20/2002 2:12:54 PM)
     * 
     * @param newMaxRetries int
     */
    public void setMaxRetries(int newMaxRetries) {
        this.MaxRetries = newMaxRetries;
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
        this.RetryAttempts = newRetryAttempts;
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
            this.SecondsBeforeRetry = newSecondsBeforeRetry;
        }
    }

    /**
     * Returns a String that represents the value of this object.
     * 
     * @return a string representation of the receiver
     */
    @Override
    public String toString() {
        // Insert code to print the receiver here.
        // This implementation forwards the message to super. You may replace or supplement this.
        return super.toString();
    }

    /**
     * Gets the creation date.
     * 
     * @return the creation date
     */
    public java.util.Date getCreationDate() {
        return this.mdCreationDate;
    }

    /**
     * Gets the project ID.
     * 
     * @return Returns the projectID.
     */
    public int getProjectID() {
        return this.ProjectID;
    }

    /**
     * Sets the project ID.
     * 
     * @param projectID The projectID to set.
     */
    public void setProjectID(int projectID) {
        this.ProjectID = projectID;
    }

    /**
     * Gets the global parameter list ID.
     * 
     * @return Returns the parameterListID.
     */
    public int getGlobalParameterListID() {
        return this.miGlobalParameterListID;
    }

    /**
     * Sets the global parameter list ID.
     * 
     * @param parameterListID The parameterListID to set.
     */
    public void setGlobalParameterListID(int parameterListID) {
        this.miGlobalParameterListID = parameterListID;
        this.msGlobalParameterListName = ResourcePool.getMetadata().getParameterListName(parameterListID);
        this.setGlobalParameterListName(null, this.msGlobalParameterListName);
    }

    /**
     * Sets the global parameter list name.
     * 
     * @param node the node
     * @param parameterList the parameter list
     */
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
     * Checks if is alerting disabled.
     * 
     * @return Returns the disableAlerting.
     */
    public boolean isAlertingDisabled() {
        return this.DisableAlerting;
    }

    /**
     * Sets the disable alerting.
     * 
     * @param disableAlerting The disableAlerting to set.
     */
    public void setDisableAlerting(boolean disableAlerting) {
        this.DisableAlerting = disableAlerting;
    }

    /**
     * Gets the parameter value.
     * 
     * @param parameterListName the parameter list name
     * @param parameterName the parameter name
     * @param defaultValue the default value
     * 
     * @return the parameter value
     */
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

    /** The counters. */
    private HashMap mCounters = new HashMap();
    
    /** The logs. */
    private HashMap mLogs = new HashMap();
    
    /** The logger failed. */
    private boolean mLoggerFailed = false;
    
    /** The mo dump. */
    private OutputStream moDump;
    
    /** The dump file. */
    private String mDumpFile;
    
    /** The mo dump buffer. */
    private BufferedOutputStream moDumpBuffer;
    
    /** The dump writer. */
    private PrintWriter mDumpWriter;

    /**
     * Gets the error counter.
     * 
     * @param name2 the name2
     * 
     * @return the error counter
     */
    final public synchronized SharedCounter getErrorCounter(String name2) {
        return this.getCounter("{${ERR}" + name2);
    }

    /**
     * Gets the counter.
     * 
     * @param pName the name
     * @param pClass the class
     * 
     * @return the counter
     */
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

    /**
     * Gets the counter.
     * 
     * @param name2 the name2
     * 
     * @return the counter
     */
    final public SharedCounter getCounter(String name2) {
        return this.getCounter(name2, Integer.class);
    }

    /**
     * Write log.
     */
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

    /**
     * Flush log.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void flushLog() throws IOException {
        if (this.moDump != null) {
            this.mDumpWriter.flush();
            this.moDumpBuffer.flush();
            this.moDump.flush();
        }
    }

    /**
     * Close log.
     */
    private void closeLog() {
        if (this.mLoggerFailed == false && this.moDump != null) {
            try {
                if (this.moDump != null) {
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

    /**
     * Gets the dump file.
     * 
     * @return the dump file
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public String getDumpFile() throws IOException {
        this.flushLog();
        return this.mDumpFile;
    }

    /**
     * Log job message.
     * 
     * @param obj the obj
     */
    public void logJobMessage(Object obj) {
        if (this.mLoggerFailed)
            return;
        try {
            if (this.moDump == null) {
                this.mDumpFile = this.getLoggingPath() + File.separator + this.getJobID() + "."
                        + this.getJobExecutionID() + ".joblog";
                this.moDump = new FileOutputStream(this.mDumpFile);
                this.moDumpBuffer = new BufferedOutputStream(this.moDump);
                this.mDumpWriter = new PrintWriter(this.moDumpBuffer);
            }

            if (obj instanceof Error)
                ((Error) obj).printStackTrace(this.mDumpWriter);
            else
                this.mDumpWriter.println(obj.toString());

        } catch (IOException e) {
            this.mLoggerFailed = true;
            System.err.println("Job logging failed: " + e.toString());
            e.printStackTrace();
        }

    }

    /**
     * Gets the logging path.
     * 
     * @return the logging path
     */
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

    /**
     * Gets the log.
     * 
     * @param name2 the name2
     * 
     * @return the log
     */
    final public synchronized ArrayList getLog(String name2) {
        ArrayList res = (ArrayList) this.mLogs.get(name2);

        if (res == null) {
            res = new ArrayList();
            this.mLogs.put(name2, res);
        }
        return res;
    }

    /**
     * Checks if is killed.
     * 
     * @return true, if is killed
     */
    final public boolean isKilled() {
        return false;
    }

    /**
     * Checks if is paused.
     * 
     * @return true, if is paused
     */
    final public boolean isPaused() {
        return false;
    }

}