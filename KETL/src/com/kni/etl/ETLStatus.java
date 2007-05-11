/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;

/**
 * Insert the type's description here. Creation date: (5/3/2002 12:21:59 PM)
 * 
 * @author: Administrator
 */
public class ETLStatus {

    protected int iStatusCode;
    private static java.lang.String[] astrStatusMessages = {};
    protected int iErrorCode;
    protected int iWarningCode, miServer;
    public boolean messageChanged = false;
    protected java.lang.String strErrorMessage = "";
    protected java.lang.String strWarningMessage = "";
    protected java.lang.String strExtendedMessage = "";
    protected Element statsNode = null;
    private java.sql.Timestamp mStartDate, mEndDate, mExecutionDate;

    /**
     * @return Returns the endDate.
     */
    public final java.sql.Timestamp getEndDate() {
        return this.mEndDate;
    }

    /**
     * @param pEndDate The endDate to set.
     */
    public final void setEndDate(java.sql.Timestamp pEndDate) {
        this.mEndDate = pEndDate;
    }

    /**
     * @return Returns the startDate.
     */
    public final java.sql.Timestamp getStartDate() {
        return this.mStartDate;
    }

    /**
     * @param pStartDate The startDate to set.
     */
    public final void setStartDate(java.sql.Timestamp pStartDate) {
        this.mStartDate = pStartDate;
    }

    /**
     * ETLStatus constructor comment.
     */
    public ETLStatus() {
        super();
        this.setStatusCode(0); // Good practice to set default code here
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:09:54 PM)
     * 
     * @return int
     */
    public synchronized int getErrorCode() {
        return this.iErrorCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:11:24 PM)
     * 
     * @return java.lang.String
     */
    public synchronized java.lang.String getErrorMessage() {
        return this.strErrorMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 12:31:11 PM)
     * 
     * @return int
     */
    public synchronized int getStatusCode() {
        return this.iStatusCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 12:31:11 PM)
     * 
     * @return int
     */
    public synchronized String getExtendedMessage() {
        return this.strExtendedMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 4:43:42 PM)
     * 
     * @return java.lang.String
     */
    public String getStatusMessage() {
        int iStatusCode = this.getStatusCode();

        return this.getStatusMessageForCode(iStatusCode) + " " + this.strExtendedMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:05:07 AM)
     * 
     * @return java.lang.String
     * @param iStatusCode int
     */
    public String getStatusMessageForCode(int iStatusCode) {
        String[] astrMessages = this.getStatusMessages();

        if (this.isValidStatusCode(iStatusCode)) {
            return astrMessages[iStatusCode];
        }

        return "";
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 10:58:21 AM)
     * 
     * @return java.lang.String[]
     */
    public String[] getStatusMessages() {
        return ETLStatus.astrStatusMessages;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:10:35 PM)
     * 
     * @return int
     */
    public synchronized int getWarningCode() {
        return this.iWarningCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:12:05 PM)
     * 
     * @return java.lang.String
     */
    public synchronized java.lang.String getWarningMessage() {
        return this.strWarningMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 1:22:09 PM)
     * 
     * @return java.lang.String
     * @param iStatus int
     */
    public boolean isValidStatusCode(int iStatusCode) {
        if ((iStatusCode >= 0) && (iStatusCode < this.getStatusMessages().length)) {
            return true;
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                "isValidStatusCode: Invalid status code ID=" + iStatusCode);

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:09:54 PM)
     * 
     * @param newErrorCode int
     */
    public synchronized void setErrorCode(int newErrorCode) {
        this.iErrorCode = newErrorCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:11:24 PM)
     * 
     * @param newErrorMessage java.lang.String
     */
    public synchronized void setErrorMessage(java.lang.String newErrorMessage) {
        this.strErrorMessage = newErrorMessage;
    }

    /**
     * Sets the status of the object.
     * 
     * @todo: Should we throw an exception for a valid status? Or perhaps return the status that we are really setting?
     *        Creation date: (5/3/2002 12:31:11 PM)
     * @param newIStatus int
     */
    public synchronized int setStatusCode(int iNewStatus) {
        // Verify it's a valid status

        // ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.DEBUG_MESSAGE, "setStatusCode: status code ID=" +
        // iNewStatus + ", previous status code ID = " + this.iStatusCode);

        if (this.isValidStatusCode(iNewStatus)) {
            this.iStatusCode = iNewStatus;
        }

        return this.iStatusCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:10:35 PM)
     * 
     * @param newWarningCode int
     */
    public synchronized void setWarningCode(int newWarningCode) {
        this.iWarningCode = newWarningCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:12:05 PM)
     * 
     * @param newWarningMessage java.lang.String
     */
    public synchronized void setWarningMessage(java.lang.String newWarningMessage) {
        this.strWarningMessage = newWarningMessage;
    }

    public synchronized void setExtendedMessage(java.lang.String newExtendedMessage) {
        if (this.strExtendedMessage != null && this.strExtendedMessage.equals(newExtendedMessage))
            return;

        this.strExtendedMessage = newExtendedMessage;
        this.messageChanged = true;
    }

    /**
     * Returns a String that represents a summary of the current status.
     * 
     * @return a string representation of the receiver
     */
    @Override
    public String toString() {
        int iStatusCode = this.getStatusCode();

        if (this.isValidStatusCode(iStatusCode)) {
            return "(" + iStatusCode + ") " + this.getStatusMessage();
        }

        return "";
    }

    /**
     * @return Returns the executionDate.
     */
    public final java.sql.Timestamp getExecutionDate() {
        return this.mExecutionDate;
    }

    /**
     * @param pExecutionDate The executionDate to set.
     */
    public final void setExecutionDate(java.sql.Timestamp pExecutionDate) {
        this.mExecutionDate = pExecutionDate;
    }

    /**
     * @return Returns the server.
     */
    public final int getServerID() {
        return this.miServer;
    }

    /**
     * @param pServer The server to set.
     */
    public final void setServerID(int pServer) {
        this.miServer = pServer;
    }

    public final String getXMLStats() {
        if (this.statsNode == null)
            return null;
        return XMLHelper.outputXML(this.statsNode);
    }

    protected Element getStatsNode() {
        if (this.statsNode == null) {
            Document documentRoot;

            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder;
            try {
                builder = dmf.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            }
            documentRoot = builder.newDocument();
            this.statsNode = documentRoot.createElement("STATS");
            documentRoot.appendChild(this.statsNode);
        }

        return this.statsNode;
    }

    public void setStats(int records, long executionTime) {
        Element e = this.getStatsNode();

        e.setAttribute("RECORDS", Integer.toString(records));
        e.setAttribute("TIMING", Long.toString(executionTime));

    }
}
