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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/3/2002 12:21:59 PM)
 * 
 * @author: Administrator
 */
public class ETLStatus {

    /** The status code. */
    protected int iStatusCode;
    
    /** The astr status messages. */
    private static java.lang.String[] astrStatusMessages = {};
    
    /** The error code. */
    protected int iErrorCode;
    
    /** The mi server. */
    protected int iWarningCode, miServer;
    
    /** The message changed. */
    public boolean messageChanged = false;
    
    /** The str error message. */
    protected java.lang.String strErrorMessage = "";
    
    /** The str warning message. */
    protected java.lang.String strWarningMessage = "";
    
    /** The str extended message. */
    protected java.lang.String strExtendedMessage = "";
    
    /** The stats node. */
    protected Element statsNode = null;
    
    /** The execution date. */
    private java.sql.Timestamp mStartDate, mEndDate, mExecutionDate;

    /**
     * Gets the end date.
     * 
     * @return Returns the endDate.
     */
    public final java.sql.Timestamp getEndDate() {
        return this.mEndDate;
    }

    /**
     * Sets the end date.
     * 
     * @param pEndDate The endDate to set.
     */
    public final void setEndDate(java.sql.Timestamp pEndDate) {
        this.mEndDate = pEndDate;
    }

    /**
     * Gets the start date.
     * 
     * @return Returns the startDate.
     */
    public final java.sql.Timestamp getStartDate() {
        return this.mStartDate;
    }

    /**
     * Sets the start date.
     * 
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
     * @param iStatusCode int
     * 
     * @return java.lang.String
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
     * @param iStatusCode The status code
     * 
     * @return java.lang.String
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
     * @param iNewStatus The new status
     * 
     * @return the int
     * 
     * @todo: Should we throw an exception for a valid status? Or perhaps return the status that we are really setting?
     * Creation date: (5/3/2002 12:31:11 PM)
     */
    public synchronized int setStatusCode(int iNewStatus) {
        // Verify it's a valid status       
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

    /**
     * Sets the extended message.
     * 
     * @param newExtendedMessage the new extended message
     */
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
     * Gets the execution date.
     * 
     * @return Returns the executionDate.
     */
    public final java.sql.Timestamp getExecutionDate() {
        return this.mExecutionDate;
    }

    /**
     * Sets the execution date.
     * 
     * @param pExecutionDate The executionDate to set.
     */
    public final void setExecutionDate(java.sql.Timestamp pExecutionDate) {
        this.mExecutionDate = pExecutionDate;
    }

    /**
     * Gets the server ID.
     * 
     * @return Returns the server.
     */
    public final int getServerID() {
        return this.miServer;
    }

    /**
     * Sets the server ID.
     * 
     * @param pServer The server to set.
     */
    public final void setServerID(int pServer) {
        this.miServer = pServer;
    }

    /**
     * Gets the XML stats.
     * 
     * @return the XML stats
     */
    public final String getXMLStats() {
        if (this.statsNode == null)
            return null;
        return XMLHelper.outputXML(this.statsNode);
    }

    /**
     * Gets the stats node.
     * 
     * @return the stats node
     */
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

    /**
     * Sets the stats.
     * 
     * @param records the records
     * @param executionTime the execution time
     */
    public void setStats(int records, long executionTime) {
        Element e = this.getStatsNode();

        e.setAttribute("RECORDS", Integer.toString(records));
        e.setAttribute("TIMING", Long.toString(executionTime));

    }
}
