/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

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

    private java.sql.Timestamp mStartDate, mEndDate, mExecutionDate;

    /**
     * @return Returns the endDate.
     */
    public final java.sql.Timestamp getEndDate() {
        return mEndDate;
    }

    /**
     * @param pEndDate The endDate to set.
     */
    public final void setEndDate(java.sql.Timestamp pEndDate) {
        mEndDate = pEndDate;
    }

    /**
     * @return Returns the startDate.
     */
    public final java.sql.Timestamp getStartDate() {
        return mStartDate;
    }

    /**
     * @param pStartDate The startDate to set.
     */
    public final void setStartDate(java.sql.Timestamp pStartDate) {
        mStartDate = pStartDate;
    }

    /**
     * ETLStatus constructor comment.
     */
    public ETLStatus() {
        super();
        setStatusCode(0); // Good practice to set default code here
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:09:54 PM)
     * 
     * @return int
     */
    public synchronized int getErrorCode() {
        return iErrorCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:11:24 PM)
     * 
     * @return java.lang.String
     */
    public synchronized java.lang.String getErrorMessage() {
        return strErrorMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 12:31:11 PM)
     * 
     * @return int
     */
    public synchronized int getStatusCode() {
        return iStatusCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 4:43:42 PM)
     * 
     * @return java.lang.String
     */
    public String getStatusMessage() {
        int iStatusCode = getStatusCode();

        return getStatusMessageForCode(iStatusCode) + " " + this.strExtendedMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:05:07 AM)
     * 
     * @return java.lang.String
     * @param iStatusCode int
     */
    public String getStatusMessageForCode(int iStatusCode) {
        String[] astrMessages = getStatusMessages();

        if (isValidStatusCode(iStatusCode)) {
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
        return astrStatusMessages;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:10:35 PM)
     * 
     * @return int
     */
    public synchronized int getWarningCode() {
        return iWarningCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:12:05 PM)
     * 
     * @return java.lang.String
     */
    public synchronized java.lang.String getWarningMessage() {
        return strWarningMessage;
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 1:22:09 PM)
     * 
     * @return java.lang.String
     * @param iStatus int
     */
    public boolean isValidStatusCode(int iStatusCode) {
        if ((iStatusCode >= 0) && (iStatusCode < getStatusMessages().length)) {
            return true;
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:09:54 PM)
     * 
     * @param newErrorCode int
     */
    public synchronized void setErrorCode(int newErrorCode) {
        iErrorCode = newErrorCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:11:24 PM)
     * 
     * @param newErrorMessage java.lang.String
     */
    public synchronized void setErrorMessage(java.lang.String newErrorMessage) {
        strErrorMessage = newErrorMessage;
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
        if (isValidStatusCode(iNewStatus)) {
            iStatusCode = iNewStatus;
        }

        return iStatusCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:10:35 PM)
     * 
     * @param newWarningCode int
     */
    public synchronized void setWarningCode(int newWarningCode) {
        iWarningCode = newWarningCode;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:12:05 PM)
     * 
     * @param newWarningMessage java.lang.String
     */
    public synchronized void setWarningMessage(java.lang.String newWarningMessage) {
        strWarningMessage = newWarningMessage;
    }

    public synchronized void setExtendedMessage(java.lang.String newExtendedMessage) {
        if(strExtendedMessage != null && strExtendedMessage.equals(newExtendedMessage))
            return;
        
        strExtendedMessage = newExtendedMessage;
        messageChanged = true;
    }

    /**
     * Returns a String that represents a summary of the current status.
     * 
     * @return a string representation of the receiver
     */
    public String toString() {
        int iStatusCode = getStatusCode();

        if (isValidStatusCode(iStatusCode)) {
            return "(" + iStatusCode + ") " + getStatusMessage();
        }

        return "";
    }

    /**
     * @return Returns the executionDate.
     */
    public final java.sql.Timestamp getExecutionDate() {
        return mExecutionDate;
    }

    /**
     * @param pExecutionDate The executionDate to set.
     */
    public final void setExecutionDate(java.sql.Timestamp pExecutionDate) {
        mExecutionDate = pExecutionDate;
    }

    
    /**
     * @return Returns the server.
     */
    public final int getServerID() {
        return miServer;
    }

    
    /**
     * @param pServer The server to set.
     */
    public final void setServerID(int pServer) {
        miServer = pServer;
    }
}
