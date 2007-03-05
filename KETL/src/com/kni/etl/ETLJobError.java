/**
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */
package com.kni.etl;

import java.util.Date;

/**
 * This is a basic class to hold info of the job-load Errors. Creation date: (7/27/2006)
 * 
 * @author dnguyen
 */
public class ETLJobError {

    // Error details
    protected int iJobExecutionID;
    protected String sJobID = null;
    protected Date dDate = new Date();
    protected String sCode = null;
    protected String sMessage = null;
    protected String sDetails = null;
    protected String sStepName = null;
    protected String sLevel = null; // TODO: implement this

    public void setExecID(int pExecID) {
        iJobExecutionID = pExecID;
    }

    public int getExecID() {
        return iJobExecutionID;
    }

    public void setJobID(String pJobID) {
        sJobID = pJobID;
    }

    public String getJobID() {
        return sJobID;
    }

    public void setDate(Date pDate) {
        dDate = pDate;
    }

    public Date getDate() {
        return dDate;
    }

    public void setCode(String pCode) {
        sCode = pCode;
    }

    public String getCode() {
        return sCode;
    }

    public void setMessag(String pMessage) {
        sMessage = pMessage;
    }

    public String getMessage() {
        return sMessage;
    }

    public void setDetails(String pDetails) {
        sDetails = pDetails;
    }

    public String getDetails() {
        return sDetails;
    }

    public void setStepName(String pStepName) {
        sStepName = pStepName;
    }

    public String getStepName() {
        return sStepName;
    }

}
