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
        this.iJobExecutionID = pExecID;
    }

    public int getExecID() {
        return this.iJobExecutionID;
    }

    public void setJobID(String pJobID) {
        this.sJobID = pJobID;
    }

    public String getJobID() {
        return this.sJobID;
    }

    public void setDate(Date pDate) {
        this.dDate = pDate;
    }

    public Date getDate() {
        return this.dDate;
    }

    public void setCode(String pCode) {
        this.sCode = pCode;
    }

    public String getCode() {
        return this.sCode;
    }

    public void setMessag(String pMessage) {
        this.sMessage = pMessage;
    }

    public String getMessage() {
        return this.sMessage;
    }

    public void setDetails(String pDetails) {
        this.sDetails = pDetails;
    }

    public String getDetails() {
        return this.sDetails;
    }

    public void setStepName(String pStepName) {
        this.sStepName = pStepName;
    }

    public String getStepName() {
        return this.sStepName;
    }

}
