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

import java.util.Date;

// TODO: Auto-generated Javadoc
/**
 * This is a basic class to hold info of the job-load Errors. Creation date: (7/27/2006)
 * 
 * @author dnguyen
 */
public class ETLJobError {

    // Error details
    /** The i job execution ID. */
    protected int iJobExecutionID;
    
    /** The s job ID. */
    protected String sJobID = null;
    
    /** The d date. */
    protected Date dDate = new Date();
    
    /** The s code. */
    protected String sCode = null;
    
    /** The s message. */
    protected String sMessage = null;
    
    /** The s details. */
    protected String sDetails = null;
    
    /** The s step name. */
    protected String sStepName = null;
    
    /** The s level. */
    protected String sLevel = null; // TODO: implement this

    /**
     * Sets the exec ID.
     * 
     * @param pExecID the new exec ID
     */
    public void setExecID(int pExecID) {
        this.iJobExecutionID = pExecID;
    }

    /**
     * Gets the exec ID.
     * 
     * @return the exec ID
     */
    public int getExecID() {
        return this.iJobExecutionID;
    }

    /**
     * Sets the job ID.
     * 
     * @param pJobID the new job ID
     */
    public void setJobID(String pJobID) {
        this.sJobID = pJobID;
    }

    /**
     * Gets the job ID.
     * 
     * @return the job ID
     */
    public String getJobID() {
        return this.sJobID;
    }

    /**
     * Sets the date.
     * 
     * @param pDate the new date
     */
    public void setDate(Date pDate) {
        this.dDate = pDate;
    }

    /**
     * Gets the date.
     * 
     * @return the date
     */
    public Date getDate() {
        return this.dDate;
    }

    /**
     * Sets the code.
     * 
     * @param pCode the new code
     */
    public void setCode(String pCode) {
        this.sCode = pCode;
    }

    /**
     * Gets the code.
     * 
     * @return the code
     */
    public String getCode() {
        return this.sCode;
    }

    /**
     * Sets the messag.
     * 
     * @param pMessage the new messag
     */
    public void setMessag(String pMessage) {
        this.sMessage = pMessage;
    }

    /**
     * Gets the message.
     * 
     * @return the message
     */
    public String getMessage() {
        return this.sMessage;
    }

    /**
     * Sets the details.
     * 
     * @param pDetails the new details
     */
    public void setDetails(String pDetails) {
        this.sDetails = pDetails;
    }

    /**
     * Gets the details.
     * 
     * @return the details
     */
    public String getDetails() {
        return this.sDetails;
    }

    /**
     * Sets the step name.
     * 
     * @param pStepName the new step name
     */
    public void setStepName(String pStepName) {
        this.sStepName = pStepName;
    }

    /**
     * Gets the step name.
     * 
     * @return the step name
     */
    public String getStepName() {
        return this.sStepName;
    }

}
