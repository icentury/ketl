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
 * The Interface XMLMetadataCalls.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public interface XMLMetadataCalls {

    /**
     * Gets the connected.
     * 
     * @param pServerID the server ID
     * 
     * @return the connected
     */
    public String getConnected(String pServerID);

    /**
     * Gets the server list.
     * 
     * @param clientHashedUser the client hashed user
     * @param clientHashedPwsd the client hashed pwsd
     * 
     * @return the server list
     */
    public String getServerList(String clientHashedUser, String clientHashedPwsd);

    /**
     * Gets the server cluster details.
     * 
     * @param pRootServerID the root server ID
     * 
     * @return the server cluster details
     */
    public String getServerClusterDetails(String pRootServerID);

    /**
     * Gets the loads.
     * 
     * @param pServerID the server ID
     * @param pLastModified the last modified
     * 
     * @return the loads
     */
    public String getLoads(String pServerID, Date pLastModified);

    /**
     * Gets the load jobs.
     * 
     * @param pServerID the server ID
     * @param pLoadID the load ID
     * @param pLastModified the last modified
     * 
     * @return the load jobs
     */
    public String getLoadJobs(String pServerID, int pLoadID, Date pLastModified);

    /**
     * Gets the project jobs.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pLastModified the last modified
     * 
     * @return the project jobs
     */
    public String getProjectJobs(String pServerID, int pProjectID, Date pLastModified);

    /**
     * Gets the projects.
     * 
     * @param pServerID the server ID
     * @param pLastModified the last modified
     * 
     * @return the projects
     */
    public String getProjects(String pServerID, Date pLastModified);

    /**
     * Gets the job.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pJobID the job ID
     * 
     * @return the job
     */
    public String getJob(String pServerID, int pProjectID, String pJobID);

    /**
     * Gets the job status.
     * 
     * @param pServerID the server ID
     * @param pJobExecutionID the job execution ID
     * 
     * @return the job status
     */
    public String getJobStatus(String pServerID, int pJobExecutionID);

    /**
     * Sets the job status.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pJobID the job ID
     * @param pLoadID the load ID
     * @param pJobExecutionID the job execution ID
     * @param pState the state
     * 
     * @return the string
     */
    public String setJobStatus(String pServerID, String pProjectID, String pJobID, int pLoadID, int pJobExecutionID,
            String pState);

    /**
     * Sets the execution status.
     * 
     * @param pServerID the server ID
     * @param pLoadID the load ID
     * @param pExecID the exec ID
     * @param pStatus the status
     * 
     * @return the string
     */
    public String setExecutionStatus(String pServerID, int pLoadID, int pExecID, String pStatus);

    /**
     * Update job.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pJobXML the job XML
     * @param pForceOverwrite the force overwrite
     * 
     * @return the string
     */
    public String updateJob(String pServerID, String pProjectID, String pJobXML, boolean pForceOverwrite);

    /**
     * Adds the server.
     * 
     * @param pUsername the username
     * @param pPassword the password
     * @param pJDBCDriver the JDBC driver
     * @param pURL the URL
     * @param pMDPrefix the MD prefix
     * @param pPassphrase the passphrase
     * 
     * @return true, if successful
     */
    public boolean addServer(String pUsername, String pPassword, String pJDBCDriver, String pURL, String pMDPrefix,
            String pPassphrase);

    /**
     * Removes the server.
     * 
     * @param pServerID the server ID
     * 
     * @return true, if successful
     */
    public boolean removeServer(String pServerID);

    /**
     * Schedule job.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pJobID the job ID
     * @param pMonth the month
     * @param pMonthOfYear the month of year
     * @param pDay the day
     * @param pDayOfWeek the day of week
     * @param pDayOfMonth the day of month
     * @param pHour the hour
     * @param pHourOfDay the hour of day
     * @param pMinute the minute
     * @param pMinuteOfHour the minute of hour
     * @param pDescription the description
     * @param pOnceOnlyDate the once only date
     * @param pEnableDate the enable date
     * @param pDisableDate the disable date
     * 
     * @return the string
     */
    public String scheduleJob(String pServerID, int pProjectID, String pJobID, int pMonth, int pMonthOfYear, int pDay,
            int pDayOfWeek, int pDayOfMonth, int pHour, int pHourOfDay, int pMinute, int pMinuteOfHour,
            String pDescription, Date pOnceOnlyDate, Date pEnableDate, Date pDisableDate);

    /**
     * Delete load.
     * 
     * @param pServerID the server ID
     * @param pLoadID the load ID
     * 
     * @return true, if successful
     */
    public boolean deleteLoad(String pServerID, String pLoadID);

    /**
     * Gets the lock.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pForceOverwrite the force overwrite
     * 
     * @return the lock
     */
    public int getLock(String pServerID, String pProjectID, boolean pForceOverwrite);

    /**
     * Release lock.
     * 
     * @param pServerID the server ID
     * @param pLockID the lock ID
     */
    public void releaseLock(String pServerID, int pLockID);

    /**
     * Refresh lock.
     * 
     * @param pServerID the server ID
     * @param pLockID the lock ID
     * 
     * @return true, if successful
     */
    public boolean refreshLock(String pServerID, int pLockID);

    /**
     * Refresh load status.
     * 
     * @param pServerID the server ID
     * @param pLoadID the load ID
     * @param pLastRefreshDate the last refresh date
     * 
     * @return the string
     */
    public String refreshLoadStatus(String pServerID, int pLoadID, Date pLastRefreshDate);

    /**
     * Refresh project status.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pLastRefreshDate the last refresh date
     * 
     * @return the string
     */
    public String refreshProjectStatus(String pServerID, String pProjectID, Date pLastRefreshDate);

    /**
     * Execute job.
     * 
     * @param pServerID the server ID
     * @param pProjectID the project ID
     * @param pJobID the job ID
     * @param pIgnoreDependencies the ignore dependencies
     * @param pAllowMultiple the allow multiple
     * 
     * @return the string
     */
    public String executeJob(String pServerID, int pProjectID, String pJobID, boolean pIgnoreDependencies,
            boolean pAllowMultiple);

    /**
     * Gets the job errors.
     * 
     * @param pServerID the server ID
     * @param pJobID the job ID
     * @param pStartDate the start date
     * 
     * @return the job errors
     */
    public String getJobErrors(String pServerID, String pJobID, Date pStartDate);

    /**
     * Gets the load errors.
     * 
     * @param pServerID the server ID
     * @param pLoadID the load ID
     * @param pLastModified the last modified
     * 
     * @return the load errors
     */
    public String getLoadErrors(String pServerID, int pLoadID, Date pLastModified);

    /**
     * Gets the execution errors.
     * 
     * @param pServerID the server ID
     * @param pLoadID the load ID
     * @param pExecID the exec ID
     * @param pLastModified the last modified
     * 
     * @return the execution errors
     */
    public String getExecutionErrors(String pServerID, int pLoadID, int pExecID, Date pLastModified);

    /**
     * Adds the jobs and params.
     * 
     * @param pServerID the server ID
     * @param xmlFile the xml file
     * 
     * @return the string
     */
    public String addJobsAndParams(String pServerID, String xmlFile);

}