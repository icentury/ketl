/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on May 2, 2006
 * 
 */
package com.kni.etl;

import java.util.Date;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public interface XMLMetadataCalls {

    public String getConnected(String pServerID);

    public String getServerList(String clientHashedUser, String clientHashedPwsd);

    public String getServerClusterDetails(String pRootServerID);

    public String getLoads(String pServerID, Date pLastModified);

    public String getLoadJobs(String pServerID, int pLoadID, Date pLastModified);

    public String getProjectJobs(String pServerID, int pProjectID, Date pLastModified);

    public String getProjects(String pServerID, Date pLastModified);

    public String getJob(String pServerID, int pProjectID, String pJobID);

    public String getJobStatus(String pServerID, int pJobExecutionID);

    public String setJobStatus(String pServerID, String pProjectID, String pJobID, int pLoadID, int pJobExecutionID,
            String pState);

    public String setExecutionStatus(String pServerID, int pLoadID, int pExecID, String pStatus);

    public String updateJob(String pServerID, String pProjectID, String pJobXML, boolean pForceOverwrite);

    public boolean addServer(String pUsername, String pPassword, String pJDBCDriver, String pURL, String pMDPrefix,
            String pPassphrase);

    public boolean removeServer(String pServerID);

    public String scheduleJob(String pServerID, int pProjectID, String pJobID, int pMonth, int pMonthOfYear, int pDay,
            int pDayOfWeek, int pDayOfMonth, int pHour, int pHourOfDay, int pMinute, int pMinuteOfHour,
            String pDescription, Date pOnceOnlyDate, Date pEnableDate, Date pDisableDate);

    public boolean deleteLoad(String pServerID, String pLoadID);

    public int getLock(String pServerID, String pProjectID, boolean pForceOverwrite);

    public void releaseLock(String pServerID, int pLockID);

    public boolean refreshLock(String pServerID, int pLockID);

    public String refreshLoadStatus(String pServerID, int pLoadID, Date pLastRefreshDate);

    public String refreshProjectStatus(String pServerID, String pProjectID, Date pLastRefreshDate);

    public String executeJob(String pServerID, int pProjectID, String pJobID, boolean pIgnoreDependencies,
            boolean pAllowMultiple);

    public String getJobErrors(String pServerID, String pJobID, Date pStartDate);

    public String getLoadErrors(String pServerID, int pLoadID, Date pLastModified);

    public String getExecutionErrors(String pServerID, int pLoadID, int pExecID, Date pLastModified);

    public String addJobsAndParams(String pServerID, String xmlFile);

}