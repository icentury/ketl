/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

/**
 * Insert the type's description here. Creation date: (5/3/2002 1:50:01 PM)
 * 
 * @author: Administrator
 */
public class ETLJobStatus extends ETLStatus {

    private static java.lang.String[] astrStatusMessages = { "Scheduled", "Executing", "Failed, pending closure",
            "Successful", "Ready to run", "Waiting for children", "Failed", "Successful, pending closure",
            "Queued for execution", "Cancelled, pending closure", "Cancelled", "Rejected", "Waiting to be retried",
            "Waiting to be restarted", "Critical failure, pausing load", "Paused" };
    public final static int SCHEDULED = 0;
    public final static int WAITING_FOR_CHILDREN = 5;
    public final static int READY_TO_RUN = 4; // Previously called WAITING_TO_BE_EXECUTED
    public final static int QUEUED_FOR_EXECUTION = 8;
    public final static int EXECUTING = 1;
    public final static int PENDING_CLOSURE_SUCCESSFUL = 7; // Previously called PENDING_CLOSURE_FINISHED
    public final static int SUCCESSFUL = 3; // Previously called FINISHED
    public final static int PENDING_CLOSURE_FAILED = 2;
    public final static int FAILED = 6;
    public final static int PENDING_CLOSURE_CANCELLED = 9;
    public final static int CANCELLED = 10;
    public final static int REJECTED = 11; // Wrong type of job for executor
    public final static int WAITING_TO_BE_RETRIED = 12;
    public final static int RESTART = 13;
    public final static int CRITICAL_FAILURE_PAUSE_LOAD = 14;
    public final static int PAUSED = 15;
    public final static int DO_NOT_SEND_EMAIL_ERROR_CODE = 99;
    public final static int CRITICAL_FAILURE_ERROR_CODE = 100;

    /**
     * ETLJobStatus constructor comment.
     */
    public ETLJobStatus() {
        super();
        setStatusCode(SCHEDULED);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:12:59 AM)
     * 
     * @return java.lang.String[]
     */
    public String[] getStatusMessages() {
        return astrStatusMessages;
    }

    Exception mException;

    public void setException(Exception e) {
        // prevent duplicate stack trace
        if (this.mException != null && e == mException)
            return;

        mException = e;        
    }

    public Exception getException() {
        return mException;
    }
}
