/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

/**
 * Insert the type's description here. Creation date: (5/7/2002 10:50:31 AM)
 * 
 * @author: Administrator
 */
public class ETLJobExecutorStatus extends ETLStatus {

    private static java.lang.String[] astrStatusMessages = { "Initializing", "Ready", "Working", "Error",
            "Shutting Down", "Terminated" };
    public final static int INITIALIZING = 0; // Starting up
    public final static int READY = 1; // Ready to receive a job
    public final static int WORKING = 2; // Currently processing a job
    public final static int ERROR = 3; // Error
    public final static int SHUTTING_DOWN = 4; // Shutting down
    public final static int TERMINATED = 5; // Shutdown complete

    /**
     * ETLJobExecutorStatus constructor comment.
     */
    public ETLJobExecutorStatus() {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:07:53 AM)
     * 
     * @return java.lang.String[]
     */
    @Override
    public String[] getStatusMessages() {
        return ETLJobExecutorStatus.astrStatusMessages;
    }
}
