/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

/**
 * Insert the type's description here. Creation date: (5/3/2002 1:13:01 PM)
 * 
 * @author: Administrator
 */
public class ETLJobManagerStatus extends ETLStatus {

    private static java.lang.String[] astrStatusMessages = { "Initializing", "Ready", "Queuing", "Full", "Error",
            "Shutting Down", "Terminated" };
    public final static int INITIALIZING = 0; // Starting up threads
    public final static int READY = 1; // At least one thread available for immediate job processing
    public final static int QUEUEING = 2; // All threads busy, but there is space in the queue for more jobs
    public final static int FULL = 3; // All threads busy and the queue is at it's maximum
    public final static int ERROR = 4; // Unable to start up, or all threads are dead
    public final static int SHUTTING_DOWN = 5; // Shutting down threads
    public final static int TERMINATED = 6; // Done shutting down threads

    /**
     * ETLJobExecutorStatus constructor comment.
     */
    public ETLJobManagerStatus() {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:13:31 AM)
     * 
     * @return java.lang.String[]
     */
    public String[] getStatusMessages() {
        return astrStatusMessages;
    }
}
