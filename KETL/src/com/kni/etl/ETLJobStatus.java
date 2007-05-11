/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import org.w3c.dom.Element;
import com.kni.etl.util.XMLHelper;

/**
 * Insert the type's description here. Creation date: (5/3/2002 1:50:01 PM)
 * 
 * @author: Administrator
 */
public class ETLJobStatus extends ETLStatus {

    private static java.lang.String[] astrStatusMessages = { "Scheduled", "Executing", "Failed, pending closure",
            "Successful", "Ready to run", "Waiting for children", "Failed", "Successful, pending closure",
            "Queued for execution", "Cancelled, pending closure", "Cancelled", "Rejected", "Waiting to be retried",
            "Waiting to be restarted", "Critical failure, pausing load", "Paused", "Waiting to pause",
            "Waiting to skip", "Attempt pause", "Resume", "Skipped, pending closure", "Skipped", "Attempt kill" };
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

    public final static int WAITING_TO_PAUSE = 16;
    public final static int WAITING_TO_SKIP = 17;
    public final static int ATTEMPT_PAUSE = 18;
    public final static int RESUME = 19; // must handle job interrupt & start of job
    public final static int PENDING_CLOSURE_SKIP = 20;
    public final static int SKIPPED = 21;
    public final static int ATTEMPT_CANCEL = 22;

    /**
     * ETLJobStatus constructor comment.
     */
    public ETLJobStatus() {
        super();
        this.setStatusCode(ETLJobStatus.SCHEDULED);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:12:59 AM)
     * 
     * @return java.lang.String[]
     */
    @Override
    public String[] getStatusMessages() {
        return ETLJobStatus.astrStatusMessages;
    }

    Throwable mException;

    public void setException(Throwable e) {
        // prevent duplicate stack trace
        if (this.mException != null && e == this.mException)
            return;

        this.mException = e;
    }

    public Throwable getException() {
        return this.mException;
    }

    public void setStats(String name, int partitions, int partitionID, int recordReaderCount, int recordWriterCount,
            int recordReadErrorCount, int recordWriteErrorCount, long timing) {
        Element e = this.getStatsNode();

        Element step = (Element) XMLHelper.getElementByName(e, "STEP", "NAME", name);

        if (step == null) {
            step = e.getOwnerDocument().createElement("STEP");
            e.appendChild(step);
            step.setAttribute("NAME", name);
            step.setAttribute("PARTITIONS", Integer.toString(partitions));
        }

        Element partition = e.getOwnerDocument().createElement("PARTITION");
        step.appendChild(partition);

        partition.setAttribute("PARTITION", Integer.toString(partitionID));
        partition.setAttribute("READ", Integer.toString(recordReaderCount));
        partition.setAttribute("WRITE", Integer.toString(recordWriterCount));
        partition.setAttribute("READERROR", Integer.toString(recordReadErrorCount));
        partition.setAttribute("WRITEERROR", Integer.toString(recordWriteErrorCount));
        partition.setAttribute("TIMING", Long.toString(timing));

    }

    public void setStats(int recordReaderCount, int recordWriterCount, int recordReadErrorCount,
            int recordWriteErrorCount, long executionTime) {
        Element e = this.getStatsNode();

        e.setAttribute("READ", Integer.toString(recordReaderCount));
        e.setAttribute("WRITE", Integer.toString(recordWriterCount));
        e.setAttribute("READERROR", Integer.toString(recordReadErrorCount));
        e.setAttribute("WRITEERROR", Integer.toString(recordWriteErrorCount));
        e.setAttribute("TIMING", Long.toString(executionTime));

    }

    public void setStats(int statement, int updateCount, long executionTime) {
        Element e = this.getStatsNode();

        Element step = e.getOwnerDocument().createElement("STATEMENT");
        e.appendChild(step);
        step.setAttribute("ID", Integer.toString(statement));
        step.setAttribute("EFFECTEDRECORDS", Integer.toString(updateCount));
        step.setAttribute("TIMING", Long.toString(executionTime));

    }
}
