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

import org.w3c.dom.Element;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/3/2002 1:50:01 PM)
 * 
 * @author: Administrator
 */
public class ETLJobStatus extends ETLStatus {

    /** The astr status messages. */
    private static java.lang.String[] astrStatusMessages = { "Scheduled", "Executing", "Failed, pending closure",
            "Successful", "Ready to run", "Waiting for children", "Failed", "Successful, pending closure",
            "Queued for execution", "Cancelled, pending closure", "Cancelled", "Rejected", "Waiting to be retried",
            "Waiting to be restarted", "Critical failure, pausing load", "Paused", "Waiting to pause",
            "Waiting to skip", "Attempt pause", "Resume", "Skipped, pending closure", "Skipped", "Attempt kill" };
    
    /** The Constant SCHEDULED. */
    public final static int SCHEDULED = 0;
    
    /** The Constant WAITING_FOR_CHILDREN. */
    public final static int WAITING_FOR_CHILDREN = 5;
    
    /** The Constant READY_TO_RUN. */
    public final static int READY_TO_RUN = 4; // Previously called WAITING_TO_BE_EXECUTED
    
    /** The Constant QUEUED_FOR_EXECUTION. */
    public final static int QUEUED_FOR_EXECUTION = 8;
    
    /** The Constant EXECUTING. */
    public final static int EXECUTING = 1;
    
    /** The Constant PENDING_CLOSURE_SUCCESSFUL. */
    public final static int PENDING_CLOSURE_SUCCESSFUL = 7; // Previously called PENDING_CLOSURE_FINISHED
    
    /** The Constant SUCCESSFUL. */
    public final static int SUCCESSFUL = 3; // Previously called FINISHED
    
    /** The Constant PENDING_CLOSURE_FAILED. */
    public final static int PENDING_CLOSURE_FAILED = 2;
    
    /** The Constant FAILED. */
    public final static int FAILED = 6;
    
    /** The Constant PENDING_CLOSURE_CANCELLED. */
    public final static int PENDING_CLOSURE_CANCELLED = 9;
    
    /** The Constant CANCELLED. */
    public final static int CANCELLED = 10;
    
    /** The Constant REJECTED. */
    public final static int REJECTED = 11; // Wrong type of job for executor
    
    /** The Constant WAITING_TO_BE_RETRIED. */
    public final static int WAITING_TO_BE_RETRIED = 12;
    
    /** The Constant RESTART. */
    public final static int RESTART = 13;
    
    /** The Constant CRITICAL_FAILURE_PAUSE_LOAD. */
    public final static int CRITICAL_FAILURE_PAUSE_LOAD = 14;
    
    /** The Constant PAUSED. */
    public final static int PAUSED = 15;
    
    /** The Constant DO_NOT_SEND_EMAIL_ERROR_CODE. */
    public final static int DO_NOT_SEND_EMAIL_ERROR_CODE = 99;
    
    /** The Constant CRITICAL_FAILURE_ERROR_CODE. */
    public final static int CRITICAL_FAILURE_ERROR_CODE = 100;

    /** The Constant WAITING_TO_PAUSE. */
    public final static int WAITING_TO_PAUSE = 16;
    
    /** The Constant WAITING_TO_SKIP. */
    public final static int WAITING_TO_SKIP = 17;
    
    /** The Constant ATTEMPT_PAUSE. */
    public final static int ATTEMPT_PAUSE = 18;
    
    /** The Constant RESUME. */
    public final static int RESUME = 19; // must handle job interrupt & start of job
    
    /** The Constant PENDING_CLOSURE_SKIP. */
    public final static int PENDING_CLOSURE_SKIP = 20;
    
    /** The Constant SKIPPED. */
    public final static int SKIPPED = 21;
    
    /** The Constant ATTEMPT_CANCEL. */
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

    /** The m exception. */
    Throwable mException;

    /**
     * Sets the exception.
     * 
     * @param e the new exception
     */
    public void setException(Throwable e) {
        // prevent duplicate stack trace
        if (this.mException != null && e == this.mException)
            return;

        this.mException = e;
    }

    /**
     * Gets the exception.
     * 
     * @return the exception
     */
    public Throwable getException() {
        return this.mException;
    }

    /**
     * Sets the stats.
     * 
     * @param name the name
     * @param partitions the partitions
     * @param partitionID the partition ID
     * @param recordReaderCount the record reader count
     * @param recordWriterCount the record writer count
     * @param recordReadErrorCount the record read error count
     * @param recordWriteErrorCount the record write error count
     * @param timing the timing
     */
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

    /**
     * Sets the stats.
     * 
     * @param recordReaderCount the record reader count
     * @param recordWriterCount the record writer count
     * @param recordReadErrorCount the record read error count
     * @param recordWriteErrorCount the record write error count
     * @param executionTime the execution time
     */
    public void setStats(int recordReaderCount, int recordWriterCount, int recordReadErrorCount,
            int recordWriteErrorCount, long executionTime) {
        Element e = this.getStatsNode();

        e.setAttribute("READ", Integer.toString(recordReaderCount));
        e.setAttribute("WRITE", Integer.toString(recordWriterCount));
        e.setAttribute("READERROR", Integer.toString(recordReadErrorCount));
        e.setAttribute("WRITEERROR", Integer.toString(recordWriteErrorCount));
        e.setAttribute("TIMING", Long.toString(executionTime));

    }

    /**
     * Sets the stats.
     * 
     * @param statement the statement
     * @param updateCount the update count
     * @param executionTime the execution time
     */
    public void setStats(int statement, int updateCount, long executionTime) {
        Element e = this.getStatsNode();

        Element step = e.getOwnerDocument().createElement("STATEMENT");
        e.appendChild(step);
        step.setAttribute("ID", Integer.toString(statement));
        step.setAttribute("EFFECTEDRECORDS", Integer.toString(updateCount));
        step.setAttribute("TIMING", Long.toString(executionTime));

    }
}
