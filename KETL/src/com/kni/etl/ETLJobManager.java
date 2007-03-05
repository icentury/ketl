/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.util.ArrayList;
import java.util.LinkedList;

import com.kni.etl.dbutils.ResourcePool;

/**
 * Insert the type's description here. Creation date: (5/1/2002 4:53:44 PM)
 * 
 * @author: Administrator
 */
public class ETLJobManager {

    protected ETLJobManagerStatus mjmsStatus;
    protected LinkedList mllPendingQueue = null;
    protected ArrayList cjeExecutorThreads;
    protected java.lang.String mstrJobExecutorClass = null;
    protected int miMaxQueueSize;
    protected int miMaxNumThreads;
    protected boolean mbShutdown = false;

    /**
     * ETLJobExecutor constructor comment.
     */
    public ETLJobManager(String strJobExecutorClass) {
        // Default contructor should have one thread and never have items waiting in queue...
        this(strJobExecutorClass, 1, 0);
    }

    /**
     * ETLJobExecutor constructor comment.
     */
    public ETLJobManager(String strJobExecutorClass, int iMaxNumThreads, int iMaxQueueSize) {
        super();
        mjmsStatus = new ETLJobManagerStatus();
        mstrJobExecutorClass = strJobExecutorClass;
        cjeExecutorThreads = new ArrayList(iMaxNumThreads);
        mllPendingQueue = new LinkedList();
        miMaxNumThreads = iMaxNumThreads;
        miMaxQueueSize = iMaxQueueSize;

        mjmsStatus.setStatusCode(ETLJobManagerStatus.INITIALIZING); // We'll set this to READY once we have a thread
                                                                    // available

        if (startExecutorThreads() == 0) {
            mjmsStatus.setStatusCode(ETLJobManagerStatus.ERROR);
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 4:59:38 PM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    public boolean cancelJob(ETLJob ejJob) {
        synchronized (mllPendingQueue) {
            // Remove it from the queue...if it's not there, then the worker thread must have already pulled it...
            mllPendingQueue.remove(ejJob);
        }

        // Mark it as cancelled in case anyone's working on it...
        return ejJob.cancelJob();
    }

    /**
     * Runs through the threads to see if they are still running... Creation date: (5/7/2002 10:30:20 AM)
     * 
     * @return number of running threads (should be same as size of collection after we're done checking)
     */
    protected int[] checkExecutorThreads() {
        int[] aiStatusCounts = null;

        // Check if each thread is still alive and mark it's status...
        for (int i = 0; i < cjeExecutorThreads.size(); i++) {
            ETLJobExecutor je = (ETLJobExecutor) cjeExecutorThreads.get(i);

            // If the thread is dead, remove it from our collection...
            if (je.isAlive() == false) {
                cjeExecutorThreads.remove(je);

                // Reset our index to one back, so when our for loop increments,
                // we'll be pointing at the right next element...
                i--;

                continue;
            }

            // Update our status counts...
            // First create the array if we haven't already - we needed to find an instance of an executor thread
            // to determine the number of states...(they should all be the same class, so it's ok to just look at one)
            if (aiStatusCounts == null) {
                aiStatusCounts = new int[je.getStatus().getStatusMessages().length];
            }

            aiStatusCounts[je.getStatus().getStatusCode()]++;
        }

        // If all our threads are dead, return null...
        if (cjeExecutorThreads.size() == 0) {
            return null;
        }

        return aiStatusCounts;
    }

    /**
     * Runs through the threads to see if they are still running... Creation date: (5/7/2002 10:30:20 AM)
     * 
     * @return number of running threads (should be same as size of collection after we're done checking)
     */
    public void kill() {
        // Check if each thread is still alive and kill it if it is...
        for (int i = 0; i < cjeExecutorThreads.size(); i++) {
            ETLJobExecutor je = (ETLJobExecutor) cjeExecutorThreads.get(i);

            // If the thread is dead, remove it from our collection...
            if (je.isAlive() == false) {
                je.interrupt();
            }
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 3:01:31 PM)
     * 
     * @return int
     */
    public int getCurrentJobCount() {
        return getCurrentQueueSize() + getCurrentWorkingThreadCount();
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 9:35:25 AM)
     * 
     * @return int
     */
    public int getCurrentQueueSize() {
        synchronized (mllPendingQueue) {
            return mllPendingQueue.size();
        }
    }

    /**
     * This function runs through the thread collection and checks status. Creation date: (5/7/2002 9:36:38 AM)
     * 
     * @return int
     */
    public int getCurrentWorkingThreadCount() {
        int[] aiStatusArray;

        if ((aiStatusArray = checkExecutorThreads()) == null) {
            return 0;
        }

        return aiStatusArray[ETLJobExecutorStatus.WORKING];
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 5:43:10 PM)
     */
    public String getJobExecutorClassName() {
        return mstrJobExecutorClass;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 6:02:15 PM)
     * 
     * @return int
     */
    public int getMaxNumThreads() {
        return miMaxNumThreads;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 9:39:00 AM)
     * 
     * @return int
     */
    public int getMaxQueueSize() {
        return miMaxQueueSize;
    }

    /**
     * Return a live count of the number of threads currently running... Creation date: (5/8/2002 5:44:44 PM)
     * 
     * @return int
     */
    public int getNumThreads() {
        checkExecutorThreads();

        // The size of the collection is the number of happy threads...
        return cjeExecutorThreads.size();
    }

    /**
     * Do not keep a reference to this status object if you are not running the manager in a different thread... ...you
     * will need to run updateStatus() to refresh the status. Creation date: (5/2/2002 1:47:48 PM)
     * 
     * @return int
     */
    public ETLJobManagerStatus getStatus() {
        updateStatus();

        return mjmsStatus;
    }

    /**
     * Note that this will not affect jobs currently in the queue. It is meant to create a new limit. Creation date:
     * (5/7/2002 9:39:00 AM)
     * 
     * @param newMaxQueueSize int
     */
    public void setMaxQueueSize(int newMaxQueueSize) {
        miMaxQueueSize = newMaxQueueSize;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 10:09:11 AM)
     * 
     * @return boolean
     */
    public boolean shutdown() {
        mbShutdown = true;

        // Set the shutdown flag on each thread...
        for (int i = 0; i < cjeExecutorThreads.size(); i++) {
            ETLJobExecutor je = (ETLJobExecutor) cjeExecutorThreads.get(i);

            // If the thread is dead, remove it from our collection...
            if (je.isAlive() == false) {
                cjeExecutorThreads.remove(je);

                // Reset our index to one back, so when our for loop increments,
                // we'll be pointing at the right next element...
                i--;

                continue;
            }

            je.shutdown();
        }

        // If there were no threads to shut down, return false, but otherwise, our work here is done...
        if (cjeExecutorThreads.size() == 0) {
            return false;
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 8:06:24 PM)
     * 
     * @return the number of threads started
     */
    protected int startExecutorThreads() {
        ETLJobExecutor jeExecutorThread = null;

        // Starting at miNumThreads allows us to restart to our max if desired...
        for (int i = cjeExecutorThreads.size(); i < miMaxNumThreads; i++) {
            try {
                jeExecutorThread = (ETLJobExecutor) Class.forName(mstrJobExecutorClass).newInstance();
            } catch (Exception e) {
                // Keep storing the last error message, since it's likely to be the same for all threads...
                mjmsStatus.setErrorCode(1);
                mjmsStatus.setErrorMessage("Error starting thread for class '" + mstrJobExecutorClass + "': "
                        + e.getMessage());

                continue;
            }

            jeExecutorThread.setPendingQueue(mllPendingQueue);
            jeExecutorThread.start();
            cjeExecutorThreads.add(jeExecutorThread); // Add the thread to our collection
        }

        return cjeExecutorThreads.size(); // Number of active threads
    }

    /**
     * Insert the method's description here. Creation date: (5/3/2002 2:58:47 PM)
     * 
     * @return boolean
     * @param ejJob com.kni.etl.ETLJob
     */
    public boolean submitJob(ETLJob ejNewJob) {
        // Don't accept jobs if we're in a bad state...
        switch (getStatus().getStatusCode()) {
        case ETLJobManagerStatus.ERROR:
        case ETLJobManagerStatus.FULL:
            return false;
        }

        // Make sure that the job executor supports this type of job...(just use the first object
        // we have - they should all return the same answer...
        if (((ETLJobExecutor) cjeExecutorThreads.get(0)).supportsJobType(ejNewJob) == false) {
            ejNewJob.getStatus().setStatusCode(ETLJobStatus.REJECTED);

            return false;
        }

        // Double check to make sure that the job isn't in a weird state...
        if (ejNewJob.getStatus().getStatusCode() != ETLJobStatus.READY_TO_RUN) {
            return false;
        }

        // Set the new status of the job...
        ejNewJob.getStatus().setStatusCode(ETLJobStatus.QUEUED_FOR_EXECUTION);

        // Add the job to the queue...
        synchronized (mllPendingQueue) {
            return mllPendingQueue.add(ejNewJob);
        }
    }

    /**
     * Creation date: (5/2/2002 1:47:48 PM)
     * 
     * @return current status code
     */
    public int updateStatus() {
        // Core stats...
        int[] aiNumThreadsAtStatus;
        int iNumThreads;
        int iNumJobsInQueue;
        int iMaxJobsInQueue;

        // Derived stats...
        int iTotalCapacity; // = iMaxJobsInQueue + iNumThreads
        int iSubmittedJobs; // = iNumJobsInQueue + number of threads in WORKING state
        int iAvailableCapacity; // = (iMaxJobsInQueue - iNumJobsInQueue) + number of threads in READY state
        int iImmediateCapacity; // = number of threads in READY state - iNumJobsInQueue

        synchronized (mllPendingQueue) {
            // Get all of our current thread statuses...
            aiNumThreadsAtStatus = checkExecutorThreads();

            // Any dead threads will be pulled from the collection, so that size is the current thread count...
            iNumThreads = cjeExecutorThreads.size();

            if ((aiNumThreadsAtStatus == null) && (iNumThreads > 0)) {
                ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                        "No status array, but there are still threads in the collection...");
                aiNumThreadsAtStatus = checkExecutorThreads();
            }

            // Take a snapshot of the number of jobs in queue...
            iNumJobsInQueue = getCurrentQueueSize();

            // Our max doesn't change much, but get a snapshot here anyways...
            iMaxJobsInQueue = getMaxQueueSize();
        }

        // Check for special states TERMINATED, SHUTTING_DOWN or ERROR, since they are computed differently than the
        // others...

        /** ** TERMINATED *** */

        // If we are already terminated, there's no coming back...
        if (mjmsStatus.getStatusCode() == ETLJobManagerStatus.TERMINATED) {
            return ETLJobManagerStatus.TERMINATED;
        }

        // If we were shutting down, and the threads are finally all put to bed (or dead), then we can terminate too...
        if (mbShutdown) {
            // If no more threads, we're shut down...
            if (iNumThreads == 0) {
                return mjmsStatus.setStatusCode(ETLJobManagerStatus.TERMINATED);
            }

            /** ** SHUTTING_DOWN *** */

            // Else, we're still shutting down...
            return mjmsStatus.setStatusCode(ETLJobManagerStatus.SHUTTING_DOWN);
        }

        /** ** ERROR *** */

        // If we are not shutting down or terminated, and there are no more living threads in our pool, that's bad...
        // NOTE: this check should be done before any peeking in the thread status array, since it will be null
        if (iNumThreads == 0) {
            return mjmsStatus.setStatusCode(ETLJobManagerStatus.ERROR);
        }

        // Now, perform the logic to determine our status (we have already taken the trouble states out of the way to
        // make
        // our math computations easier)...
        // Derive our stats for clearer logic...
        iTotalCapacity = iMaxJobsInQueue + iNumThreads;
        iSubmittedJobs = iNumJobsInQueue + aiNumThreadsAtStatus[ETLJobExecutorStatus.WORKING];
        iAvailableCapacity = (iMaxJobsInQueue - iNumJobsInQueue) + aiNumThreadsAtStatus[ETLJobExecutorStatus.READY];
        iImmediateCapacity = aiNumThreadsAtStatus[ETLJobExecutorStatus.READY] - iNumJobsInQueue;

        /** ** INITIALIZING *** */

        // If we have threads, but none are done initializing, then the status of the manager is INITIALIZING...
        if (aiNumThreadsAtStatus[ETLJobExecutorStatus.INITIALIZING] == iNumThreads) {
            return mjmsStatus.setStatusCode(ETLJobManagerStatus.INITIALIZING);
        }

        /** ** READY *** */

        // Check to see if we have the ability to run a job immediately (READY)...
        if (iImmediateCapacity > 0) {
            return mjmsStatus.setStatusCode(ETLJobManagerStatus.READY);
        }

        /** ** QUEUEING *** */

        // If we weren't READY, but we still have available capacity, then we must have some space left on the queue...
        if (iAvailableCapacity > 0) {
            return mjmsStatus.setStatusCode(ETLJobManagerStatus.QUEUEING);
        }

        /** ** FULL *** */

        // Check to see if we're at (or past) maximum capacity...
        if (iSubmittedJobs >= iTotalCapacity) {
            return mjmsStatus.setStatusCode(ETLJobManagerStatus.FULL);
        }

        // We should never get to this state...so there must be an error in our counts. The safest thing to do here is
        // to signal FULL and wait for the jobs to move around... but set the warning flag...
        mjmsStatus.setWarningCode(1);
        mjmsStatus
                .setWarningMessage("Unexpected state derived when checking job and executor status.  Temporarily setting to FULL.");

        return mjmsStatus.setStatusCode(ETLJobManagerStatus.FULL);
    }
}
