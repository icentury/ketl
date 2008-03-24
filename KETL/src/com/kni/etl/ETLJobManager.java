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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.kni.etl.dbutils.ResourcePool;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/1/2002 4:53:44 PM)
 * 
 * @author: Administrator
 */
public class ETLJobManager {

	/** The mjms status. */
	protected ETLJobManagerStatus mjmsStatus;

	/** The mll pending queue. */
	protected LinkedBlockingQueue mllPendingQueue = null;

	/** The cje executor threads. */
	protected ArrayList cjeExecutorThreads;

	/** The mstr job executor class. */
	protected java.lang.String mstrJobExecutorClass = null;

	/** The mi max queue size. */
	protected int miMaxQueueSize;

	/** The mi max num threads. */
	protected int miMaxNumThreads;

	/** The mb shutdown. */
	protected boolean mbShutdown = false;

	private String msType;

	/**
	 * ETLJobExecutor constructor comment.
	 * 
	 * @param strJobExecutorClass
	 *            the str job executor class
	 * @param iMaxNumThreads
	 *            The max num threads
	 * @param iMaxQueueSize
	 *            The max queue size
	 */
	public ETLJobManager(String strJobExecutorClass, int iMaxNumThreads, int iMaxQueueSize, String type) {
		super();
		this.mjmsStatus = new ETLJobManagerStatus();
		this.mstrJobExecutorClass = strJobExecutorClass;
		this.cjeExecutorThreads = new ArrayList(iMaxNumThreads);
		if(iMaxQueueSize<1)
			iMaxQueueSize = 1;
		
		this.mllPendingQueue = new LinkedBlockingQueue(iMaxQueueSize);
		this.miMaxNumThreads = iMaxNumThreads;
		this.miMaxQueueSize = iMaxQueueSize;
		this.msType = type;

		this.mjmsStatus.setStatusCode(ETLJobManagerStatus.INITIALIZING); // We'll
																			// set
																			// this
																			// to
																			// READY
																			// once
																			// we
																			// have
																			// a
		// thread
		// available

		if (this.startExecutorThreads() == 0) {
			this.mjmsStatus.setStatusCode(ETLJobManagerStatus.ERROR);
		}
	}

	/**
	 * Insert the method's description here. Creation date: (5/3/2002 4:59:38
	 * PM)
	 * 
	 * @param ejJob
	 *            the ej job
	 * 
	 * @return boolean
	 */
	public boolean cancelJob(ETLJob ejJob) {
		// Remove it from the queue...if it's not there, then the worker thread
		// must have already pulled it...
		this.mllPendingQueue.remove(ejJob);

		// Mark it as cancelled in case anyone's working on it...
		return ejJob.cancelJob();
	}

	/**
	 * Runs through the threads to see if they are still running... Creation
	 * date: (5/7/2002 10:30:20 AM)
	 * 
	 * @return number of running threads (should be same as size of collection
	 *         after we're done checking)
	 */
	protected int[] checkExecutorThreads() {
		int[] aiStatusCounts = null;

		// Check if each thread is still alive and mark it's status...
		for (int i = 0; i < this.cjeExecutorThreads.size(); i++) {
			ETLJobExecutor je = (ETLJobExecutor) this.cjeExecutorThreads.get(i);

			// If the thread is dead, remove it from our collection...
			if (je.isAlive() == false) {
				this.cjeExecutorThreads.remove(je);

				// Reset our index to one back, so when our for loop increments,
				// we'll be pointing at the right next element...
				i--;

				continue;
			}

			// Update our status counts...
			// First create the array if we haven't already - we needed to find
			// an instance of an executor thread
			// to determine the number of states...(they should all be the same
			// class, so it's ok to just look at one)
			if (aiStatusCounts == null) {
				aiStatusCounts = new int[je.getStatus().getStatusMessages().length];
			}

			aiStatusCounts[je.getStatus().getStatusCode()]++;
		}

		// If all our threads are dead, return null...
		if (this.cjeExecutorThreads.size() == 0) {
			return null;
		}

		return aiStatusCounts;
	}

	/**
	 * Runs through the threads to see if they are still running... Creation
	 * date: (5/7/2002 10:30:20 AM)
	 * 
	 * @return number of running threads (should be same as size of collection
	 *         after we're done checking)
	 */
	public void kill() {
		// Check if each thread is still alive and kill it if it is...
		for (int i = 0; i < this.cjeExecutorThreads.size(); i++) {
			ETLJobExecutor je = (ETLJobExecutor) this.cjeExecutorThreads.get(i);

			// If the thread is dead, remove it from our collection...
			if (je.isAlive() == false) {
				je.interrupt();
			}
		}
	}

	/**
	 * Insert the method's description here. Creation date: (5/3/2002 3:01:31
	 * PM)
	 * 
	 * @return int
	 */
	public int getCurrentJobCount() {
		return this.getCurrentQueueSize() + this.getCurrentWorkingThreadCount();
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 9:35:25
	 * AM)
	 * 
	 * @return int
	 */
	public int getCurrentQueueSize() {
		return this.mllPendingQueue.size();

	}

	/**
	 * This function runs through the thread collection and checks status.
	 * Creation date: (5/7/2002 9:36:38 AM)
	 * 
	 * @return int
	 */
	public int getCurrentWorkingThreadCount() {
		int[] aiStatusArray;

		if ((aiStatusArray = this.checkExecutorThreads()) == null) {
			return 0;
		}

		return aiStatusArray[ETLJobExecutorStatus.WORKING];
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 5:43:10
	 * PM)
	 * 
	 * @return the job executor class name
	 */
	public String getJobExecutorClassName() {
		return this.mstrJobExecutorClass;
	}
	
	public String getJobType() {
		return this.msType;
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 6:02:15
	 * PM)
	 * 
	 * @return int
	 */
	public int getMaxNumThreads() {
		return this.miMaxNumThreads;
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 9:39:00
	 * AM)
	 * 
	 * @return int
	 */
	public int getMaxQueueSize() {
		return this.miMaxQueueSize;
	}

	/**
	 * Return a live count of the number of threads currently running...
	 * Creation date: (5/8/2002 5:44:44 PM)
	 * 
	 * @return int
	 */
	public int getNumThreads() {
		this.checkExecutorThreads();

		// The size of the collection is the number of happy threads...
		return this.cjeExecutorThreads.size();
	}

	/**
	 * Do not keep a reference to this status object if you are not running the
	 * manager in a different thread... ...you will need to run updateStatus()
	 * to refresh the status. Creation date: (5/2/2002 1:47:48 PM)
	 * 
	 * @return int
	 */
	public ETLJobManagerStatus getStatus() {
		this.updateStatus();

		return this.mjmsStatus;
	}

	/**
	 * Note that this will not affect jobs currently in the queue. It is meant
	 * to create a new limit. Creation date: (5/7/2002 9:39:00 AM)
	 * 
	 * @param newMaxQueueSize
	 *            int
	 */
	public void setMaxQueueSize(int newMaxQueueSize) {
		this.miMaxQueueSize = newMaxQueueSize;
	}

	/**
	 * Insert the method's description here. Creation date: (5/9/2002 10:09:11
	 * AM)
	 * 
	 * @return boolean
	 */
	public boolean shutdown() {
		this.mbShutdown = true;

		// Set the shutdown flag on each thread...
		for (int i = 0; i < this.cjeExecutorThreads.size(); i++) {
			ETLJobExecutor je = (ETLJobExecutor) this.cjeExecutorThreads.get(i);

			// If the thread is dead, remove it from our collection...
			if (je.isAlive() == false) {
				this.cjeExecutorThreads.remove(je);

				// Reset our index to one back, so when our for loop increments,
				// we'll be pointing at the right next element...
				i--;

				continue;
			}

			je.shutdown();
		}

		// If there were no threads to shut down, return false, but otherwise,
		// our work here is done...
		if (this.cjeExecutorThreads.size() == 0) {
			return false;
		}

		return true;
	}

	/**
	 * Insert the method's description here. Creation date: (5/4/2002 8:06:24
	 * PM)
	 * 
	 * @return the number of threads started
	 */
	protected int startExecutorThreads() {
		ETLJobExecutor jeExecutorThread = null;

		// Starting at miNumThreads allows us to restart to our max if
		// desired...
		for (int i = this.cjeExecutorThreads.size(); i < this.miMaxNumThreads; i++) {
			try {
				jeExecutorThread = (ETLJobExecutor) Class.forName(this.mstrJobExecutorClass).newInstance();
			} catch (Exception e) {
				// Keep storing the last error message, since it's likely to be
				// the same for all threads...
				this.mjmsStatus.setErrorCode(1);
				this.mjmsStatus.setErrorMessage("Error starting thread for class '" + this.mstrJobExecutorClass + "': "
						+ e.getMessage());

				continue;
			}

			jeExecutorThread.setType(this.msType);
			jeExecutorThread.setPendingQueue(this.mllPendingQueue);
			jeExecutorThread.start();
			this.cjeExecutorThreads.add(jeExecutorThread); // Add the thread to
															// our collection
		}

		return this.cjeExecutorThreads.size(); // Number of active threads
	}

	/**
	 * Insert the method's description here. Creation date: (5/3/2002 2:58:47
	 * PM)
	 * 
	 * @param ejNewJob
	 *            the ej new job
	 * 
	 * @return boolean
	 */
	public boolean submitJob(ETLJob ejNewJob) {
		
		if(this.mllPendingQueue.remainingCapacity()==0)
			return false;
		
		// Don't accept jobs if we're in a bad state...
		switch (this.getStatus().getStatusCode()) {
		case ETLJobManagerStatus.ERROR:
		case ETLJobManagerStatus.FULL:
			return false;
		}

		// Make sure that the job executor supports this type of job...(just use
		// the first object
		// we have - they should all return the same answer...
		if (((ETLJobExecutor) this.cjeExecutorThreads.get(0)).supportsJobType(ejNewJob) == false) {
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
		return this.mllPendingQueue.add(ejNewJob);

	}

	/**
	 * Creation date: (5/2/2002 1:47:48 PM).
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
		int iSubmittedJobs; // = iNumJobsInQueue + number of threads in WORKING
							// state
		int iAvailableCapacity; // = (iMaxJobsInQueue - iNumJobsInQueue) +
								// number of threads in READY state
		int iImmediateCapacity; // = number of threads in READY state -
								// iNumJobsInQueue

		synchronized (this.mllPendingQueue) {
			// Get all of our current thread statuses...
			aiNumThreadsAtStatus = this.checkExecutorThreads();

			// Any dead threads will be pulled from the collection, so that size
			// is the current thread count...
			iNumThreads = this.cjeExecutorThreads.size();

			if ((aiNumThreadsAtStatus == null) && (iNumThreads > 0)) {
				ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
						"No status array, but there are still threads in the collection...");
				aiNumThreadsAtStatus = this.checkExecutorThreads();
			}

			// Take a snapshot of the number of jobs in queue...
			iNumJobsInQueue = this.getCurrentQueueSize();

			// Our max doesn't change much, but get a snapshot here anyways...
			iMaxJobsInQueue = this.getMaxQueueSize();
		}

		// Check for special states TERMINATED, SHUTTING_DOWN or ERROR, since
		// they are computed differently than the
		// others...

		/** ** TERMINATED *** */

		// If we are already terminated, there's no coming back...
		if (this.mjmsStatus.getStatusCode() == ETLJobManagerStatus.TERMINATED) {
			return ETLJobManagerStatus.TERMINATED;
		}

		// If we were shutting down, and the threads are finally all put to bed
		// (or dead), then we can terminate too...
		if (this.mbShutdown) {
			// If no more threads, we're shut down...
			if (iNumThreads == 0) {
				return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.TERMINATED);
			}

			/** ** SHUTTING_DOWN *** */

			// Else, we're still shutting down...
			return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.SHUTTING_DOWN);
		}

		/** ** ERROR *** */

		// If we are not shutting down or terminated, and there are no more
		// living threads in our pool, that's bad...
		// NOTE: this check should be done before any peeking in the thread
		// status array, since it will be null
		if (iNumThreads == 0) {
			return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.ERROR);
		}

		// Now, perform the logic to determine our status (we have already taken
		// the trouble states out of the way to
		// make
		// our math computations easier)...
		// Derive our stats for clearer logic...
		iTotalCapacity = iMaxJobsInQueue + iNumThreads;
		iSubmittedJobs = iNumJobsInQueue + aiNumThreadsAtStatus[ETLJobExecutorStatus.WORKING];
		iAvailableCapacity = (iMaxJobsInQueue - iNumJobsInQueue) + aiNumThreadsAtStatus[ETLJobExecutorStatus.READY];
		iImmediateCapacity = aiNumThreadsAtStatus[ETLJobExecutorStatus.READY] - iNumJobsInQueue;

		/** ** INITIALIZING *** */

		// If we have threads, but none are done initializing, then the status
		// of the manager is INITIALIZING...
		if (aiNumThreadsAtStatus[ETLJobExecutorStatus.INITIALIZING] == iNumThreads) {
			return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.INITIALIZING);
		}

		/** ** READY *** */

		// Check to see if we have the ability to run a job immediately
		// (READY)...
		if (iImmediateCapacity > 0) {
			return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.READY);
		}

		/** ** QUEUEING *** */

		// If we weren't READY, but we still have available capacity, then we
		// must have some space left on the queue...
		if (iAvailableCapacity > 0) {
			return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.QUEUEING);
		}

		/** ** FULL *** */

		// Check to see if we're at (or past) maximum capacity...
		if (iSubmittedJobs >= iTotalCapacity) {
			return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.FULL);
		}

		// We should never get to this state...so there must be an error in our
		// counts. The safest thing to do here is
		// to signal FULL and wait for the jobs to move around... but set the
		// warning flag...
		this.mjmsStatus.setWarningCode(1);
		this.mjmsStatus
				.setWarningMessage("Unexpected state derived when checking job and executor status.  Temporarily setting to FULL.");

		return this.mjmsStatus.setStatusCode(ETLJobManagerStatus.FULL);
	}
}
