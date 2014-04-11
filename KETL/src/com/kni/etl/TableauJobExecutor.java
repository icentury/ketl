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

import java.io.File;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.InputStreamHandler;
import com.kni.util.ExternalJarLoader;
import com.kni.util.tableau.ServerConnector;
import com.kni.util.tableau.ServerConnector.TableauResponse;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/7/2002 2:26:26 PM)
 * 
 * @author: Administrator
 */
public class TableauJobExecutor extends ETLJobExecutor {

	private enum Stage {
		Preparing, Connecting_to_Server, Executing, Completed
	};

	private Stage stage = Stage.Preparing;

	/** The monitor. */
	private TableauJobMonitor monitor;

	private TableauJob ojJob;

	/**
	 * The Class OSJobMonitor.
	 */
	private class TableauJobMonitor extends Thread {

		/** The alive. */
		boolean alive = true;

		/** The process. */
		public Process process = null;

		private Thread caller;;

		public TableauJobMonitor(ETLJob job, Thread caller) {
			this.setName("Tableau Job Monitor - " + job.getJobID());
			this.caller = caller;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			try {
				while (this.alive) {
					TableauJob job = ojJob;
					if (job != null && job.isCancelled()) {
						caller.interrupt();
						job.cancelSuccessfull(true);
					}
					Thread.sleep(500);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 2:54:55
	 * PM)
	 */
	public TableauJobExecutor() {
		super();
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 2:26:26
	 * PM)
	 * 
	 * @param jCurrentJob
	 *            the j current job
	 * 
	 * @return boolean
	 */
	@Override
	protected boolean executeJob(ETLJob jCurrentJob) {
		boolean bSuccess = true;

		this.monitor = new TableauJobMonitor(jCurrentJob,
				Thread.currentThread());
		try {
			stage = Stage.Preparing;
			this.monitor.start();
			ETLStatus jsJobStatus;
			long start = (System.currentTimeMillis() - 1);
			// Only accept OS jobs...
			if ((jCurrentJob instanceof TableauJob) == false) {
				return false;
			}

			this.ojJob = (TableauJob) jCurrentJob;
			jsJobStatus = ojJob.getStatus();

			ServerConnector client = new ServerConnector();

			try {
				if (ojJob.isDebug())
					ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE,
							"Executing tableau refresh: " + ojJob.toString());

				stage = Stage.Connecting_to_Server;
				client.authenticate(ojJob.getServerAddress(),
						ojJob.getUsername(), ojJob.getPassword());

			} catch (Throwable e) {
				jsJobStatus.setErrorCode(1); // BRIAN: NEED TO SET UP OS JOB
				// ERROR CODES
				jsJobStatus.setErrorMessage("Error running exec(): "
						+ e.getMessage());

				return false;
			}

			// Wait for the process to finish and return the exit code.
			// BRIAN: we should probably do a periodic call to exitStatus() and
			// catch the exception until the
			// process is done. This way, we can terminate the process during a
			// shutdown.
			try {

				stage = Stage.Executing;

				ojJob.initRefresh();
				TableauResponse response = client.refreshExtract(
						ojJob.getObjectProject(), ojJob.getType(),
						ojJob.getObjectName(), ojJob.getSynchronous());

				int iReturnValue = response.success()?0:response.exitCode();

				stage = Stage.Completed;

				try {
					this.fireJobTriggers(ojJob.iLoadID, ojJob.getJobTriggers(),
							Integer.toString(iReturnValue));
				} catch (Exception e) {
					ResourcePool
							.LogMessage(
									Thread.currentThread(),
									ResourcePool.ERROR_MESSAGE,
									"Error firing triggers, check format <EXITCODE>=<VALUE>=(exec|setStatus)(..);... : "
											+ e.getMessage());
				}

				jsJobStatus.setErrorCode(iReturnValue); // Set the return value
				// as the error code

				if (!response.success()) {
					jsJobStatus.setErrorMessage("STDERROR:"
							+ response.errorMessage());
					jsJobStatus.setExtendedMessage("STDOUT:"
							+ response.message());

					if (iReturnValue == ETLJobStatus.CRITICAL_FAILURE_ERROR_CODE) {
						jsJobStatus.setErrorMessage("Server has been paused\n"
								+ jsJobStatus.getErrorMessage());

						jsJobStatus
								.setStatusCode(ETLJobStatus.CRITICAL_FAILURE_PAUSE_LOAD);
					}

					bSuccess = false;
				} else
					jsJobStatus
							.setStats(-1, System.currentTimeMillis() - start);
			} catch (Exception e) {
				ResourcePool.logException(e);
				jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP OS JOB
				// ERROR CODES
				jsJobStatus.setErrorMessage("Error in process: "
						+ e.getMessage());
				bSuccess = false;
			}
		} finally {
			this.stage = Stage.Completed;
			this.monitor.alive = false;
			if (this.monitor.process != null)
				this.monitor.process.destroy();
			this.ojJob = null;
		}

		return bSuccess;
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 2:26:26
	 * PM)
	 * 
	 * @return true, if initialize
	 */
	@Override
	protected boolean initialize() {
		return true;
	}

	/**
	 * Insert the method's description here. Creation date: (5/8/2002 2:50:03
	 * PM)
	 * 
	 * @param jJob
	 *            the j job
	 * 
	 * @return boolean
	 */
	@Override
	public boolean supportsJobType(ETLJob jJob) {
		// Only accept OS jobs...
		return this.isValidType(jJob) && (jJob instanceof OSJob);
	}

	/**
	 * Insert the method's description here. Creation date: (5/7/2002 2:26:26
	 * PM)
	 * 
	 * @return true, if terminate
	 */
	@Override
	protected boolean terminate() {
		// No need to do anything here until we're asyncronously running
		// executables with a polling mechanism...
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ETLJobExecutor#getNewJob()
	 */
	@Override
	public ETLJob getNewJob() throws Exception {
		return new TableauJob();
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {
		String ketldir = System.getenv("KETLDIR");
		if (ketldir == null) {
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.WARNING_MESSAGE,
					"KETLDIR not set, defaulting to working dir");
			ketldir = ".";
		}

		ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf"
				+ File.separator + "Extra.Libraries"), "ketlextralibs", ";");

		ETLJobExecutor.execute(args, new TableauJobExecutor(), true);
	}

	@Override
	public ETLJob getCurrentETLJob() {
		return this.ojJob;
	}
}
