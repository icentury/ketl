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
import com.kni.etl.util.InputStreamHandler;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/7/2002 2:26:26 PM)
 * 
 * @author: Administrator
 */
public class OSJobExecutor extends ETLJobExecutor {

    /** The monitor. */
    private OSJobMonitor monitor;
	private OSJob ojJob;

    /**
     * The Class OSJobMonitor.
     */
    private class OSJobMonitor extends Thread {

        /** The alive. */
        boolean alive = true;
        
        /** The current job. */
        OSJob currentJob = null;
        
        /** The process. */
        public Process process = null;;

        /* (non-Javadoc)
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            try {
                while (this.alive) {

                    if (this.process != null && this.currentJob != null && this.currentJob.isCancelled()) {
                        this.process.destroy();
                        this.currentJob.cancelSuccessfull(true);
                    }
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:54:55 PM)
     */
    public OSJobExecutor() {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:26:26 PM)
     * 
     * @param jCurrentJob the j current job
     * 
     * @return boolean
     */
    @Override
    protected boolean executeJob(ETLJob jCurrentJob) {
        boolean bSuccess = true;
        this.monitor = new OSJobMonitor();
        try {
            this.monitor.start();

            ETLStatus jsJobStatus;
            String strWorkingDirectory;
            Process pProcess = null;
            File fWorkingDirectory = null;
            long start = (System.currentTimeMillis() - 1);
            // Only accept OS jobs...
            if ((jCurrentJob instanceof OSJob) == false) {
                return false;
            }

            this.ojJob = (OSJob) jCurrentJob;
            jsJobStatus = ojJob.getStatus();

            // Create a File object to define the working directory (if specified)...
            if ((strWorkingDirectory = ojJob.getWorkingDirectory()) != null) {
                // Java 1.4 way - We can't use this because Nick is a girl and wants to stick to 1.2...
                fWorkingDirectory = new File(strWorkingDirectory);
            }

            try {
                String osName = System.getProperty("os.name");
                String strExecStmt;

                if (osName.startsWith("Windows")) {
                    strExecStmt = "cmd.exe /c " + ojJob.getCommandLine();
                }
                else // assume some UNIX/Linux system
                {
                    // strExecStmt = "/bin/sh -c " + ojJob.getCommandLine(); // this is only for script files!
                    strExecStmt = ojJob.getCommandLine();
                }

                if (fWorkingDirectory != null) {
                    pProcess = Runtime.getRuntime().exec(strExecStmt, null, fWorkingDirectory);
                }
                else {
                    pProcess = Runtime.getRuntime().exec(strExecStmt);
                }

                this.monitor.process = pProcess;
            } catch (Exception e) {
                jsJobStatus.setErrorCode(1); // BRIAN: NEED TO SET UP OS JOB ERROR CODES
                jsJobStatus.setErrorMessage("Error running exec(): " + e.getMessage());

                return false;
            }

            // Wait for the process to finish and return the exit code.
            // BRIAN: we should probably do a periodic call to exitStatus() and catch the exception until the
            // process is done. This way, we can terminate the process during a shutdown.
            try {
                StringBuffer inBuffer = new StringBuffer();
                InputStream inStream = pProcess.getInputStream();
                new InputStreamHandler(inBuffer, inStream);

                StringBuffer errBuffer = new StringBuffer();
                InputStream errStream = pProcess.getErrorStream();
                new InputStreamHandler(errBuffer, errStream);

                int iReturnValue = pProcess.waitFor();

                if (inBuffer.length() > 0) {
                    jsJobStatus.setExtendedMessage(inBuffer.toString());
                }

                jsJobStatus.setErrorCode(iReturnValue); // Set the return value as the error code

                if (iReturnValue != 0) {
                    jsJobStatus.setErrorMessage("STDERROR:" + errBuffer.toString());
                    jsJobStatus.setExtendedMessage("STDOUT:" + inBuffer.toString());

                    if (iReturnValue == ETLJobStatus.CRITICAL_FAILURE_ERROR_CODE) {
                        jsJobStatus.setErrorMessage("Server has been paused\n" + jsJobStatus.getErrorMessage());

                        jsJobStatus.setStatusCode(ETLJobStatus.CRITICAL_FAILURE_PAUSE_LOAD);
                    }

                    bSuccess = false;
                }
                else
                    jsJobStatus.setStats(-1, System.currentTimeMillis() - start);
            } catch (Exception e) {
                jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP OS JOB ERROR CODES
                jsJobStatus.setErrorMessage("Error in process: " + e.getMessage());
                return false;
            }
        } finally {
            this.monitor.alive = false;
            this.ojJob = null;
        }

        return bSuccess;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:26:26 PM)
     * 
     * @return true, if initialize
     */
    @Override
    protected boolean initialize() {
        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 2:50:03 PM)
     * 
     * @param jJob the j job
     * 
     * @return boolean
     */
    @Override
    public boolean supportsJobType(ETLJob jJob) {
        // Only accept OS jobs...
        return (jJob instanceof OSJob);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:26:26 PM)
     * 
     * @return true, if terminate
     */
    @Override
    protected boolean terminate() {
        // No need to do anything here until we're asyncronously running executables with a polling mechanism...
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLJobExecutor#getNewJob()
     */
    @Override
	public ETLJob getNewJob() throws Exception {
        // TODO Auto-generated method stub
        return new OSJob();
    }

    /**
     * The main method.
     * 
     * @param args the arguments
     */
    public static void main(String[] args) {
        ETLJobExecutor.execute(args, new OSJobExecutor(), true);
    }

	@Override
	public ETLJob getCurrentETLJob() {
		return this.ojJob;
	}
}
