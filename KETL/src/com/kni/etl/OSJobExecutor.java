/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.File;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.InputStreamHandler;

/**
 * Insert the type's description here. Creation date: (5/7/2002 2:26:26 PM)
 * 
 * @author: Administrator
 */
public class OSJobExecutor extends ETLJobExecutor {

    private OSJobMonitor monitor;

    private class OSJobMonitor extends Thread {

        boolean alive = true;
        OSJob currentJob = null;
        public Process process = null;;

        @Override
        public void run() {
            try {
                while (alive) {

                    if (process != null && currentJob != null && currentJob.isCancelled()) {
                        process.destroy();
                        currentJob.cancelSuccessfull(true);
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
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    protected boolean executeJob(ETLJob jCurrentJob) {
        OSJob ojJob;
        boolean bSuccess = true;
        this.monitor = new OSJobMonitor();
        try {
            this.monitor.start();

            ETLJobStatus jsJobStatus;
            String strWorkingDirectory;
            Process pProcess = null;
            File fWorkingDirectory = null;

            // Only accept OS jobs...
            if ((jCurrentJob instanceof OSJob) == false) {
                return false;
            }

            ojJob = (OSJob) jCurrentJob;
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
            } catch (Exception e) {
                jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP OS JOB ERROR CODES
                jsJobStatus.setErrorMessage("Error in process: " + e.getMessage());
                return false;
            }
        } finally {
            this.monitor.alive = false;
        }

        return bSuccess;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:26:26 PM)
     */
    protected boolean initialize() {
        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 2:50:03 PM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    public boolean supportsJobType(ETLJob jJob) {
        // Only accept OS jobs...
        return (jJob instanceof OSJob);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:26:26 PM)
     */
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
    protected ETLJob getNewJob() throws Exception {
        // TODO Auto-generated method stub
        return new OSJob();
    }

    public static void main(String[] args) {
        ETLJobExecutor.execute(args, new OSJobExecutor(), true);
    }
}
