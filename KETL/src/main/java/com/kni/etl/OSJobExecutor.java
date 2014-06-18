/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl;

import java.io.File;
import java.io.InputStream;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.InputStreamHandler;
import com.kni.util.ExternalJarLoader;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/7/2002 2:26:26 PM)
 * 
 * @author: Administrator
 */
public class OSJobExecutor extends ETLJobExecutor {

  private enum Stage {
    Preparing, About_To_Execute, Executing, Completed, Connecting_STDOUT, Connecting_STDIN
  };

  private Stage stage = Stage.Preparing;

  /** The monitor. */
  private OSJobMonitor monitor;

  private OSJob ojJob;

  private String cmd;


  /**
   * The Class OSJobMonitor.
   */
  private class OSJobMonitor extends Thread {

    /** The alive. */
    boolean alive = true;

    /** The process. */
    public Process process = null;

    private Thread caller;;

    public OSJobMonitor(ETLJob job, Thread caller) {
      this.setName("OS Job Monitor - " + job.getJobID());
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
        boolean killAttempt = false;
        long startTime = System.currentTimeMillis();
        int cnt = 0;
        while (this.alive) {

          Process currentProcess = this.process;
          OSJob job = ojJob;
          if (currentProcess != null && job != null && job.isCancelled()) {
            interruptExecution(currentProcess);
            job.cancelSuccessfull(true);
          }

          if (cnt % 8 == 0 && cnt > 0) {
            long runTime = (System.currentTimeMillis() - startTime) / 1000;
            job.getStatus().setExtendedMessage(
                stage.name().replace("_", " ") + "(" + runTime + "s): "
                    + (cmd == null ? "n/a" : cmd));

            // if stuck in about to execute and 60s have gone by
            // fail command due solaris issue
            if (stage == Stage.About_To_Execute && runTime > 60 && killAttempt == false) {
              killAttempt = true;
              ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                  "Failing OS job, due to unresponsive execution");

              interruptExecution(currentProcess);
            } else if (runTime > job.getTimeout()) {
              String msg = "Job being failed timeout exceeded of " + job.getTimeout() + "s";
              ResourcePool.LogMessage(this.caller, ResourcePool.ERROR_MESSAGE, msg);
              job.getStatus().setExtendedMessage(msg);
              interruptExecution(currentProcess);
            }
          }


          Thread.sleep(500);
          cnt++;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private void interruptExecution(Process currentProcess) throws InterruptedException {
      if (currentProcess == null)
        caller.interrupt();
      else {
        currentProcess.destroy();
        Thread.sleep(5000);
        if (stage != Stage.Completed)
          caller.interrupt();
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

    this.monitor = new OSJobMonitor(jCurrentJob, Thread.currentThread());
    try {
      stage = Stage.Preparing;
      cmd = null;
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

      // Create a File object to define the working directory (if
      // specified)...
      if ((strWorkingDirectory = ojJob.getWorkingDirectory()) != null) {
        // Java 1.4 way - We can't use this because Nick is a girl and
        // wants to stick to 1.2...
        fWorkingDirectory = new File(strWorkingDirectory);
      }

      try {
        String osName = System.getProperty("os.name");
        String strExecStmt;

        cmd = ojJob.getCommandLine();
        if (osName.startsWith("Windows")) {
          strExecStmt = "cmd.exe /c " + cmd;
        } else // assume some UNIX/Linux system
        {
          // strExecStmt = "/bin/sh -c " + ojJob.getCommandLine(); //
          // this is only for script files!
          strExecStmt = cmd;
        }

        if (ojJob.isDebug())
          ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Executing os command: "
              + strExecStmt);

        stage = Stage.About_To_Execute;

        if (fWorkingDirectory != null) {
          pProcess = Runtime.getRuntime().exec(strExecStmt, null, fWorkingDirectory);
        } else {
          pProcess = Runtime.getRuntime().exec(strExecStmt);
        }

        stage = Stage.Connecting_STDOUT;

        this.monitor.process = pProcess;
      } catch (Throwable e) {
        jsJobStatus.setErrorCode(1); // BRIAN: NEED TO SET UP OS JOB
        // ERROR CODES
        jsJobStatus.setErrorMessage("Error running exec(): " + e.getMessage());

        return false;
      }

      // Wait for the process to finish and return the exit code.
      // BRIAN: we should probably do a periodic call to exitStatus() and
      // catch the exception until the
      // process is done. This way, we can terminate the process during a
      // shutdown.
      try {
        StringBuilder inBuffer = new StringBuilder();
        InputStream inStream = pProcess.getInputStream();
        new InputStreamHandler(inBuffer, inStream);

        stage = Stage.Connecting_STDIN;
        StringBuilder errBuffer = new StringBuilder();
        InputStream errStream = pProcess.getErrorStream();
        new InputStreamHandler(errBuffer, errStream);

        stage = Stage.Executing;
        int iReturnValue = pProcess.waitFor();
        stage = Stage.Completed;

        if (inBuffer.length() > 0) {
          jsJobStatus.setExtendedMessage(inBuffer.toString());
        }

        try {
          this.fireJobTriggers(ojJob.iLoadID, ojJob.getJobTriggers(),
              Integer.toString(iReturnValue));
        } catch (Exception e) {
          ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
              "Error firing triggers, check format <EXITCODE>=<VALUE>=(exec|setStatus)(..);... : "
                  + e.getMessage());
        }

        jsJobStatus.setErrorCode(iReturnValue); // Set the return value
        // as the error code


        if (iReturnValue != 0) {
          jsJobStatus.setErrorMessage("STDERROR:" + errBuffer.toString());
          jsJobStatus.setExtendedMessage("STDOUT:" + inBuffer.toString());

          if (iReturnValue == ETLJobStatus.CRITICAL_FAILURE_ERROR_CODE) {
            jsJobStatus.setErrorMessage("Server has been paused\n" + jsJobStatus.getErrorMessage());

            jsJobStatus.setStatusCode(ETLJobStatus.CRITICAL_FAILURE_PAUSE_LOAD);
          }

          bSuccess = false;
        } else {
          if (ojJob.hasWritebackParameter()) {
            this.setWritebackParameter(ojJob.getWritebackParameterListName(),
                ojJob.getWritebackParameterName(), inBuffer.toString().trim());
          }

          jsJobStatus.setStats(-1, System.currentTimeMillis() - start);
        }
      } catch (Exception e) {
        jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP OS JOB
        // ERROR CODES
        jsJobStatus.setErrorMessage("Error in process: " + e.getMessage());
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
    return this.isValidType(jJob) && (jJob instanceof OSJob);
  }

  /**
   * Insert the method's description here. Creation date: (5/7/2002 2:26:26 PM)
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
    // TODO Auto-generated method stub
    return new OSJob();
  }

  /**
   * The main method.
   * 
   * @param args the arguments
   */
  public static void main(String[] args) {
    String ketldir = System.getenv("KETLDIR");
    if (ketldir == null) {
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
          "KETLDIR not set, defaulting to working dir");
      ketldir = ".";
    }

    ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf" + File.separator
        + "Extra.Libraries"), "ketlextralibs", ";");

    ETLJobExecutor.execute(args, new OSJobExecutor(), true);
  }

  @Override
  public ETLJob getCurrentETLJob() {
    return this.ojJob;
  }
}
