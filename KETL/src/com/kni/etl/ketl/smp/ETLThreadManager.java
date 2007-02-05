/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 6, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.w3c.dom.Element;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.writer.DatabaseELTWriter;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class ETLThreadManager {

    ArrayList threads = new ArrayList();

    class WorkerThread {

        Thread thread;
        ETLWorker step;

    }

    private HashSet duplicateCheck = new HashSet();

    KETLJobExecutor mkjExecutor;
    ThreadGroup jobThreadGroup;

    public ETLThreadManager(KETLJobExecutor executor) {
        mkjExecutor = executor;
        jobThreadGroup = new ThreadGroup(executor.getCurrentETLJob().getJobID());

    }

    public void addStep(ETLWorker es) {

        if (duplicateCheck.contains(es))
            return;

        this.addStep(es, es.getClass().getName());
        duplicateCheck.add(es);
    }

    public synchronized ETLWorker getStep(ETLWorker sourceStep, String name) throws KETLThreadException {
        for (Object o : this.threads) {
            WorkerThread wt = (WorkerThread) o;

            if (wt.step.getName().equals(name)) {
                if (wt.step.partitions == 1)
                    return wt.step;

                if (wt.step.partitions == sourceStep.partitions && wt.step.partitionID == sourceStep.partitionID)
                    return wt.step;

                if (wt.step.partitions != sourceStep.partitions) {
                    throw new KETLThreadException(
                            "Cannot get target step if parallism is greater than 1 and does not match source step, check steps Source: "
                                    + sourceStep.getName() + ",Target: " + wt.step.getName(), sourceStep);
                }
            }
        }

        throw new KETLThreadException("Could not find step " + name, sourceStep);
    }

    public ManagedBlockingQueue requestQueue(int queueSize) {
        return new ManagedBlockingQueueImpl(queueSize);
    }

    private void addStep(ETLWorker es, String name) {

        // Create the thread supplying it with the runnable object
        WorkerThread wt = new WorkerThread();
        wt.thread = new Thread(jobThreadGroup, es);
        wt.step = es;
        threads.add(wt);
        wt.thread.setName(es.getName() + ", Type:" + name + " [" + (es.partitionID + 1) + " of " + es.partitions + "]");

    }

    private long startTime, previousTime;
    private int previousReaderRecords = 0, previousWriterRecords = 0;
    public static final String[] flowTypes = { "FANIN", "FANOUT", "PIPELINE" };
    static final int[] flowTypeMappings = { ETLThreadGroup.FANIN, ETLThreadGroup.FANOUT, ETLThreadGroup.PIPELINE };

    public void start() throws KETLThreadException {
        // get start time
        startTime = System.currentTimeMillis();
        previousTime = startTime;

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "- Initializing threads");
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "- Registering queues");
        for (Object o : this.threads) {
            ((WorkerThread) o).step.initializeQueues();
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "- Initializing core managers");
        for (Object o : this.threads) {
            try {
                ((WorkerThread) o).step.initialize(mkjExecutor);
            } catch (Throwable e) {
                if(e instanceof KETLThreadException)
                    throw (KETLThreadException) e;
                throw new KETLThreadException(e.getMessage(), e);
            }
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                "- Compiling and instantiating cores");
        for (Object o : this.threads) {
            ((WorkerThread) o).step.compile();
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "- Starting threads");
        synchronized (this) {
            for (Object o : this.threads) {
                ((WorkerThread) o).thread.start();
            }
        }
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Threads initialized");
    }

    public void monitor(int sleepTime) throws Throwable {
        this.monitor(sleepTime, sleepTime);
    }

    boolean detailed = true;

    public void monitor(int sleepTime, int maxTime) throws Throwable {
        this.monitor(sleepTime, maxTime, null);
    }

    public String finalStatus(ETLJobStatus jsJobStatus) {
        int recordWriterCount = 0, recordReaderCount = 0;
        long currentTime = System.currentTimeMillis();

        for (Object o : this.threads) {
            if (((WorkerThread) o).step instanceof ETLReader) {
                recordReaderCount += ((WorkerThread) o).step.getRecordsProcessed();
            }
            else if (((WorkerThread) o).step instanceof ETLWriter) {
                recordWriterCount += ((WorkerThread) o).step.getRecordsProcessed();
            }
        }

        long allTimeDiff = currentTime - startTime;
        long prevTimeDiff = currentTime - previousTime;
        StringBuilder sb = new StringBuilder("Final Throughput Statistics(Records Per Second)\n");
        int recordDiff = recordReaderCount - previousReaderRecords;
        sb.append("\tOverall Read: " + recordReaderCount / ((allTimeDiff / 1000) + 1) + "\n");

        sb.append("\tAverage Read: " + recordDiff / ((prevTimeDiff / 1000) + 1) + "\n");
        sb.append("\tTotal Records Read: " + recordReaderCount + "\n");

        recordDiff = recordWriterCount - previousWriterRecords;
        sb.append("\tOverall Write: " + recordWriterCount / ((allTimeDiff / 1000) + 1) + "\n");
        sb.append("\tAverage Write: " + recordDiff / ((prevTimeDiff / 1000) + 1) + "\n");
        sb.append("\tTotal Records Written: " + recordWriterCount + "\n");
        sb.append("\tThread Statistics\n\t----------------------------------------\n");
        for (Object o : this.threads) {
            ETLWorker es = ((WorkerThread) o).step;
            sb.append("\t" + ((WorkerThread) o).thread.getName() + ": " + es.getRecordsProcessed() + ", errors: "
                    + ((ETLStep) es).getErrorCount() + ", timing: " + es.getTiming() + "\n");
        }

        jsJobStatus.setExtendedMessage("Total records read: " + recordReaderCount + ", Total records written: "
                + recordWriterCount);

        return sb.toString();
    }

    public static int getThreadingType(Element config) throws KETLThreadException {
        int res = Arrays.binarySearch(flowTypes, XMLHelper.getAttributeAsString(config.getAttributes(), "FLOWTYPE",
                flowTypes[2]));
        if (res < 0)
            throw new KETLThreadException("Invalid flow type, valid values are - " + Arrays.toString(flowTypes), Thread
                    .currentThread());

        return flowTypeMappings[res];
    }

    public static String getStackTrace(Throwable aThrowable) {
        Writer result = new StringWriter();
        PrintWriter printWriter = new PrintWriter(result);
        aThrowable.printStackTrace(printWriter);
        return result.toString();
    }

    public void close(ETLJob eJob) {
        StringBuilder sb = new StringBuilder();
        boolean errorsOccured = false;
        Throwable cause = this.mkjExecutor.getCurrentETLJob().getStatus().getException();

        if (cause != null) {
            sb.append("\n\nCause: " + cause.toString() + "\n" + getStackTrace(cause));
            sb.append("\n\nTrace\n------\n");
        }
        for (Object o : this.threads) {

            WorkerThread wt = (WorkerThread) o;

            if (wt.step != null) {

                wt.step.closeStep(wt.step.success());

                // if errors occured log them to the db and send out an email
                if (!wt.step.success()) {
                    errorsOccured = true;
                    ArrayList a = ((ETLStep) wt.step).getLog();

                    for (int x = 0; x < a.size(); x++) {
                        Object[] tmp = (Object[]) a.get(x);
                        java.util.Date dt = (java.util.Date) tmp[1];
                        String msg = null;
                        String extMsg = "";

                        sb.append("Step - " + ((ETLStep) wt.step).toString() + "\n");

                        if (tmp[0] instanceof Exception) {
                            if (tmp[0] == cause) {
                                sb.append("\t" + x + " - see cause\n\n");
                                continue;
                            }
                            msg = ((Exception) tmp[0]).getMessage();
                            extMsg = "See trace in log";
                        }
                        else if (tmp[0] != null) {
                            msg = tmp[0].toString();
                        }

                        if (msg != null)
                            sb.append("\t" + x + " - [" + dt.toString() + "]" + msg.replace("\t", "\t\t") + "\n\n");

                        if (ResourcePool.getMetadata() != null) {
                            ResourcePool.getMetadata().recordJobMessage(eJob, (ETLStep) wt.step,
                                    eJob.getStatus().getErrorCode(),ResourcePool.ERROR_MESSAGE,  msg, extMsg, false, dt);

                        }
                    }

                }

            }
        }

        if (ResourcePool.getMetadata() != null) {
            if (errorsOccured == true) {
                eJob.getStatus().setExtendedMessage(sb.toString());
            }
        }

    }

    public KETLJobExecutor getJobExecutor() {
        return this.mkjExecutor;
    }

    public void monitor(int sleepTime, int maxTime, ETLJobStatus jsJobStatus) throws Throwable {
        boolean state = true;
        Throwable failureException = null;
        boolean interruptAllThreads = false;
        while (state) {
            state = false;
            int recordWriterCount = 0, recordReaderCount = 0;
            boolean showStatus = false;
            long currentTime = System.currentTimeMillis();

            for (Object o : this.threads) {

                // if any thread has failed then force failure for alive threads, they should be failing already but
                // interrupt are sometimes missed
                if (interruptAllThreads == false) {
                    if ((interruptAllThreads = ((WorkerThread) o).step.failAll())) {
                        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                                "Critical job failure all steps being interrupted");
                    }
                }

                if (((WorkerThread) o).thread.isAlive()) {
                    state = true;
                    if (interruptAllThreads)
                        ((WorkerThread) o).thread.interrupt();
                }
                else {
                    if (((WorkerThread) o).step.success() == false) {
                        failureException = this.mkjExecutor.getCurrentETLJob().getStatus().getException();
                        if (failureException == null) {
                            failureException = new KETLThreadException(
                                    "Unknown failure, exception not received from step "
                                            + ((WorkerThread) o).step.getName(), this);
                            this.mkjExecutor.getCurrentETLJob().getStatus().setException(failureException);
                            this.mkjExecutor.getCurrentETLJob().getStatus().setErrorCode(-1);
                        }
                    }

                    // if thread shutdown wasn't clean then interrupt all threads
                    if (interruptAllThreads == false && ((WorkerThread) o).step.cleanShutdown() == false) {
                        interruptAllThreads = true;
                        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                                "Critical job failure all steps being interrupted");
                    }
                }
                if (currentTime - previousTime > 10000) {
                    showStatus = true;
                    if (((WorkerThread) o).step instanceof ETLReader) {
                        recordReaderCount += ((WorkerThread) o).step.getRecordsProcessed();
                    }
                    else if (((WorkerThread) o).step instanceof ETLWriter) {
                        recordWriterCount += ((WorkerThread) o).step.getRecordsProcessed();
                    }
                }
            }

            if (showStatus) {
                long allTimeDiff = currentTime - startTime;
                long prevTimeDiff = currentTime - previousTime;

                if (jsJobStatus == null) {
                    StringBuilder sb = new StringBuilder("Current Throughput Statistics(Records Per Second)\n");
                    int recordDiff = recordReaderCount - previousReaderRecords;
                    sb.append("\tOverall Read: " + recordReaderCount / (allTimeDiff / 1000) + "\n");

                    sb.append("\tAverage Read: " + recordDiff / (prevTimeDiff / 1000) + "\n");
                    sb.append("\tTotal Records Read: " + recordReaderCount + "\n");

                    recordDiff = recordWriterCount - previousWriterRecords;
                    sb.append("\tOverall Write: " + recordWriterCount / (allTimeDiff / 1000) + "\n");
                    sb.append("\tAverage Write: " + recordDiff / (prevTimeDiff / 1000) + "\n");
                    sb.append("\tTotal Records Written: " + recordWriterCount + "\n");

                    sb.append("\tThread Statistics\n\t----------------------------------------\n");
                    for (Object o : this.threads) {
                        ETLWorker es = ((WorkerThread) o).step;
                        if (es.isWaiting())
                            sb.append("\t" + ((WorkerThread) o).thread.getName() + ": Waiting for " + es.waitingFor()
                                    + "\n");

                        else
                            sb.append("\t"
                                    + ((WorkerThread) o).thread.getName()
                                    + ": "
                                    + es.getRecordsProcessed()
                                    + ", errors: "
                                    + ((ETLStep) es).getErrorCount()
                                    + (es.getTiming() == null || es.getTiming().equals("N/A") ? "" : ", timing: "
                                            + es.getTiming())
                                    + (((WorkerThread) o).thread.isAlive() ? "" : ", Complete") + "\n");
                    }
                    if (this.mkjExecutor != null && this.mkjExecutor.getCurrentETLJob() != null) {
                        this.mkjExecutor.getCurrentETLJob().getStatus().setExtendedMessage(sb.toString());
                    }
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, sb.toString());
                }
                else {
                    String waiting = "";
                    for (Object o : this.threads) {
                        ETLWorker es = ((WorkerThread) o).step;
                        if (es.isWaiting())
                            waiting = ", " + ((WorkerThread) o).thread.getName() + ": Waiting for " + es.waitingFor();
                    }
                    jsJobStatus.setExtendedMessage("Records read: " + recordReaderCount + ", Records written: "
                            + recordWriterCount + waiting);
                }
                previousTime = currentTime;
                previousReaderRecords = recordReaderCount;
                previousWriterRecords = recordWriterCount;
                showStatus = false;

            }

            Thread.sleep(sleepTime);

            if (sleepTime < maxTime)
                sleepTime += sleepTime;
            else if (sleepTime > maxTime)
                sleepTime = maxTime;

        }

        failureException = this.mkjExecutor.getCurrentETLJob().getStatus().getException();

        if (failureException != null)
            throw failureException;

    }

    public int countOfStepThreadsAlive(ETLStep writer) {
        int cnt = 0;
        for (Object o : this.threads) {
            WorkerThread wrk = (WorkerThread) o;

            if (wrk.thread.isAlive() && wrk.step.mstrName.endsWith(writer.getName()))
                cnt++;
        }
        return cnt;
    }
}
