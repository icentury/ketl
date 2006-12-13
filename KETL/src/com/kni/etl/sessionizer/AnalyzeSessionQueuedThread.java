/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jun 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.sessionizer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import com.kni.etl.ETLJobStatus;
import com.kni.etl.KETLJobStatus;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.ketl_v1.ResultRecord;


/**
 * @author nwakefield
 * Creation Date: Jun 17, 2003
 */
public class AnalyzeSessionQueuedThread extends Thread implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 3257570611449442873L;
    public final static int RESTART_SESSIONS_NO_STORE = 1;
    public final static int DISABLE_RESTART_NO_STORE = 2;
    public final static int DISABLE_RESTART_AND_STORE = 3;
    public final static int RESTART_SESSIONS_AND_STORE = 4;
    public final static ResultRecord END_MARKER = new ResultRecord();
    public final static ResultRecord START_MARKER = new ResultRecord();
    transient SessionizationWriterRoot pageViewWriter = null;
    transient SessionizationWriterRoot sessionWriter = null;
    transient XMLSessionizeJob sjJob = null;
    transient public IDCounter idCounter = null;
    transient public IDCounter iBadSessions = null;
    transient PageViewItemFinderAccelerator pageViewItemAccelerator = null;
    transient public Thread ParentThread = null;
    transient public PageParserPageDefinition[] pDef = null;
    transient public SessionDefinition sessionDef = null;
    transient KETLJobStatus mJobStatus = null;
    public int iBatchSize = 5000;
    public int closeOutMode = DISABLE_RESTART_AND_STORE;
    transient public boolean shutdown = false;
    transient public boolean fatalError = false;
    public int dmLoadID = -1;
    public int loadID = -1;
    public int waitQueueSize = 100;
    private BlockingQueue DataQueue;
    boolean mbPagesOnly = false;
    transient BlockingQueue pageViewQueue = null;
    transient BlockingQueue sessionQueue = null;
    SessionBuilder sBuilder = null;
    ResultRecord first = null;
    int mPartitionID;
    int mStepID;

    /**
     *
     */
    public AnalyzeSessionQueuedThread(ETLJobStatus esJobStatus, int pPartitionIdentifier)
    {
        super();

        setJobStatus(esJobStatus);
        setPartitionIdentifier(pPartitionIdentifier);
        setName();
    }

    public void setJobStatus(ETLJobStatus esJobStatus)
    {
        if (esJobStatus instanceof KETLJobStatus)
        {
            this.mJobStatus = (KETLJobStatus) esJobStatus;
            this.mStepID = mJobStatus.generateStepIdentifier();
        }
    }

    public void setPartitionIdentifier(int pPartitionIdentifier)
    {
        this.mPartitionID = pPartitionIdentifier;
    }

    public void setName()
    {
        super.setName(this.getName() + "(" + this.getClass().getName() + ")");
    }

    public void setJob(XMLSessionizeJob pJob)
    {
        this.sjJob = pJob;

        mbPagesOnly = pJob.mbPagesOnly;

        String pWriterClass = sjJob.getSessionWriterClass();

        if (pWriterClass != null)
        {
            sessionWriter = this.getWriter(pWriterClass);
        }
        else
        {
            sessionWriter = new SessionDatabaseWriter();
        }

        pWriterClass = sjJob.getHitWriterClass();

        if (pWriterClass != null)
        {
            pageViewWriter = this.getWriter(pWriterClass);
        }
        else
        {
            pageViewWriter = new HitDatabaseWriter();
        }

        pageViewQueue = sjJob.getHitWriterDataQueue(XMLSessionizeJob.WRITER, this);
        sessionQueue = sjJob.getSessionWriterDataQueue(XMLSessionizeJob.WRITER, this);

        sjJob.addSessionDataQueueReader(this.sessionWriter);
        sjJob.addHitDataQueueReader(this.pageViewWriter);
    }

    SessionizationWriterRoot getWriter(String pClassName)
    {
        SessionizationWriterRoot writer = null;

        if (pClassName != null)
        {
            try
            {
                Class cStepClass = Class.forName(pClassName);
                writer = (SessionizationWriterRoot) cStepClass.newInstance();
                writer.setStatusObject(this.mJobStatus);
                writer.setPartitionIdentifier(this.mPartitionID);

                return writer;
            }
            catch (Exception e)
            {
                this.mJobStatus.setErrorMessage("Error initializing AnalyzeSessionThread (" + pClassName +
                    ").  Unable to instantiate class.");

                return null;
            }
        }

        return null;
    }

    private boolean analyzeHit() throws Exception
    {
        boolean insertsHappened = false;
        ResultRecord resultRecord;
        Session foundSession;

        // get next record
        resultRecord = (ResultRecord) DataQueue.take();

        if ((resultRecord == END_MARKER))
        {
            shutdown = true;
        }
        else
        {
            try
            {
                foundSession = sBuilder.analyzeHit(resultRecord);
            }
            catch (KETLException e)
            {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,e.toString());

                return false;
            }

            if ((foundSession == null) && (sBuilder.ignoreHit == false))
            {
                outputBadSession(resultRecord);
            }

            // get the page details
            if (foundSession != null)
            {
                insertsHappened = true;

                // increments hits for session
                foundSession.Hit++;

                PageView pageView = new PageView();
                pageView.ItemFinderAccelerator = pageViewItemAccelerator;

                // keep reference to last page
                foundSession.lastHit = pageView;

                ResultRecord tmpRecord = null;

                try
                {
                    tmpRecord = (ResultRecord) resultRecord.clone();
                }
                catch (Exception ee)
                {
                    ResourcePool.LogException(ee, this);
                }

                if (first == null)
                {
                    first = tmpRecord;
                }

                // lets not write the pageview to the session instead put it straight to the writer
                pageView.LineFields = tmpRecord.LineFields;
                pageView.SourceLine = tmpRecord.SourceLine;
                pageView.OverallLine = tmpRecord.OverallLine;
                pageView.SourceFile = tmpRecord.SourceFile;
                pageView.setSessionID(foundSession.getID());
                pageView.Session = foundSession;

                // if a queue occurs then there maybe a pageview writer error
                if (pageViewWriter.fatalError)
                {
                    ResourcePool.LogMessage(this, "Fatal error writing page views");

                    return false;
                }

                pageViewQueue.put(pageView);
            }
            else
            {
                insertsHappened = false;
            }
        }

        if (insertsHappened == true)
        {
            // only print message every 20 batches
            if (this.isLoggingOn())
            {
                ResourcePool.LogMessage(this,
                    "AnalyzeHit (" + Thread.currentThread().toString() + "): Queued = " + DataQueue.size());
            }
        }

        return true;
    }

    /**
     * @param resultRecord
     */
    private void outputBadSession(ResultRecord resultRecord)
    {
        iBadSessions.incrementID();
        System.out.println("Invalid session:" + resultRecord.SourceFile + " Line:" + resultRecord.SourceLine);

        for (int i = 0; i < resultRecord.LineFields.length; i++)
        {
            if ((resultRecord.LineFields[i] != null) && (resultRecord.LineFields[i].isNull() == false))
            {
                System.out.print(resultRecord.LineFields[i].getValAsString() + " - ");
            }
            else
            {
                System.out.print(" - ");
            }
        }

        System.out.println();
    }

    private boolean isLoggingOn()
    {
        return false;
    }

    void initializeWriters()
    {
        sessionWriter.setJob(sjJob);
        sessionWriter.setSkipInserts(sjJob.skipInserts());
        sessionWriter.LoadID = loadID;
        sessionWriter.DMLoadID = dmLoadID;
        sessionWriter.WriterName = "BG-Session";
        sessionWriter.setColumnMaps(sjJob.getSessionColumnMaps());
        sessionWriter.setSQLHint(sjJob.getSessionSQLHint());
        sessionWriter.setTable(sjJob.getSessionTable());
        sessionWriter.BatchSize = sjJob.getBatchCommitSize();
        sessionWriter.setDataQueue(sBuilder.getDoneSessionsQueue());
        sessionWriter.setDatabase(XMLSessionizeJob.SESSION_CONNECTION);

        pageViewWriter.setJob(sjJob);
        pageViewWriter.setSkipInserts(sjJob.skipInserts());
        pageViewWriter.LoadID = loadID;
        pageViewWriter.DMLoadID = dmLoadID;
        pageViewWriter.WriterName = "BG-PageView";
        pageViewWriter.PagesOnly = this.mbPagesOnly;
        pageViewWriter.setPageParserDefinitions(pDef);
        pageViewWriter.setColumnMaps(sjJob.getHitColumnMaps());
        pageViewWriter.setSQLHint(sjJob.getHitSQLHint());
        pageViewWriter.setTable(sjJob.getHitTable());
        pageViewWriter.BatchSize = sjJob.getBatchCommitSize();
        pageViewWriter.setDataQueue(pageViewQueue);
        pageViewWriter.setDatabase(XMLSessionizeJob.HIT_CONNECTION);
    }

    public void run()
    {
        try
        {
            sessionDef = sjJob.getSessionDefinition();
            pDef = sjJob.getPageParserDefinition();

            // if this is a restart then reconfigure the session builder
            if (sBuilder == null)
            {
                sBuilder = new SessionBuilder(loadID, dmLoadID, idCounter, sessionQueue);
            }
            else
            {
                sBuilder.setLoadIDs(loadID, dmLoadID);
                sBuilder.setIDCounter(idCounter);
                sBuilder.setWaitQueueSize(sessionQueue);
                sBuilder.preLoadWebServerSettings();
            }

            this.initializeWriters();

            sBuilder.setSessionWriterThread(sessionWriter);
            sBuilder.setPagesOnly(pDef, this.mbPagesOnly, sjJob.isHitCountRequired());
            sBuilder.setSessionDefinition(sessionDef);

            sessionWriter.start();
            pageViewWriter.start();

            if (pageViewItemAccelerator == null)
            {
                pageViewItemAccelerator = new PageViewItemFinderAccelerator();
            }

            while (shutdown == false)
            {
                if (analyzeHit() == false)
                {
                    shutdown = true;
                    fatalError = true;
                    DataQueue.clear();
                }
            }

            // shutdown the pageview writer, do not serialize until all pages written
            if (pageViewWriter != null)
            {
                sjJob.releaseHitWriterDataQueue(this);

                if (sjJob.noMoreHitWriters())
                {
                    while (sjJob.noMoreHitReaders() == false)
                    {
                        if (this.fatalError)
                        {
                            pageViewWriter.getDataQueue().clear();
                        }

                        pageViewWriter.getDataQueue().put(END_MARKER);
                        sleep(100);
                    }
                }

                waitForShutdown(pageViewWriter);

                if (pageViewWriter.fatalError)
                {
                    ResourcePool.LogMessage(this, "Fatal error writing page views");

                    fatalError = true;
                }
            }

            // ready to serialize
            mReadyToSerialize = true;

            // wait for serialization to finish
            if ((fatalError == false) && sjJob.restartSessions())
            {
                synchronized (this)
                {
                    this.wait();
                }
            }

            // mark remaining sessions done or still open
            if ((sBuilder != null) && (fatalError == false))
            {
                if (closeOutMode == DISABLE_RESTART_AND_STORE)
                {
                    // store last hit date last activity
                    sBuilder.closeOutAllSessions(false);
                }
                else if (closeOutMode == RESTART_SESSIONS_AND_STORE)
                { // store null for last activity, makes identification of non closed
                  // sessions easier
                    sBuilder.closeOutAllSessions(true);
                }
            }

            // wait for session writer thread to shutdown
            if (sessionWriter != null)
            {
                sjJob.releaseSessionWriterDataQueue(this);

                if (sjJob.noMoreSessionWriters())
                {
                    while (sjJob.noMoreSessionReaders() == false)
                    {
                        if (this.fatalError)
                        {
                            sessionWriter.getDataQueue().clear();
                        }

                        sessionWriter.getDataQueue().put(END_MARKER);
                        sleep(100);
                    }
                }

                waitForShutdown(sessionWriter);

                if (sessionWriter.fatalError)
                {
                    ResourcePool.LogMessage(this, "Fatal error writing sessions");

                    fatalError = true;
                }
            }
        }
        catch (Exception e)
        {
            sessionWriter.interrupt();
            pageViewWriter.interrupt();
            ResourcePool.LogException(e, this);
        }
    }

    private void waitForShutdown(Thread pThread) throws InterruptedException
    {
        boolean threadsAlive = true;

        do
        {
            threadsAlive = false;

            if (pThread.isAlive())
            {
                threadsAlive = true;
                Thread.sleep(25);
            }
        }
        while (threadsAlive);
    }

    transient boolean mReadyToSerialize = false;

    public boolean readyToSerialize()
    {
        return mReadyToSerialize;
    }

    public void setDataQueue(BlockingQueue newDataQueue)
    {
        DataQueue = newDataQueue;
    }

    public BlockingQueue getDataQueue()
    {
        return DataQueue;
    }

    public int getSessionDataQueueSize()
    {
        return this.sessionQueue.size();
    }

    public int getPageViewDataQueueSize()
    {
        return this.pageViewQueue.size();
    }

    private void writeObject(ObjectOutputStream s) throws IOException
    {
        s.defaultWriteObject();
    }

    private void readObject(ObjectInputStream s) throws IOException
    {
        try
        {
            s.defaultReadObject();
            this.fatalError = false;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }
}
