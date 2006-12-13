/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import com.kni.etl.ETLJobStatus;
import com.kni.etl.KETLJobStatus;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;
import com.kni.etl.urltools.URLCleaner;


public abstract class SessionizationWriterRoot extends Thread
{
    int BatchCntTmp = 0;
    public int BatchSize = 2000;
    BlockingQueue DataQueue;
    SimpleDateFormat DateTimeFormatter;
    Object dbConnection = null;
    public boolean fatalError = false;
    public int DMLoadID = -1;
    protected String JDBCDriver;
    protected String JDBCURL;
    public int LoadID = -1;
    boolean LogginOn = false;
    transient PageParserPageDefinition[] PageParserDefinitions;
    protected String Password;
    protected String PreSQL;
    int RowsInserted = 0;
    public Object run = null;
    boolean SkipInserts = false;
    boolean shutdown = false;
    public boolean PagesOnly = false;
    int SleepTime = 1000;
    protected String Username;
    public String WriterName = null;
    public boolean currentlyWaiting = false;
    transient public Object waiting;
    transient protected KETLJobStatus mJobStatus = null;
    transient protected XMLSessionizeJob mJob = null;
    int mStepID;
    int mPartitionIdentifier;
    int[] maHitParameterPosition = new int[256];
    int[] maHitParameterSize = new int[256];
    static final int UNKNOWN_LENGTH = 512;
    static final int LOAD_ID = 0;
    static final int DM_LOAD_ID = 1;
    static final int TEMP_SESSION_ID = 2;
    static final int FIRST_CLICK_SESSION_IDENTIFIER = 3;
    static final int PERSISTANT_IDENTIFIER = 4;
    static final int MAIN_SESSION_IDENTIFIER = 5;
    static final int IP_ADDRESS = 6;
    static final int REFERRER = 7;
    static final int FIRST_SESSION_ACTIVITY = 8;
    static final int LAST_SESSION_ACTIVITY = 9;
    static final int BROWSER = 10;
    static final int REPEAT_VISITOR = 11;
    static final int HITS = 12;
    static final int PAGEVIEWS = 13;
    static final int KEEP_VARIABLES = 14;
    static final int START_PERSISTANT_IDENTIFIER = 15;
    static final int DNS_ADDRESS = 16;
    static final int DNS_ADDRESS_TRANSLATION_LEVEL = 17;
    static final int SOURCE_FILE = 18;
    static final int CUSTOMFIELD1 = 19;
    static final int CUSTOMFIELD2 = 20;
    static final int CUSTOMFIELD3 = 21;
    static final int SERVER_NAME = 22;
    static final int PAGE_SEQUENCE = 23;
    static final int ACTIVITY_DT = 3;
    static final int GET_REQUEST = 4;
    static final int STATUS = 5;
    static final int BYTES_SENT = 6;
    static final int TIME_TAKEN_TO_SERV_REQUEST = 7;
    static final int CANONICAL_SERVER_PORT = 8;
    static final int REFERRER_URL = 9;
    static final int CLEANSED = 10;
    static final int ASSOCIATED_HITS = 11;
    static final int CLEANSED_ID = 12;
    static final String[] ValidSessionColumnNames = 
        {
            "LOAD_ID", "DM_LOAD_ID", "TEMP_SESSION_ID", "FIRST_CLICK_SESSION_IDENTIFIER", "PERSISTANT_IDENTIFIER",
            "MAIN_SESSION_IDENTIFIER", "IP_ADDRESS", "REFERRER", "FIRST_SESSION_ACTIVITY", "LAST_SESSION_ACTIVITY",
            "BROWSER", "REPEAT_VISITOR", "HITS", "PAGEVIEWS", "KEEP_VARIABLES", "START_PERSISTANT_IDENTIFIER",
            "DNS_ADDRESS", "DNS_ADDRESS_TRANSLATION_LEVEL", "SOURCE_FILE", "CUSTOMFIELD1", "CUSTOMFIELD2",
            "CUSTOMFIELD3"
        };

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.DatabaseWriterRoot#resolveColumnMaps()
     */
    static final String[] ValidHitColumnNames = 
        {
            "LOAD_ID", "DM_LOAD_ID", "TEMP_SESSION_ID", "ACTIVITY_DATE_TIME", "GET_REQUEST", "HTML_ERROR_CODE",
            "BYTES_SENT", "TIME_TAKEN_TO_SERV_REQUEST", "CANONICAL_SERVER_PORT", "REFERRER_URL", "CLEANSED",
            "ASSOCIATED_HITS", "CLEANSED_ID", null, null, null, null, null, null, "CUSTOMFIELD1", "CUSTOMFIELD2",
            "CUSTOMFIELD3", "SERVER_NAME", "PAGE_SEQUENCE"
        };

    public static String getHitColumnName()
    {
        return ValidSessionColumnNames[HITS];
    }

    /**
     * DatabaseWriter constructor comment.
     */
    public SessionizationWriterRoot(ETLJobStatus esJobStatus, int pPartitionIdentifier)
    {
        this();

        this.setStatusObject(esJobStatus);
        this.setPartitionIdentifier(pPartitionIdentifier);
    }

    public SessionizationWriterRoot()
    {
        super();

        // DateTimeFormatter = DateFormat.getDateTimeInstance(DateFormat.MEDIUM,DateFormat.MEDIUM);
        DateTimeFormatter = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");

        this.setName(this.getName() + "(" + this.getClass().getName() + ")");

        run = new Object();
    }

    public void setStatusObject(ETLJobStatus esJobStatus)
    {
        if (esJobStatus instanceof KETLJobStatus)
        {
            this.mJobStatus = (KETLJobStatus) esJobStatus;
            this.mStepID = mJobStatus.generateStepIdentifier();
        }
    }

    public void setPartitionIdentifier(int pPartitionIdentifier)
    {
        this.mPartitionIdentifier = pPartitionIdentifier;
    }

    /**
     * DatabaseWriter constructor comment.
     * @param target java.lang.Runnable
     */
    public SessionizationWriterRoot(Runnable target)
    {
        super(target);
    }

    /**
     * DatabaseWriter constructor comment.
     * @param target java.lang.Runnable
     * @param name java.lang.String
     */
    public SessionizationWriterRoot(Runnable target, String name)
    {
        super(target, name);
    }

    /**
     * DatabaseWriter constructor comment.
     * @param name java.lang.String
     */
    public SessionizationWriterRoot(String name)
    {
        super(name);
    }

    /**
     * DatabaseWriter constructor comment.
     * @param group java.lang.ThreadGroup
     * @param target java.lang.Runnable
     */
    public SessionizationWriterRoot(ThreadGroup group, Runnable target)
    {
        super(group, target);
    }

    /**
     * DatabaseWriter constructor comment.
     * @param group java.lang.ThreadGroup
     * @param target java.lang.Runnable
     * @param name java.lang.String
     */
    public SessionizationWriterRoot(ThreadGroup group, Runnable target, String name)
    {
        super(group, target, name);
    }

    /**
     * DatabaseWriter constructor comment.
     * @param group java.lang.ThreadGroup
     * @param name java.lang.String
     */
    public SessionizationWriterRoot(ThreadGroup group, String name)
    {
        super(group, name);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 4:33:09 PM)
     * @return java.lang.String
     * @param pString java.lang.String
     */
    public final String fieldTrim(String pString, int pMaxLength)
    {
        if (pString == null)
        {
            return (null);
        }

        if ((pMaxLength != UNKNOWN_LENGTH) && (pString.length() > pMaxLength))
        {
            ResourcePool.LogMessage(this, "String to long, trimming to length of " + pMaxLength + ":" + pString);

            return (pString.substring(0, pMaxLength));
        }

        return pString;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 9:34:15 PM)
     * @return boolean
     */
    abstract boolean checkConnection();

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:58:55 PM)
     * @return datasources.DataQueue
     */
    final public BlockingQueue getDataQueue()
    {
        return DataQueue;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/12/2002 4:39:24 PM)
     * @return com.kni.etl.PageParserPageDefinition[]
     */
    final public com.kni.etl.sessionizer.PageParserPageDefinition[] getPageParserDefinitions()
    {
        return PageParserDefinitions;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 11:23:20 PM)
     * @return boolean
     */
    final public boolean isLogginOn()
    {
        return LogginOn;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/15/2002 10:22:21 PM)
     * @return boolean
     */
    final public boolean isSkipInserts()
    {
        return SkipInserts;
    }

    abstract public boolean refreshWriterConnection();

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:54:48 PM)
     */
    final public void run()
    {
        this.setName(this.getName() + ": " + this.WriterName);

        try
        {
            if (writeToDB() == false)
            {
                fatalError = true;

                // clear queue to any waiting threads can detect error
                this.DataQueue.clear();
            }
        }
        catch (Exception e)
        {
            // hashmap probably set to null
            ResourcePool.LogException(e, this);
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + e.getMessage());
            fatalError = true;
            this.mJob.releaseReaderDataQueue(this);

            return;
        }

        this.mJob.releaseReaderDataQueue(this);

        if (closeWriterConnection() == false)
        {
            return;
        }
    }

    abstract boolean closeWriterConnection();

    /**
         * Insert the method's description here.
         * Creation date: (3/5/2002 3:16:13 PM)
     * @param pUserName java.lang.String
     * @param pPassword java.lang.String
     * @param pJDBCConnection java.lang.String
         */
    abstract public boolean setDatabase(int pType);

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:58:55 PM)
     * @param newRemovedSessionsQueue datasources.DataQueue
     */
    final public void setDataQueue(BlockingQueue newDataQueue)
    {
        DataQueue = newDataQueue;
    }

    final public void setJob(XMLSessionizeJob pJob)
    {
        this.mJob = pJob;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 11:23:20 PM)
     * @param newLogginOn boolean
     */
    final public void setLogginOn(boolean newLogginOn)
    {
        LogginOn = newLogginOn;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/12/2002 4:39:24 PM)
     * @param newPageParserDefinitions com.kni.etl.PageParserPageDefinition[]
     */
    final public void setPageParserDefinitions(
        com.kni.etl.sessionizer.PageParserPageDefinition[] newPageParserDefinitions)
    {
        PageParserDefinitions = newPageParserDefinitions;
    }

    Object[][] maColumnMaps = null;

    final public void setColumnMaps(Object[][] pColumnsMaps)
    {
        maColumnMaps = pColumnsMaps;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/15/2002 10:22:21 PM)
     * @param newSkipInserts boolean
     */
    final public void setSkipInserts(boolean newSkipInserts)
    {
        SkipInserts = newSkipInserts;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:57:55 PM)
     */
    final public void stopWriting()
    {
        ResourcePool.LogMessage(this, this.WriterName + " being stopped.");
        run = null;
    }

    transient URLCleaner referrerURLCleaner = null;
    transient URLCleaner urlCleaner = null;
    int insertCnt = 0;
    transient boolean startCheckingForNoMoreWriters = false;

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:55:28 PM)
     */
    final public boolean writeToDB()
    {
        // switch to oracle specific batch update, performance tweak.
        ResultRecord resultRecord = null;
        insertCnt = 0;

        try
        {
            // check db connection
            if ((this.SkipInserts == false) && (dbConnection == null))
            {
                if (refreshWriterConnection() == false)
                {
                    ResourcePool.LogMessage("Error refreshing connection");

                    return false;
                }
            }

            // only shutdown on error or end marker
            shutdown = false;

            // enter loop feeding data from queue
            do
            {
                resultRecord = (ResultRecord) DataQueue.take();

                if (resultRecord == AnalyzeSessionQueuedThread.END_MARKER)
                {
                    this.mJob.releaseReaderDataQueue(this);
                    shutdown = true;
                }
                else
                {
                    if (this.SkipInserts == false)
                    {
                        writeRecord(resultRecord);
                    }

                    // if pending inserts greater than batchsize then execute batch
                    if ((dbConnection != null) && (insertCnt >= this.BatchSize))
                    {
                        RowsInserted = RowsInserted + insertCnt;

                        if (this.checkConnection() == false)
                        {
                            ResourcePool.LogMessage(this, "Connection Lost in batch end commit:" + RowsInserted);

                            return false;
                        }

                        if (submitBatch())
                        {
                            commitData();
                        }
                        else
                        {
                            ResourcePool.LogMessage("Failed to submit batch");

                            return false;
                        }

                        insertCnt = 0;
                    }
                }
            }
            while (shutdown == false);

            if (insertCnt > 0)
            {
                RowsInserted = RowsInserted + insertCnt;

                if (this.checkConnection() == false)
                {
                    ResourcePool.LogMessage(this, "Connection Lost at queue empty write");

                    return false;
                }
                else if (this.SkipInserts == false)
                {
                    if (submitBatch())
                    {
                        commitData();
                    }
                    else
                    {
                        ResourcePool.LogMessage("Failed to submit batch");

                        return false;
                    }
                }

                if (dbConnection != null)
                {
                    commitData();
                    resetTimeStampCache();
                }
            }
        }

        catch (Exception ee)
        {
            ResourcePool.LogException(ee, this);
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());

            return false;
        }

        return true;
    }

    final public boolean directFinish()
    {
        if (insertCnt > 0)
        {
            RowsInserted = RowsInserted + insertCnt;

            if (this.checkConnection() == false)
            {
                ResourcePool.LogMessage(this, "Connection Lost at queue empty write");

                return false;
            }
            else if (this.SkipInserts == false)
            {
                if (submitBatch())
                {
                    commitData();
                }
                else
                {
                    ResourcePool.LogMessage("Failed to submit batch");

                    return false;
                }
            }

            if (dbConnection != null)
            {
                commitData();
                resetTimeStampCache();
            }
        }

        return true;
    }

    final public boolean directConnect()
    {
        // check db connection
        if ((this.SkipInserts == false) && (dbConnection == null))
        {
            if (refreshWriterConnection() == false)
            {
                ResourcePool.LogMessage("Error refreshing connection");

                return false;
            }
        }

        return true;
    }

    final public boolean directWrite(ResultRecord resultRecord)
    {
        try
        {
            if (this.SkipInserts == false)
            {
                writeRecord(resultRecord);

                // if pending inserts greater than batchsize then execute batch
                if (insertCnt >= this.BatchSize)
                {
                    RowsInserted = RowsInserted + insertCnt;

                    if (this.checkConnection() == false)
                    {
                        ResourcePool.LogMessage(this, "Connection Lost in batch end commit:" + RowsInserted);

                        return false;
                    }

                    if (submitBatch())
                    {
                        commitData();
                    }
                    else
                    {
                        ResourcePool.LogMessage("Failed to submit batch");

                        return false;
                    }

                    insertCnt = 0;
                }
            }
        }
        catch (Exception ee)
        {
            ResourcePool.LogException(ee, this);
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());

            return false;
        }

        return true;
    }

    abstract boolean commitData();

    abstract boolean submitBatch();

    java.sql.Timestamp[][] maTimestampCache;
    int[] miTimestampPos;

    final java.sql.Timestamp getCachedTimeStamp(int batchPosition)
    {
        return maTimestampCache[batchPosition][miTimestampPos[batchPosition]++];
    }

    final void buildTimestampCache()
    {
        maTimestampCache = new Timestamp[this.BatchSize][numberOfTimestampsPerStatement()];
        miTimestampPos = new int[this.BatchSize];

        for (int i = 0; i < this.BatchSize; i++)
        {
            for (int x = 0; x < numberOfTimestampsPerStatement(); x++)
                maTimestampCache[i][x] = new Timestamp(0);
        }
    }

    abstract int numberOfTimestampsPerStatement();

    final void resetTimeStampCache()
    {
        for (int i = 0; i < this.BatchSize; i++)
        {
            miTimestampPos[i] = 0;
        }

        return;
    }

    abstract void writeRecord(ResultRecord resultRecord)
        throws Exception;

    ArrayList maColumns = null;
    String msTable = null;
    String msHint = null;

    final void addColumn(String psColumnName, int psColumnType)
    {
        if (maColumns == null)
        {
            maColumns = new ArrayList();
        }

        Object[] o = new Object[2];
        o[0] = psColumnName;
        o[1] = new Integer(psColumnType);
        maColumns.add(o);
    }

    abstract String buildStatement();

    /**
     * @param string
     */
    final public void setSQLHint(String string)
    {
        msHint = string;
    }

    /**
     * @param string
     */
    final public void setTable(String string)
    {
        msTable = string;
    }

    abstract void getColumnMaxSizes();

    final void resolveSessionColumnMaps()
    {
        resolveColumnMaps(SessionizationWriterRoot.ValidSessionColumnNames);
    }

    final void resolveHitColumnMaps()
    {
        resolveColumnMaps(SessionizationWriterRoot.ValidHitColumnNames);
    }

    final void resolveColumnMaps(String[] columnMap)
    {
        for (int i = 0; i < this.maColumnMaps.length; i++)
        {
            Object[] o = this.maColumnMaps[i];

            String columnName = (String) o[0];
            String sourceColumn = (String) o[1];

            for (int x = 0; x < columnMap.length; x++)
            {
                if (sourceColumn.equalsIgnoreCase(columnMap[x]))
                {
                    this.addColumn(columnName, x);
                }
            }
        }
    }
}
