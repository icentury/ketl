/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.sql.SQLException;

import com.kni.etl.ketl_v1.ResultRecord;


public abstract class TestSessionizationWriterRoot extends SessionizationWriterRoot
{
    Object stmt = null;

    /**
     * @param esJobStatus
     */

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#commitData()
     */
    boolean commitData()
    {
        return true;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#submitBatch()
     */
    boolean submitBatch()
    {
        resetTimeStampCache();

        // incremen batch and insert counter
        //this.mJobStatus.incrementBatchCount(this.mStepID, mPartitionIdentifier, 1);
        //this.mJobStatus.incrementInsertCount(this.mStepID, mPartitionIdentifier, insertCnt);
        return true;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 9:34:15 PM)
     * @return boolean
     */
    final boolean checkConnection()
    {
        return true;
    }

    final public boolean refreshWriterConnection()
    {
        if (stmt == null)
        {
            this.buildStatement();
            stmt = new String();
            buildTimestampCache();
        }

        return true;
    }

    boolean closeWriterConnection()
    {
        return true;
    }

    /**
         * Insert the method's description here.
         * Creation date: (3/5/2002 3:16:13 PM)
     * @param pUserName java.lang.String
     * @param pPassword java.lang.String
     * @param pJDBCConnection java.lang.String
         */
    final public boolean setDatabase(int pType)
    {
        return true;
    }

    abstract void writeRecord(ResultRecord resultRecord)
        throws SQLException, Exception;

    String buildStatement()
    {
        for (int i = 0; i < maHitParameterPosition.length; i++)
        {
            maHitParameterPosition[i] = 1;
        }

        return "";
    }

    final void getColumnMaxSizes()
    {
    }

    final void addBatch() throws Exception
    {
        insertCnt++;
    }
}
