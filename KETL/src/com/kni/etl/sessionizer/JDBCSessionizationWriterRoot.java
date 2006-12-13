/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.kni.etl.ETLJobStatus;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl_v1.ResultRecord;


public abstract class JDBCSessionizationWriterRoot extends SessionizationWriterRoot  implements DBConnection
{
    /**
     * @param esJobStatus
     */
    public JDBCSessionizationWriterRoot(ETLJobStatus esJobStatus, int pPartitionIdentifier)
    {
        super(esJobStatus, pPartitionIdentifier);
    }

    public JDBCSessionizationWriterRoot()
    {
        super();
    }

    PreparedStatement stmt = null;
    PreparedStatement testStmt = null;

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#commitData()
     */
    boolean commitData()
    {
        try
        {
            ((Connection) this.dbConnection).commit();
        }
        catch (Exception ee)
        {
            ResourcePool.LogException(ee, this);
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());

            return false;
        }

        return true;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#submitBatch()
     */
    boolean submitBatch()
    {
        try
        {
            this.stmt.executeBatch();
            resetTimeStampCache();

            // increment batch and insert counter
            //this.mJobStatus.incrementBatchCount(this.mStepID, mPartitionIdentifier, 1);
            //this.mJobStatus.incrementInsertCount(this.mStepID, mPartitionIdentifier, insertCnt);
        }
        catch (SQLException ee)
        {
            do
            {
                ResourcePool.LogMessage(this,
                    "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());

                if ((ee.getNextException() != null) && (ee.getNextException() != ee))
                {
                    ee = ee.getNextException();
                }
                else
                {
                    return false;
                }
            }
            while (ee != null);
        }

        return true;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 9:34:15 PM)
     * @return boolean
     */
    final boolean checkConnection()
    {
        boolean test = false;

        try
        {
            if (this.dbConnection == null)
            {
                this.refreshWriterConnection();
            }

            if ((this.dbConnection != null) && ((Connection) this.dbConnection).isClosed())
            {
                System.err.println("checkConnection connection closed for reason unknown");

                return false;
            }

            test = ResourcePool.testConnection((Connection) this.dbConnection);

            return test;
        }
        catch (SQLException ee)
        {
            System.err.println("checkConnection SQLException: " + ee);

            return false;
        }
        catch (Exception ee)
        {
            System.err.println("checkConnection Exception: " + ee);

            return false;
        }
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        return (Connection)this.dbConnection;
    }

    final public boolean refreshWriterConnection()
    {
        try
        {
            dbConnection = ResourcePool.getConnection(JDBCDriver, JDBCURL, Username, Password, PreSQL, true);
        }
        catch (Exception e)
        {
            ResourcePool.LogMessage(this, "Error refreshing db connection:" + this.WriterName + ":" + e);

            return false;
        }

        return true;
    }

    boolean closeWriterConnection()
    {
        try
        {
            if (stmt != null)
            {
                stmt.close();
            }

            if (dbConnection != null)
            {
                ResourcePool.releaseConnection((Connection) dbConnection);
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

    /**
         * Insert the method's description here.
         * Creation date: (3/5/2002 3:16:13 PM)
     * @param pUserName java.lang.String
     * @param pPassword java.lang.String
     * @param pJDBCConnection java.lang.String
         */
    final public boolean setDatabase(int pType)
    {
        if (mJob == null)
        {
            return false;
        }

        JDBCDriver = mJob.getParameterValueForType(pType, DRIVER_ATTRIB);
        JDBCURL = mJob.getParameterValueForType(pType, URL_ATTRIB);
        Username = mJob.getParameterValueForType(pType, USER_ATTRIB);
        Password = mJob.getParameterValueForType(pType, PASSWORD_ATTRIB);
        PreSQL = mJob.getParameterValueForType(pType, PRESQL_ATTRIB);

        return true;
    }

    abstract void writeRecord(ResultRecord resultRecord)
        throws SQLException, Exception;

    //ArrayList maColumns = null;
    //String msTable = null;
    //String msHint = null;
    String buildStatement()
    {
        getColumnMaxSizes();

        String sql = "INSERT " + msHint + " into " + msTable + "(";

        for (int i = 0; i < maColumns.size(); i++)
        {
            Object[] o = (Object[]) maColumns.get(i);

            if (i > 0)
            {
                sql = sql + ",";
            }

            sql = sql + (String) o[0];

            maHitParameterPosition[((Integer) o[1]).intValue()] = i + 1;
        }

        sql = sql + ") values(";

        for (int i = 0; i < maColumns.size(); i++)
        {
            if (i > 0)
            {
                sql = sql + ",";
            }

            sql = sql + "?";
        }

        return sql + ")";
    }

    final void getColumnMaxSizes()
    {
        DatabaseMetaData dmd;
        ResultSet rs = null;
        StringBuffer sb = new StringBuffer();

        try
        {
            dmd = ((Connection) this.dbConnection).getMetaData();

            for (int i = 0; i < maColumns.size(); i++)
            {
                Object[] o = (Object[]) maColumns.get(i);

                String columnName = (String) o[0];

                rs = dmd.getColumns(null, null, msTable, columnName);

                int cLength = 0;

                if (rs.next())
                {
                    cLength = rs.getInt("COLUMN_SIZE");
                }
                else
                {
                    cLength = UNKNOWN_LENGTH;
                    sb.append("\n\tSchema query not supported. " + "Using default value " + cLength + " for " +
                        msTable + "." + columnName + " column length.");
                }

                rs.close();

                maHitParameterSize[((Integer) o[1]).intValue()] = cLength;
            }

            if (sb.length() > 0)
            {
                ResourcePool.LogMessage(this, sb.toString());
            }
        }
        catch (SQLException e)
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e1)
                {
                    ResourcePool.LogException(e1, this);
                }
            }

            ResourcePool.LogException(e, this);

            for (int i = 0; i < this.maHitParameterSize.length; i++)
                this.maHitParameterSize[i] = UNKNOWN_LENGTH;

            return;
        }
    }

    final void addBatch() throws Exception
    {
        // date and time
        if (dbConnection == null)
        {
            refreshWriterConnection();
        }

        if (this.isSkipInserts() == false)
        {
            stmt.addBatch();
        }

        insertCnt++;
    }
}
