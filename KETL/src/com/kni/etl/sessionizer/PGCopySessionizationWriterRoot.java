/*
 * LMR, Version 2.0
 *
 * Copyright (C) 2003 by Metapa, Inc. All Rights Reserved.
 *
 * $Id: PGCopySessionizationWriterRoot.java,v 1.1 2006/12/13 07:06:43 nwakefield Exp $
 */
package com.kni.etl.sessionizer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl_v1.ResultRecord;

/**
 * This class provides CDB specific implementation to the abstract SessionizationWriterRoot class.
 * 
 * @author <A HREF="mailto:mohit@metapa.net">Mohit Kumar</A>
 * @version $Revision: 1.1 $
 */
public abstract class PGCopySessionizationWriterRoot extends SessionizationWriterRoot implements DBConnection {

    /**
     * @param esJobStatus
     */

    PGCopyWriter stmt = null;
    PreparedStatement testStmt = null;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#commitData()
     */
    boolean commitData() {
        try {
            ((Connection) this.dbConnection).commit();
        } catch (Exception ee) {
            ResourcePool.LogException(ee, this);
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());

            return false;
        }

        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#submitBatch()
     */
    boolean submitBatch() {
        try {
            this.stmt.executeBatch();
        } catch (SQLException ee) {
            
            // dump bad batch
            try {
                String dumpFile = "log" + File.separator + this.mJob.getJobID() + "." + this.mJob.getJobExecutionID();

                ResourcePool.LogMessage(this, "Bad batch logged to: " + dumpFile);
                ResourcePool.LogMessage(this, "Copy command used: " + this.stmt.loadCommand());

                OutputStream dump = new FileOutputStream(dumpFile);
                OutputStream dumpBuffer = new BufferedOutputStream(dump);

                dumpBuffer.write(this.stmt.badLoadContents());

                dumpBuffer.close();
                dump.close();

            } catch (IOException e) {
                ResourcePool.LogMessage(e, "Bad batch logging failed: " + e.toString());
                ResourcePool.LogException(e, this);
            }
            
            do {
                ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":"
                        + ee.getMessage());

                if ((ee.getNextException() != null) && (ee.getNextException() != ee)) {
                    ee = ee.getNextException();
                }
                else {
                    return false;
                }
            } while (ee != null);

        } catch (IOException ee) {
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 9:34:15 PM)
     * 
     * @return boolean
     */
    final boolean checkConnection() {
        boolean test = false;

        try {
            if (this.dbConnection == null) {
                this.refreshWriterConnection();
            }

            if ((this.dbConnection != null) && ((Connection) this.dbConnection).isClosed()) {
                System.err.println("checkConnection connection closed for reason unknown");

                return false;
            }

            test = ResourcePool.testConnection((Connection) this.dbConnection);

            return test;
        } catch (SQLException ee) {
            System.err.println("checkConnection SQLException: " + ee);

            return false;
        } catch (Exception ee) {
            System.err.println("checkConnection Exception: " + ee);

            return false;
        }
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        return (Connection)this.dbConnection;
    }

    final public boolean refreshWriterConnection() {
        try {
            dbConnection = ResourcePool.getConnection(JDBCDriver, JDBCURL, Username, Password, PreSQL, true);

            stmt = new PGCopyWriter((Connection) dbConnection);

            miTimestampPos = new int[this.BatchSize];
        } catch (Exception e) {
            ResourcePool.LogMessage(this, "Error refreshing db connection:" + this.WriterName + ":" + e);

            return false;
        }

        return true;
    }

    boolean closeWriterConnection() {
        try {
            if (stmt != null) {
                stmt.close();
            }

            if (dbConnection != null) {
                ResourcePool.releaseConnection((Connection) dbConnection);
            }
        } catch (Exception ee) {
            ResourcePool.LogException(ee, this);
            ResourcePool.LogMessage(this, "writeToDB Exception, Insert Batch:" + RowsInserted + ":" + ee.getMessage());

            return false;
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (3/5/2002 3:16:13 PM)
     * 
     * @param pUserName java.lang.String
     * @param pPassword java.lang.String
     * @param pJDBCConnection java.lang.String
     */
    final public boolean setDatabase(int pType) {
        if (mJob == null) {
            return false;
        }

        JDBCDriver = mJob.getParameterValueForType(pType, DRIVER_ATTRIB);
        JDBCURL = mJob.getParameterValueForType(pType, URL_ATTRIB);
        Username = mJob.getParameterValueForType(pType, USER_ATTRIB);
        Password = mJob.getParameterValueForType(pType, PASSWORD_ATTRIB);
        PreSQL = mJob.getParameterValueForType(pType, PRESQL_ATTRIB);

        return true;
    }

    abstract void writeRecord(ResultRecord resultRecord) throws SQLException, Exception;

    final void getColumnMaxSizes() {
        DatabaseMetaData dmd;
        ResultSet rs = null;
        StringBuffer sb = new StringBuffer();

        try {
            dmd = ((Connection) this.dbConnection).getMetaData();

            for (int i = 0; i < maColumns.size(); i++) {
                Object[] o = (Object[]) maColumns.get(i);

                String columnName = (String) o[0];

                rs = dmd.getColumns(null, null, msTable, columnName);

                int cLength = 0;

                if (rs.next()) {
                    cLength = rs.getInt("COLUMN_SIZE");
                }
                else {
                    cLength = UNKNOWN_LENGTH;
                    sb.append("\n\tSchema query not supported. " + "Using default value " + cLength + " for " + msTable
                            + "." + columnName + " column length.");
                }

                rs.close();

                maHitParameterSize[((Integer) o[1]).intValue()] = cLength;
            }

            if (sb.length() > 0) {
                ResourcePool.LogMessage(this, sb.toString());
            }
        } catch (SQLException e) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e1) {
                    ResourcePool.LogException(e1, this);
                }
            }

            ResourcePool.LogException(e, this);

            for (int i = 0; i < this.maHitParameterSize.length; i++)
                this.maHitParameterSize[i] = UNKNOWN_LENGTH;

            return;
        }
    }

    final void addBatch() throws Exception {
        // date and time
        if (dbConnection == null) {
            refreshWriterConnection();
        }

        if (this.isSkipInserts() == false) {
            stmt.addBatch();
        }

        insertCnt++;
    }

    String buildStatement() {
        if (this.stmt.loadCommandReady() == false) {
            // sessionSQLStmt = SESSION_SQL_CMDS.getString("SESSION_SQL_STMT");
            getColumnMaxSizes();

            this.sortColumnsForStatementCreation();

            String[] tmp = new String[maColumns.size()];

            for (int i = 0; i < maColumns.size(); i++) {
                Object[] o = (Object[]) maColumns.get(i);

                tmp[i] = (String) o[0];

                maHitParameterPosition[((Integer) o[1]).intValue()] = i + 1;
            }

            this.stmt.createLoadCommand(msTable, tmp);
        }

        return this.stmt.loadCommand();
    }

    abstract void sortColumnsForStatementCreation();

    void sortColumns(int[] nOrderOfColumns) {
        Object[] tmp = new Object[nOrderOfColumns.length];

        for (int i = 0; i < maColumns.size(); i++) {
            Object[] o = (Object[]) maColumns.get(i);

            for (int x = 0; x < nOrderOfColumns.length; x++) {
                if (nOrderOfColumns[x] == ((Integer) o[1]).intValue()) {
                    tmp[x] = o;
                    x = nOrderOfColumns.length;
                }
            }
        }

        maColumns.clear();

        for (int i = 0; i < tmp.length; i++) {
            if (tmp[i] != null) {
                maColumns.add(tmp[i]);
            }
        }
    }
}
