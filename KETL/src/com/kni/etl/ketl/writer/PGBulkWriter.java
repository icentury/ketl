/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.dbutils.postgresql.PGCopyWriter;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;

/**
 * <p>
 * Title: PGBulkWriter
 * </p>
 * <p>
 * Description: Similar functionality to JDBC writer but the data is bulk loaded using a customized JDBC driver for
 * PostgreSQL.
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class PGBulkWriter extends ETLWriter implements DefaultWriterCore, WriterBatchManager, DBConnection, PrePostSQL {

    public PGBulkWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    // String strDataStoreName = null;
    String strUserName = null;
    String strPassword = null;
    String strURL = null;
    String strDriverClass = null;
    String strPreSQL = null;

    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    public static final String TABLE_ATTRIB = "TABLE";
    int recordsInBatch = 0;
    String mstrTableName = null;

    DatabaseColumnDefinition[] madcdColumns = null;
    Vector mvColumns = new Vector(); // for building the column list and later converting it into the array

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.writer.SubComponentBatchRunnerThread#write(java.lang.Object)
     */

    protected void write(PGCopyWriter pPreparedBatch) throws SQLException, IOException {
        PGCopyWriter currentStatement = pPreparedBatch;
        currentStatement.executeBatch();
        currentStatement.getConnection().commit();

    }

    Connection mcDBConnection = null;

    /**
     * DOCUMENT ME!
     * 
     * @param nConfig DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws KETLThreadException
     */
    @Override
    public int initialize(Node nConfig) throws KETLThreadException {
        int res = super.initialize(nConfig);

        if (res != 0) {
            return res;
        }

        // Get the attributes
        NamedNodeMap nmAttrs = nConfig.getAttributes();

        // Pull the parameters from the list...
        this.strUserName = this.getParameterValue(0, DBConnection.USER_ATTRIB);
        this.strPassword = this.getParameterValue(0, DBConnection.PASSWORD_ATTRIB);
        this.strURL = this.getParameterValue(0, DBConnection.URL_ATTRIB);
        this.strDriverClass = this.getParameterValue(0, DBConnection.DRIVER_ATTRIB);
        this.strPreSQL = this.getParameterValue(0, DBConnection.PRESQL_ATTRIB);

        // Convert the vector we've been building into a more common array...
        this.madcdColumns = (DatabaseColumnDefinition[]) this.mvColumns.toArray(new DatabaseColumnDefinition[0]);

        if (res != 0)
            return res;
        // Pull the name of the table to be written to...
        this.mstrTableName = XMLHelper.getAttributeAsString(nmAttrs, PGBulkWriter.TABLE_ATTRIB, null);

        // Pull the commit size...
        this.batchSize = XMLHelper.getAttributeAsInt(nmAttrs, PGBulkWriter.COMMITSIZE_ATTRIB, this.batchSize);

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Commit and Batch size: " + this.batchSize);

        String[] cols = new String[this.madcdColumns.length];
        for (int i = 0; i < this.madcdColumns.length; i++) {
            cols[i] = this.madcdColumns[i].getColumnName(null, -1);
        }

        try {
            this.mcDBConnection = ResourcePool.getConnection(this.strDriverClass, this.strURL, this.strUserName,
                    this.strPassword, this.strPreSQL, true);
            this.executePreStatements();
            this.executePreBatchStatements();

            this.stmt = new PGCopyWriter(this.mcDBConnection);
            this.stmt.createLoadCommand(this.mstrTableName, cols);

        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        return 0;
    }

    PGCopyWriter stmt = null;

    public class PGBulkETLInPort extends ETLInPort {

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);

            if (res != 0)
                return res;

            DatabaseColumnDefinition dcdNewColumn;

            // Create a new column definition with the default properties...
            dcdNewColumn = new DatabaseColumnDefinition(xmlConfig, "", 0);
            dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN); // INSERT by default

            // Get the column's target name...
            dcdNewColumn.setColumnName(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), ETLStep.NAME_ATTRIB,
                    null));

            // It's ok if not specified
            PGBulkWriter.this.mvColumns.add(dcdNewColumn);

            return 0;
        }

        public PGBulkETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    boolean mbSetDatatype = true;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.writer.SubComponentParallelBatchWriter#complete()
     */
    @Override
    public int complete() throws KETLThreadException {
        int res = super.complete();
        if (res != 0)
            return res;
        try {

            if (this.recordsInBatch > 0) {
                this.stmt.executeBatch();
                this.recordsInBatch = 0;
                this.executePostBatchStatements();
            }

            this.executePostStatements();
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        } finally {

            try {
                this.stmt.close();
            } catch (SQLException e) {
                ResourcePool.LogException(e, this);
            }

            this.stmt = null;

            ResourcePool.releaseConnection(this.getConnection());
            this.mcDBConnection = null;

        }

        return res;
    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new PGBulkETLInPort(this, srcStep);
    }

    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {

        try {

            for (int i = 0; i < pRecordWidth; i++) {

                Class cl = pExpectedDataTypes[this.mInPorts[i].getSourcePortIndex()];

                Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue()
                        : pInputRecords[this.mInPorts[i].getSourcePortIndex()];
                if (data == null)
                    this.stmt.setNull(i + 1, -1);
                else if (cl == String.class)
                    this.stmt.setString(i + 1, (String) data);
                else if (cl == Integer.class || cl == int.class)
                    this.stmt.setInt(i + 1, (Integer) data);
                else if (cl == Double.class || cl == double.class)
                    this.stmt.setDouble(i + 1, (Double) data, 20);
                else if (cl == Long.class || cl == long.class)
                    this.stmt.setLong(i + 1, (Long) data);
                else if (cl == Float.class || cl == Float.class)
                    this.stmt.setFloat(i + 1, (Float) data);
                else if (cl == java.util.Date.class || cl == java.sql.Timestamp.class || cl == java.sql.Time.class
                        || cl == java.sql.Date.class)
                    this.stmt.setTimestamp(i + 1, (java.util.Date) data);
                else if (cl == Boolean.class || cl == boolean.class)
                    this.stmt.setBoolean(i + 1, (Boolean) data);
                else if (cl == byte[].class)
                    this.stmt.setByteArrayValue(i + 1, (byte[]) data);
                else
                    throw new KETLWriteException("Unsupported class for bulk writer " + cl.getCanonicalName());
            }

            this.stmt.addBatch();
        } catch (Exception e) {
            throw new KETLWriteException(e);
        }
        this.recordsInBatch++;
        return 1;
    }

    public Connection getConnection() {
        return this.mcDBConnection;
    }

    public int finishBatch(int len) throws KETLWriteException {
        if (this.recordsInBatch == this.batchSize || (this.recordsInBatch > 0 && len == BatchManager.LASTBATCH)) {
            try {
                this.stmt.executeBatch();
                this.executePostBatchStatements();
                this.executePreBatchStatements();
            } catch (Exception e) {
                throw new KETLWriteException(e);
            }
            this.recordsInBatch = 0;
        }
        return len;
    }

    public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {

        return data;
    }

    public void executePostStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
    }

    public void executePreStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
    }

    public void executePostBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTBATCHSQL");
    }

    public void executePreBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PREBATCHSQL");
    }

    @Override
    protected void close(boolean success) {

        try {
            if (this.stmt != null)
                this.stmt.close();
        } catch (SQLException e) {
            ResourcePool.LogException(e, this);
        }

        if (this.mcDBConnection != null)
            ResourcePool.releaseConnection(this.mcDBConnection);
    }

}
