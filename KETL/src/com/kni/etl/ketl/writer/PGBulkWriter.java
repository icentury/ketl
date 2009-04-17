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
package com.kni.etl.ketl.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
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

// TODO: Auto-generated Javadoc
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

    /**
     * Instantiates a new PG bulk writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public PGBulkWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    // String strDataStoreName = null;
    /** The str user name. */
    String strUserName = null;
    
    /** The str password. */
    String strPassword = null;
    
    /** The str URL. */
    String strURL = null;
    
    /** The str driver class. */
    String strDriverClass = null;
    
    /** The str pre SQL. */
    String strPreSQL = null;

    /** The Constant COMMITSIZE_ATTRIB. */
    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    
    /** The Constant TABLE_ATTRIB. */
    public static final String TABLE_ATTRIB = "TABLE";
    
    /** The records in batch. */
    int recordsInBatch = 0;
    
    /** The mstr table name. */
    String mstrTableName = null;

    /** The madcd columns. */
    DatabaseColumnDefinition[] madcdColumns = null;
    
    /** The mv columns. */
    Vector mvColumns = new Vector(); // for building the column list and later converting it into the array

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.writer.SubComponentBatchRunnerThread#write(java.lang.Object)
     */

    /**
     * Write.
     * 
     * @param pPreparedBatch the prepared batch
     * 
     * @throws SQLException the SQL exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void write(PGCopyWriter pPreparedBatch) throws SQLException, IOException {
        PGCopyWriter currentStatement = pPreparedBatch;
        currentStatement.executeBatch();
        currentStatement.getConnection().commit();

    }

    /** The mc DB connection. */
    Connection mcDBConnection = null;

	private Properties mDatabaseProperties;

    /**
     * DOCUMENT ME!.
     * 
     * @param nConfig DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     * 
     * @throws KETLThreadException the KETL thread exception
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
        try {
			this.setDatabaseProperties(this.getParameterListValues(0));
		} catch (Exception e1) {
			throw new KETLThreadException(e1,this);
		}
        
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
                    this.strPassword, this.strPreSQL, true, this.getDatabaseProperties());
            this.executePreStatements();
            this.executePreBatchStatements();

            this.stmt = new PGCopyWriter(this.mcDBConnection);
            this.stmt.createLoadCommand(this.mstrTableName, cols);

        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        return 0;
    }
    
    private Properties getDatabaseProperties() {
		return this.mDatabaseProperties;
	}

	private void setDatabaseProperties(Map<String, Object>  parameterListValues) throws Exception {
		this.mDatabaseProperties = JDBCItemHelper.getProperties(parameterListValues);		
	}

    /** The stmt. */
    PGCopyWriter stmt = null;

    /**
     * The Class PGBulkETLInPort.
     */
    public class PGBulkETLInPort extends ETLInPort {

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
         */
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

        /**
         * Instantiates a new PG bulk ETL in port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public PGBulkETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /** The mb set datatype. */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new PGBulkETLInPort(this, srcStep);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.DBConnection#getConnection()
     */
    public Connection getConnection() {
        return this.mcDBConnection;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.WriterBatchManager#finishBatch(int)
     */
    public int finishBatch(int len) throws KETLWriteException {
    	int result = 0;
        if (this.recordsInBatch == this.batchSize || (this.recordsInBatch > 0 && len == BatchManager.LASTBATCH)) {
            try {
                this.stmt.executeBatch();
            	result = len;
                this.executePostBatchStatements();
                this.executePreBatchStatements();
            } catch (Exception e) {
                throw new KETLWriteException(e);
            }
            this.recordsInBatch = 0;
        }
        return result;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.WriterBatchManager#initializeBatch(java.lang.Object[][], int)
     */
    public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {

        return data;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePostStatements()
     */
    public void executePostStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
     */
    public void executePreStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePostBatchStatements()
     */
    public void executePostBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTBATCHSQL");
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
     */
    public void executePreBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PREBATCHSQL");
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success, boolean jobFailed) {

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
