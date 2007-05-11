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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.SharedCounter;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLJob;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.lookup.LookupCreatorImpl;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.ketl.lookup.SCDValue;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title: JDBCELTWriter
 * </p>
 * <p>
 * Description: Similar functionality to JDBC writer but the data is bulk loaded into a new table in the database then
 * joined to the destination table to create a final table. This code is beta and the following items still need to be
 * addressed. 1. Extra row created - bug 2. Support for slowly changing dimensions 3. Total index recreation 4. Support
 * for DB specific bulk loader API's, such as COPY in PgSQL 5. Support for roll and archive of new and old table 6.
 * Support for partition swapping 7. Support for lookups direclty in the db 8. Support for partitioning key in temp
 * table Once this is done this approach to loading will leverage the database for greater performance
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
abstract public class SCDWriter extends ETLWriter implements DefaultWriterCore, DBConnection, WriterBatchManager,
        LookupCreatorImpl, PrePostSQL {

    /**
     * Instantiates a new SCD writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public SCDWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    }

    /**
     * The Class Index.
     */
    class Index {

        /** The m columns. */
        ArrayList mColumns = new ArrayList();
        
        /** The m name. */
        String mName;
        
        /** The m non unique. */
        boolean mNonUnique;
    }

    /**
     * The Class IndexColumn.
     */
    class IndexColumn {

        /** The m ascending. */
        boolean mAscending;
        
        /** The m column. */
        String mColumn;
        
        /** The m position. */
        short mPosition;
    }

    /** The Constant ALTERNATE_INSERT_VALUE. */
    public static final String ALTERNATE_INSERT_VALUE = "ALTERNATE_INSERT_VALUE";
    
    /** The Constant ALTERNATE_UPDATE_VALUE. */
    public static final String ALTERNATE_UPDATE_VALUE = "ALTERNATE_UPDATE_VALUE";
    
    /** The Constant BATCH_ATTRIB. */
    public static final String BATCH_ATTRIB = "BATCHDATA";
    
    /** The Constant HANDLER_ATTRIB. */
    public static final String HANDLER_ATTRIB = "HANDLER";
    
    /** The Constant COMMITSIZE_ATTRIB. */
    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    
    /** The Constant COMPARE_ATTRIB. */
    public static final String COMPARE_ATTRIB = "COMPARE";
    
    /** The Constant SURROGATE_KEY_ATTRIB. */
    public static final String SURROGATE_KEY_ATTRIB = "SURROGATEKEY";
    
    /** The Constant INSERT_ATTRIB. */
    public static final String INSERT_ATTRIB = "INSERT";
    
    /** The Constant MAXTRANSACTIONSIZE_ATTRIB. */
    public static final String MAXTRANSACTIONSIZE_ATTRIB = "MAXTRANSACTIONSIZE";
    
    /** The Constant LOWER_CASE. */
    static final int LOWER_CASE = 0;
    
    /** The Constant MIXED_CASE. */
    static final int MIXED_CASE = 2;
    
    /** The Constant PK_ATTRIB. */
    public static final String PK_ATTRIB = "PK";
    
    /** The Constant SCHEMA_ATTRIB. */
    public static final String SCHEMA_ATTRIB = "SCHEMA";

    /** The Constant SEQUENCE_ATTRIB. */
    public static final String SEQUENCE_ATTRIB = "SEQUENCE";
    
    /** The Constant SK_ATTRIB. */
    public static final String SK_ATTRIB = "SK";
    
    /** The Constant SWAP_PARTITION. */
    static final int SWAP_PARTITION = 0;
    
    /** The Constant SWAP_TABLE. */
    static final int SWAP_TABLE = 1;
    
    /** The Constant TABLE_ATTRIB. */
    public static final String TABLE_ATTRIB = "TABLE";
    
    /** The Constant TYPE_ATTRIB. */
    public static final String TYPE_ATTRIB = "TYPE";
    
    /** The Constant STREAM_ATTRIB. */
    public static final String STREAM_ATTRIB = "STREAMCHANGES";
    
    /** The Constant UPDATE_ATTRIB. */
    public static final String UPDATE_ATTRIB = "UPDATE";
    
    /** The Constant IGNOREINVALIDCOLUMNS_ATTRIB. */
    public static final String IGNOREINVALIDCOLUMNS_ATTRIB = "IGNOREINVALIDCOLUMNS";
    
    /** The Constant UPPER_CASE. */
    static final int UPPER_CASE = 1;
    
    /** The madcd columns. */
    DatabaseColumnDefinition[] madcdColumns = null;
    
    /** The ma other columns. */
    String[] maOtherColumns = null;
    
    /** The m batch data. */
    boolean mBatchData = true;
    
    /** The m stream changes. */
    boolean mStreamChanges = true;
    
    /** The mc DB connection. */
    private Connection mcDBConnection;
    
    /** The m DB case. */
    int mDBCase = -1;
    
    /** The m DB type. */
    String mDBType = null;
    
    /** The m dont compound statements. */
    boolean mDontCompoundStatements = false;
    
    /** The mi commit size. */
    int miCommitSize;
    
    /** The mi field population order. */
    private int[] miFieldPopulationOrder;
    
    /** The mi insert count. */
    int miInsertCount = 0;
    
    /** The mi max transaction size. */
    int miMaxTransactionSize = -1;
    
    /** The mi replace technique. */
    int miReplaceTechnique = SCDWriter.SWAP_TABLE;
    
    /** The ms insert source columns. */
    private String msAllColumns, msInBatchSQLStatement, msInsertValues, msJoin, msTempTableName, mstrSchemaName,
            mstrTableName, msUpdateColumns, msUpdateTriggers, strDriverClass, strPassword, strPreSQL, strURL,
            strUserName, msInsertSourceColumns;
    
    /** The m source key specified. */
    private boolean mSourceKeySpecified = false;
    
    /** The ms pre load SQL. */
    private Object[] msPostLoadSQL, msPreLoadSQL = null;
    
    /** The ms join columns. */
    private String msJoinColumns;
    
    /** The jdbc helper. */
    private JDBCItemHelper jdbcHelper;

    /** The m used connections. */
    ArrayList mUsedConnections = new ArrayList();

    /** The mv column index. */
    HashMap mvColumnIndex = new HashMap();

    /** The mv columns. */
    Vector mvColumns = new Vector(); // for building the column list and later converting it into the array

    /** The mi analyze pos. */
    private int miAnalyzePos = -1;
    
    /** The m effective date column. */
    private String mEffectiveDateColumn;
    
    /** The m effective data port. */
    private JDBCETLInPort mEffectiveDataPort = null;
    
    /** The m delta tablename. */
    private String mDeltaTablename;
    
    /** The m keys. */
    private int mKeys = 0;

    /**
     * The Class JDBCETLInPort.
     */
    public class JDBCETLInPort extends ETLInPort {

        /**
         * The Class JDBCDatabaseColumnDefinition.
         */
        class JDBCDatabaseColumnDefinition extends DatabaseColumnDefinition {

            /* (non-Javadoc)
             * @see com.kni.etl.dbutils.DatabaseColumnDefinition#getSourceClass()
             */
            @Override
            public Class getSourceClass() {
                return JDBCETLInPort.this.getPortClass();
            }

            /**
             * Instantiates a new JDBC database column definition.
             * 
             * @param pNode the node
             * @param pColumnName the column name
             * @param pDataType the data type
             */
            public JDBCDatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType) {
                super(pNode, pColumnName, pDataType);
            }

        }

        /** The dcd new column. */
        DatabaseColumnDefinition dcdNewColumn;
        
        /** The m key. */
        private int mKey;

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlNode) throws ClassNotFoundException, KETLThreadException {

            super.initialize(xmlNode);

            // Create a new column definition with the default properties...
            this.dcdNewColumn = new JDBCDatabaseColumnDefinition(xmlNode, "", 0);

            NamedNodeMap attr = xmlNode.getAttributes();

            if (XMLHelper.getAttributeAsBoolean(attr, "EFFECTIVEDATE", false))
                SCDWriter.this.mEffectiveDataPort = this;

            // Get the column's target name...
            this.dcdNewColumn.setColumnName(this.getPortName());

            this.dcdNewColumn.setAlternateInsertValue(XMLHelper.getAttributeAsString(attr,
                    SCDWriter.ALTERNATE_INSERT_VALUE, null));
            this.dcdNewColumn.setAlternateUpdateValue(XMLHelper.getAttributeAsString(attr,
                    SCDWriter.ALTERNATE_UPDATE_VALUE, null));

            // Find out what the upsert flags are for this input...

            // Source key
            if ((this.mKey = XMLHelper.getAttributeAsInt(attr, SCDWriter.SK_ATTRIB, -1)) != -1) {
                this.dcdNewColumn.setProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY);
                SCDWriter.this.mSourceKeySpecified = true;
            }

            if (this.mKey != -1) {
                if (this.mKey < 1)
                    throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
                            + " KEY order starts at 1, invalid value of " + this.mKey, this);
                SCDWriter.this.mKeys++;
                this.mKey--;
            }

            // Insert field
            if (XMLHelper.getAttributeAsBoolean(attr, SCDWriter.INSERT_ATTRIB, true)) {
                this.dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN);
            }

            // Update field
            if (XMLHelper.getAttributeAsBoolean(attr, SCDWriter.UPDATE_ATTRIB, true)) {
                this.dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_COLUMN);
            }

            // Compare field, drives updates
            if (XMLHelper.getAttributeAsBoolean(attr, SCDWriter.COMPARE_ATTRIB, true)) {
                this.dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN);
            }

            this.dcdNewColumn.exists = false;
            // It's ok if not specified
            SCDWriter.this.mvColumns.add(this.dcdNewColumn);
            SCDWriter.this.mvColumnIndex.put(this.dcdNewColumn.getColumnName(null, SCDWriter.this.mDBCase),
                    this.dcdNewColumn);

            return 0;
        }

        /**
         * Instantiates a new JDBCETL in port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public JDBCETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /**
     * Builds the in batch SQL.
     * 
     * @param pTable the table
     * 
     * @return the string
     * 
     * @throws Exception the exception
     */
    abstract protected String buildInBatchSQL(String pTable) throws Exception;

    /**
     * Gets the dimension update SQL.
     * 
     * @param sql the sql
     * 
     * @return the dimension update SQL
     * 
     * @throws KETLThreadException the KETL thread exception
     * @throws SQLException the SQL exception
     */
    private void getDimensionUpdateSQL(ArrayList sql) throws KETLThreadException, SQLException {

        String expdtfunc = this.getStepTemplate(this.mDBType, "EXPDTFUNC", true);
        String template = this.getStepTemplate(this.mDBType, "CREATEINDEX", true);

        template = EngineConstants
                .replaceParameterV2(template, "TABLENAME", this.mstrSchemaName + this.msTempTableName);
        template = EngineConstants.replaceParameterV2(template, "INDEXNAME", this.getUniqueObjectName("idx"));
        template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.msJoinColumns);

        sql.add(template);

        String sqlToExecute = this.getStepTemplate(this.mDBType, "ANALYZETABLE", true);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mstrSchemaName
                + this.msTempTableName);
        sql.add(sqlToExecute);

        this.miAnalyzePos = sql.indexOf(sqlToExecute);

        sqlToExecute = this.getStepTemplate(this.mDBType, "ACTIVEDATASET", true);

        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DELTATABLENAME", this.mDeltaTablename);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDTFUNC", expdtfunc);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SERIALCOLUMN", "seqcol");
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDT", this.mExpirationDateColumn);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EFFDT", this.mEffectiveDataPort.mstrName);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NKEY", this.msTempTableName + "."
                + this.msJoinColumns.replace(",", "," + this.msTempTableName + "."));
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SKEY", this.mSurrogateKey);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCECOLUMNS", this.getAllColumns());
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NAMEDSOURCECOLUMNS", this.msTempTableName
                + "." + this.getAllColumns().replace(",", "," + this.msTempTableName + "."));
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTCOLUMNS", this.msInsertSourceColumns);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTDESTINATIONCOLUMNS",
                this.msInsertColumns);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                + this.msTempTableName);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME", this.mstrSchemaName
                + this.mstrTableName);

        sql.add(sqlToExecute);

        sqlToExecute = this.getStepTemplate(this.mDBType, "CREATEINDEX", true);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mDeltaTablename);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INDEXNAME", this.getUniqueObjectName("idxd"));
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "COLUMNS", "MATCH_SK");

        sql.add(sqlToExecute);

        sqlToExecute = this.getStepTemplate(this.mDBType, "UPDATEPOSTBATCH", true);
        if (sqlToExecute != null && sqlToExecute.equals("") == false) {
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDT", this.mExpirationDateColumn);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EFFDT", this.mEffectiveDataPort.mstrName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDTFUNC", expdtfunc);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SERIALCOLUMN", "seqcol");
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SKEY", this.mSurrogateKey);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DELTATABLENAME", this.mDeltaTablename);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                    + this.msTempTableName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME", this.mstrSchemaName
                    + this.mstrTableName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
            sql.add(sqlToExecute);
        }

        sqlToExecute = this.getStepTemplate(this.mDBType, "INSERTPOSTBATCH", true);
        if (sqlToExecute != null && sqlToExecute.equals("") == false) {
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDT", this.mExpirationDateColumn);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SKEY", this.mSurrogateKey);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EFFDT", this.mEffectiveDataPort.mstrName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SERIALCOLUMN", "seqcol");
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DELTATABLENAME", this.mDeltaTablename);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCECOLUMNS", this.getAllColumns());
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDTFUNC", expdtfunc);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NAMEDSOURCECOLUMNS", this.msTempTableName
                    + "." + this.getAllColumns().replace(",", "," + this.msTempTableName + "."));
            sqlToExecute = EngineConstants
                    .replaceParameterV2(sqlToExecute, "INSERTCOLUMNS", this.msInsertSourceColumns);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTDESTINATIONCOLUMNS",
                    this.msInsertColumns);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                    + this.msTempTableName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME", this.mstrSchemaName
                    + this.mstrTableName);
            sql.add(sqlToExecute);
        }

        sqlToExecute = this.getStepTemplate(this.mDBType, "DROPTABLE", true);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mDeltaTablename);

        sql.add(sqlToExecute);

    }

    /** The m index enable list. */
    private ArrayList mIndexEnableList = new ArrayList();
    
    /** The m index disable list. */
    private ArrayList mIndexDisableList = new ArrayList();

    /**
     * Builds the post load SQL.
     * 
     * @return the object[]
     * 
     * @throws Exception the exception
     */
    private Object[] buildPostLoadSQL() throws Exception {

        ArrayList sql = new ArrayList();

        this.getDimensionUpdateSQL(sql);
        String rollSQL = this.getStepTemplate(this.mDBType, "DROPTABLE", true);
        rollSQL = EngineConstants.replaceParameterV2(rollSQL, "TABLENAME", this.mstrSchemaName + this.msTempTableName);

        sql.add(rollSQL);
        return sql.toArray();

    }

    /**
     * Gets the failure cleanup load SQL.
     * 
     * @return the failure cleanup load SQL
     * 
     * @throws Exception the exception
     */
    private Object[] getFailureCleanupLoadSQL() throws Exception {

        ArrayList sql = new ArrayList();

        sql.add(EngineConstants.replaceParameterV2(this.getStepTemplate(this.mDBType, "DROPTABLE", true), "TABLENAME",
                this.mstrSchemaName + this.msTempTableName));

        return sql.toArray();

    }

    /**
     * Builds the pre load SQL.
     * 
     * @return the object[]
     * 
     * @throws Exception the exception
     */
    private Object[] buildPreLoadSQL() throws Exception {

        ArrayList sql = new ArrayList();

        String template = this.getStepTemplate(this.mDBType, "CREATETABLE", true);

        template = EngineConstants.replaceParameterV2(template, "NEWTABLENAME", this.mstrSchemaName
                + this.msTempTableName);
        template = EngineConstants.replaceParameterV2(template, "SOURCETABLENAME", this.mstrSchemaName
                + this.mstrTableName);
        template = EngineConstants.replaceParameterV2(template, "SOURCECOLUMNS", this.getAllColumns());

        template = EngineConstants.replaceParameterV2(template, "DEDUPECOLUMN", ",1 as seqcol");

        sql.add(template);

        return sql.toArray();
    }

    /** The stmt. */
    StatementWrapper stmt;
    
    /** The max char length. */
    private int maxCharLength;
    
    /** The m batch counter. */
    private int mBatchCounter;
    
    /** The fire pre batch. */
    private boolean firePreBatch;
    
    /** The mb reinit on error. */
    private boolean mbReinitOnError;

    /**
     * DOCUMENT ME!.
     * 
     * @return DOCUMENT ME!
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public int complete() throws KETLThreadException {
        int res = super.complete();

        try {
            this.stmt.close();
            this.stmt = null;
        } catch (Exception e) {
            ResourcePool.LogException(e, this);
        }

        if (res < 0) {
            ResourcePool
                    .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error during final batch, see previous messages");
        }
        else {
            try {
                this.executePostStatements();

                if (this.mIncrementalCommit == false) {
                    this.mcDBConnection.commit();
                }
            } catch (Exception e) {
                try {
                    StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
                            this.mStatementSeperator, StatementManager.END, this, true);
                } catch (Exception e1) {

                }
                throw new KETLThreadException("Running post load " + e.getMessage(), e, this);
            }
        }

        if (this.mcDBConnection != null) {
            ResourcePool.releaseConnection(this.mcDBConnection);
            this.mcDBConnection = null;
        }

        return res;
    }

    /** The lookup locked. */
    boolean lookupLocked = true;

    /**
     * Seed SCD lookup.
     * 
     * @throws SQLException the SQL exception
     * @throws KETLTransformException the KETL transform exception
     * @throws KETLThreadException the KETL thread exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void seedSCDLookup() throws SQLException, KETLTransformException, KETLThreadException, IOException {
        try {
            // download values to lookup
            // scd lookup format
            // key, result, paired array of surrogates
            Statement statement = this.mcDBConnection.createStatement();

            String sqlToExecute = this.getStepTemplate(this.mDBType, "QUERYFORKEYS", true);

            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME", this.mstrSchemaName
                    + this.mstrTableName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EFFDT", this.mEffectiveDataPort.mstrName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "EXPDT", this.mExpirationDateColumn);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NKEY", this.msJoinColumns);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SKEY", this.mSurrogateKey);

            ResultSet rs = statement.executeQuery(sqlToExecute);

            if (this.types == null)
                this.types = this.mLookup.getKeyTypes();

            Object[] key = new Object[this.types.length];
            Object[] previousKey = null;
            ArrayList skeys = new ArrayList();
            ArrayList skeyEffDates = new ArrayList();
            ArrayList skeyExpDates = new ArrayList();

            while (rs.next()) {
                int pos = 1;
                for (int i = 0; i < this.mKeys; i++) {
                    Object data = this.jdbcHelper.getObjectFromResultSet(rs, pos++, this.types[i], this.maxCharLength);
                    if (data == null)
                        throw new KETLTransformException("NULL values are not allowed in the key table, check table "
                                + this.mstrTableName + " for errors");
                    key[i] = data;
                }

                if (previousKey == null) {
                    previousKey = key;
                    key = new Object[this.types.length];
                }
                else if (java.util.Arrays.equals(key, previousKey) == false) {
                    // record key
                    this.putSCDKey(previousKey, skeyEffDates, skeyExpDates, skeys);
                    skeys.clear();
                    skeyEffDates.clear();
                    skeyExpDates.clear();
                    previousKey = key;
                    key = new Object[this.types.length];
                }

                skeys.add(rs.getInt(pos++));
                skeyEffDates.add(rs.getTimestamp(pos++));
                skeyExpDates.add(rs.getTimestamp(pos++));

            }

            this.putSCDKey(previousKey, skeyEffDates, skeyExpDates, skeys);

            rs.close();
            statement.close();

            this.mLookup.commit(true);

            if (this.lookupLocked)
                ((KETLJob) this.getJobExecutor().getCurrentETLJob()).releaseLookupWriteLock(this.getName(), this);

        } catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Gets the all other table columns.
     * 
     * @return the all other table columns
     * 
     * @throws SQLException the SQL exception
     */
    private String[] getAllOtherTableColumns() throws SQLException {
        Statement stmt = this.mcDBConnection.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + this.mstrSchemaName + this.mstrTableName + " WHERE 1 = 0");

        ResultSetMetaData md = rs.getMetaData();

        ArrayList ar = new ArrayList();

        for (int i = 1; i <= md.getColumnCount(); i++) {
            String col = this.setDBCase(md.getColumnName(i));

            if (this.mvColumnIndex.containsKey(col) == false) {
                ar.add(col);
            }
        }

        String[] res = new String[ar.size()];

        ar.toArray(res);

        rs.close();
        stmt.close();

        return res;
    }

    /**
     * Gets the column data types.
     * 
     * @return the column data types
     * 
     * @throws SQLException the SQL exception
     */
    private void getColumnDataTypes() throws SQLException {
        ResultSet rs = this.mcDBConnection.getMetaData().getColumns(null,
                XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), SCDWriter.SCHEMA_ATTRIB, null),
                this.mstrTableName, "%");

        boolean found = false;
        while (rs.next()) {
            found = true;
            for (int i = 0; i < this.mvColumns.size(); i++) {
                DatabaseColumnDefinition dc = (DatabaseColumnDefinition) this.mvColumns.get(i);

                if (rs.getString(4).equalsIgnoreCase(dc.getColumnName(null, this.mDBCase))) {
                    dc.iSQLDataType = rs.getInt(5);
                    dc.sTypeName = rs.getString("TYPE_NAME");
                    dc.iSize = rs.getInt("COLUMN_SIZE");
                    dc.iPrecision = rs.getInt("DECIMAL_DIGITS");
                    dc.exists = true;
                    for (int x = 1; x <= rs.getMetaData().getColumnCount(); x++) {
                        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, rs.getMetaData().getColumnName(x)
                                + ": " + rs.getString(x));
                    }
                }
            }
        }

        if (rs != null) {
            rs.close();
        }

        if (found == false)
            throw new SQLException("Target table " + this.mstrTableName + " was not found");

    }

    /** The m temp table feed. */
    static private int mTempTableFeed = 0;

    /**
     * Gets the unique object name.
     * 
     * @param pPrefix the prefix
     * 
     * @return the unique object name
     * 
     * @throws SQLException the SQL exception
     */
    private String getUniqueObjectName(String pPrefix) throws SQLException {
        boolean notFound = true;
        String res = null;
        int x = 0;

        // check for temp table existance
        while (notFound) {
            res = this.setDBCase(pPrefix + Integer.toString(x++) + "_" + SCDWriter.mTempTableFeed++);
            notFound = false;

            ResultSet rs = this.mcDBConnection.getMetaData().getTables("%", "%", res, null);

            if (rs != null) {
                while (rs.next()) {
                    notFound = true;
                }

                rs.close();
            }
        }

        return res;
    }

    /** The id quote. */
    private String idQuote;
    
    /** The id quote enabled. */
    private boolean idQuoteEnabled = false;
    
    /** The m statement seperator. */
    private String mStatementSeperator;
    
    /** The ms best join. */
    private String msBestJoin;
    
    /** The mb ignore invalid columns. */
    private boolean mbIgnoreInvalidColumns;
    
    /** The ms insert columns. */
    private String msInsertColumns;
    
    /** The m first SK. */
    private String mFirstSK;
    
    /** The m manage indexes. */
    private boolean mManageIndexes;
    
    /** The m surrogate key. */
    private String mSurrogateKey;
    
    /** The m expiration date column. */
    private String mExpirationDateColumn;

    /**
     * Gets the ID quote.
     * 
     * @return the ID quote
     */
    protected String getIDQuote() {
        if (this.idQuoteEnabled)
            return this.idQuote;

        return null;
    }

    /**
     * DOCUMENT ME!.
     * 
     * @param nConfig DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public int initialize(Node nConfig) throws KETLThreadException {
        int res = super.initialize(nConfig);

        if (res != 0) {
            return res;
        }

        // Pull the parameters from the list...
        this.strUserName = this.getParameterValue(0, DBConnection.USER_ATTRIB);
        this.strPassword = this.getParameterValue(0, DBConnection.PASSWORD_ATTRIB);
        this.strURL = this.getParameterValue(0, DBConnection.URL_ATTRIB);
        this.strDriverClass = this.getParameterValue(0, DBConnection.DRIVER_ATTRIB);
        this.strPreSQL = this.getParameterValue(0, DBConnection.PRESQL_ATTRIB);

        // Get the attributes
        NamedNodeMap nmAttrs = nConfig.getAttributes();

        // pull parameters from step definition
        this.mbReinitOnError = XMLHelper.getAttributeAsBoolean(nmAttrs, "RECONNECTONERROR", true);
        this.mManageIndexes = XMLHelper.getAttributeAsBoolean(nmAttrs, "MANAGEINDEXES", false);
        this.mbIgnoreInvalidColumns = XMLHelper.getAttributeAsBoolean(nmAttrs, SCDWriter.IGNOREINVALIDCOLUMNS_ATTRIB,
                false);
        this.mBatchData = XMLHelper.getAttributeAsBoolean(nmAttrs, SCDWriter.BATCH_ATTRIB, this.mBatchData);
        this.mSurrogateKey = XMLHelper.getAttributeAsString(nmAttrs, SCDWriter.SURROGATE_KEY_ATTRIB, null);
        this.mExpirationDateColumn = XMLHelper.getAttributeAsString(nmAttrs, "EXPIRATIONDATECOLUMN", null);

        this.idQuoteEnabled = XMLHelper.getAttributeAsBoolean(nmAttrs, "IDQUOTE", false);
        this.jdbcHelper = this.instantiateHelper(XMLHelper
                .getAttributeAsString(nmAttrs, SCDWriter.HANDLER_ATTRIB, null));

        if (this.mExpirationDateColumn == null)
            throw new KETLThreadException(
                    "Expiration date column name not supplied, add EXPIRATIONDATECOLUMN attribute to step definition",
                    this);

        if (this.mEffectiveDataPort == null)
            throw new KETLThreadException(
                    "Effective date port not supplied, add EFFECTIVEDATE=\"TRUE\" to appropiate port", this);

        if (this.mSurrogateKey == null)
            throw new KETLThreadException(
                    "Surrogate key name not supplied, add SURROGATEKEY attribute to step definition", this);

        try {
            this.mcDBConnection = ResourcePool.getConnection(this.strDriverClass, this.strURL, this.strUserName,
                    this.strPassword, this.strPreSQL, true);

            this.mDBType = this.mcDBConnection.getMetaData().getDatabaseProductName();
            this.mUsedConnections.add(this.mcDBConnection);

            DatabaseMetaData md = this.mcDBConnection.getMetaData();

            this.idQuote = md.getIdentifierQuoteString();
            if (this.idQuote == null || this.idQuote.equals(" "))
                this.idQuote = "";

            if (md.storesUpperCaseIdentifiers()) {
                this.mDBCase = SCDWriter.UPPER_CASE;
            }
            else if (md.storesLowerCaseIdentifiers()) {
                this.mDBCase = SCDWriter.LOWER_CASE;
            }
            else if (md.storesMixedCaseIdentifiers()) {
                this.mDBCase = SCDWriter.MIXED_CASE;

            }
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        // Pull the name of the table to be written to...
        this.mstrTableName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, SCDWriter.TABLE_ATTRIB, null));
        this.mstrSchemaName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, SCDWriter.SCHEMA_ATTRIB, null));

        if (this.mstrSchemaName == null)
            this.mstrSchemaName = "";
        else
            this.mstrSchemaName = this.mstrSchemaName + ".";

        // Pull the commit size...
        this.miCommitSize = XMLHelper.getAttributeAsInt(nmAttrs, SCDWriter.COMMITSIZE_ATTRIB, 20000);
        this.mLowMemoryThreashold = this.miCommitSize * 100 * this.mInPorts.length;
        this.miMaxTransactionSize = XMLHelper.getAttributeAsInt(nmAttrs, SCDWriter.MAXTRANSACTIONSIZE_ATTRIB, -1);
        this.mStatementSeperator = this.getStepTemplate(this.mDBType, "STATEMENTSEPERATOR", true);
        if (this.mStatementSeperator != null && this.mStatementSeperator.length() == 0)
            this.mStatementSeperator = null;

        // Convert the vector we've been building into a more common array...
        this.madcdColumns = (DatabaseColumnDefinition[]) this.mvColumns.toArray(new DatabaseColumnDefinition[0]);

        this.setSerialKeyStartValue();

        // get column datatype from the database
        try {
            this.getColumnDataTypes();

            int joinKey = -1, bestJoinKey = -1;
            // review pk choice instead of sk for updates and inserts
            if (this.mSourceKeySpecified) {
                joinKey = DatabaseColumnDefinition.SRC_UNIQUE_KEY;
                bestJoinKey = bestJoinKey == -1 ? joinKey : bestJoinKey;
            }

            StringBuffer updateColumns = new StringBuffer();
            StringBuffer insertColumns = new StringBuffer();
            StringBuffer insertSourceColumns = new StringBuffer();
            StringBuffer allColumns = new StringBuffer();
            StringBuffer join = new StringBuffer();
            StringBuffer bestjoin = new StringBuffer();
            StringBuffer updateTriggers = new StringBuffer();
            StringBuffer joinColumns = new StringBuffer();
            StringBuffer insertValues = new StringBuffer();

            String updColFormat = this.getStepTemplate(this.mDBType, "UPDATECOLUMNFORMAT", true);

            ArrayList fieldPopulationOrder = new ArrayList();

            int cntJoinColumns = 0, cntInsertColumns = 0, cntUpdateTriggers = 0, cntUpdateCols = 0, cntBestJoinColumns = 0, allColumnCount = 0;

            for (int i = 0; i < this.madcdColumns.length; i++) {

                if (this.mbIgnoreInvalidColumns && this.madcdColumns[i].exists == false) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Column "
                            + this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase)
                            + " not found, skipping");
                    continue;
                }

                if (bestJoinKey != -1 && this.madcdColumns[i].hasProperty(bestJoinKey)) {

                }
                else if (joinKey != -1 && this.madcdColumns[i].hasProperty(joinKey) == false) {

                    if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.UPDATE_COLUMN)) {
                        if (cntUpdateCols > 0) {
                            updateColumns.append(",\n\t");
                        }
                        cntUpdateCols++;

                        String tmp = EngineConstants.replaceParameterV2(updColFormat, "TARGETCOLUMN",
                                this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));

                        if (this.madcdColumns[i].getAlternateUpdateValue() == null) {
                            tmp = EngineConstants.replaceParameterV2(tmp, "SOURCECOLUMN", "${SOURCETABLENAME}."
                                    + this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        }
                        else {
                            tmp = EngineConstants.replaceParameterV2(tmp, "SOURCECOLUMN", this.madcdColumns[i]
                                    .getAlternateUpdateValue());
                        }

                        updateColumns.append(tmp);

                    }

                    if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN)) {
                        if (cntUpdateTriggers > 0) {
                            updateTriggers.append(" OR ");
                        }
                        cntUpdateTriggers++;

                        updateTriggers.append("((${DESTINATIONTABLENAME}.");
                        updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        updateTriggers.append(" != ${SOURCETABLENAME}.");
                        updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        updateTriggers.append(") OR (");
                        updateTriggers.append("${DESTINATIONTABLENAME}.");
                        updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is null and ${SOURCETABLENAME}.");
                        updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is not null");
                        updateTriggers.append(") OR (");
                        updateTriggers.append("${DESTINATIONTABLENAME}.");
                        updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is not null and ${SOURCETABLENAME}.");
                        updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is null))");
                    }
                }

                if (bestJoinKey != -1 && this.madcdColumns[i].hasProperty(bestJoinKey)) {

                    if (cntBestJoinColumns > 0) {
                        bestjoin.append("\n\tAND ");
                    }
                    else {
                        this.mFirstSK = " ${DESTINATIONTABLENAME}."
                                + this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase);
                    }

                    cntBestJoinColumns++;

                    bestjoin.append(" ${DESTINATIONTABLENAME}.");
                    bestjoin.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                    bestjoin.append(" = ${SOURCETABLENAME}.");
                    bestjoin.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                }

                if (joinKey != -1 && this.madcdColumns[i].hasProperty(joinKey)) {

                    if (cntJoinColumns > 0) {
                        joinColumns.append(',');
                        join.append("\n\tAND ");
                    }

                    cntJoinColumns++;

                    joinColumns.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                    join.append(" ${DESTINATIONTABLENAME}.");
                    join.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                    join.append(" = ${SOURCETABLENAME}.");
                    join.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                }

                if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.INSERT_COLUMN)) {
                    if (cntInsertColumns > 0) {
                        insertColumns.append(',');
                        insertSourceColumns.append(',');
                    }

                    insertColumns.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));

                    if (this.madcdColumns[i].getAlternateInsertValue() == null)
                        insertSourceColumns.append("${SOURCETABLENAME}."
                                + this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                    else
                        insertSourceColumns.append(this.madcdColumns[i].getAlternateInsertValue());

                    cntInsertColumns++;
                }

                if (allColumnCount > 0) {
                    allColumns.append(',');
                    insertValues.append(',');
                }

                allColumnCount++;
                allColumns.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
                insertValues.append('?');
                fieldPopulationOrder.add(new Integer(i));

            }

            fieldPopulationOrder.add(fieldPopulationOrder.size());
            insertValues.append(",?");

            this.miFieldPopulationOrder = new int[fieldPopulationOrder.size()];
            for (int i = 0; i < fieldPopulationOrder.size(); i++) {
                this.miFieldPopulationOrder[i] = ((Integer) fieldPopulationOrder.get(i)).intValue();
            }

            this.msUpdateColumns = updateColumns.toString();
            this.setAllColumns(allColumns.toString());
            this.msJoin = join.toString();
            this.msBestJoin = bestjoin.toString();
            this.msInsertColumns = insertColumns.toString();
            this.msInsertSourceColumns = insertSourceColumns.toString();
            this.setInsertValues(insertValues.toString());
            this.msJoinColumns = joinColumns.toString();
            this.msUpdateTriggers = (cntUpdateTriggers == 0 ? "1=0" : updateTriggers.toString());
            this.getAllIndexes(XMLHelper.getAttributeAsString(nmAttrs, "JOININDEX", null));

            // if roll then table needs to be created to dump data too
            if (this.mSourceKeySpecified == false) {
                throw new KETLThreadException(
                        "Insert type requires source keys to be specified, either specify them or switch to BULK insert",
                        this);
            }

            this.msTempTableName = this.getUniqueObjectName("t");
            this.mDeltaTablename = this.getUniqueObjectName("td");
            this.maOtherColumns = this.getAllOtherTableColumns();

            this.msPreLoadSQL = this.buildPreLoadSQL();
            this.msPostLoadSQL = this.buildPostLoadSQL();
            this.msInBatchSQLStatement = this.buildInBatchSQL(this.mstrSchemaName + this.msTempTableName);

            if (this.debug())
                ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Insert statement: "
                        + this.msInBatchSQLStatement);
            this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement, this.jdbcHelper);

            this.maxCharLength = this.mcDBConnection.getMetaData().getMaxCharLiteralLength();
            this.supportsSetSavepoint = this.mcDBConnection.getMetaData().supportsSavepoints();

            if (this.supportsSetSavepoint) {
                Savepoint sPoint = null;
                try {
                    sPoint = this.mcDBConnection.setSavepoint();
                } catch (SQLException e) {
                    this.supportsSetSavepoint = false;
                }

                if (sPoint != null) {
                    try {
                        this.mcDBConnection.releaseSavepoint(sPoint);
                        this.supportsReleaseSavepoint = true;
                    } catch (SQLException e) {
                        this.supportsReleaseSavepoint = false;
                    }
                }

            }

            this.mIncrementalCommit = XMLHelper.getAttributeAsBoolean(nmAttrs, "INCREMENTALCOMMIT", true);

            if (this.mIncrementalCommit == false && this.supportsSetSavepoint == false) {
                throw new KETLThreadException(
                        "Incremental commit cannot be disabled for database's that do not support savepoints", this);
            }
            this.miRetryBatch = XMLHelper.getAttributeAsInt(nmAttrs, "RETRYBATCH", 1);

            this.executePreStatements();

        } catch (Exception e) {
            try {
                StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
                        this.mStatementSeperator, StatementManager.END, this, true);
            } catch (Exception e1) {

            }
            throw new KETLThreadException(e, this);
        }

        this.prepareSCDLookupCache();
        return 0;
    }

    /** The m cache persistence ID. */
    private Integer mCachePersistenceID = -1;

    /** The m cache size. */
    private int mCacheSize;

    /** The cache persistence. */
    private int cachePersistence;

    /**
     * Prepare SCD lookup cache.
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    private void prepareSCDLookupCache() throws KETLThreadException {
        int minSize = NumberFormatter.convertToBytes(EngineConstants.getDefaultCacheSize());
        this.cachePersistence = EngineConstants.JOB_PERSISTENCE;

        String tmp = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "PERSISTENCE", null);
        if (tmp == null || tmp.equalsIgnoreCase("JOB")) {
            this.mCachePersistenceID = ((Long) this.getJobExecutionID()).intValue();
            this.cachePersistence = EngineConstants.JOB_PERSISTENCE;
        }
        else if (tmp.equalsIgnoreCase("LOAD")) {
            this.mCachePersistenceID = this.mkjExecutor.getCurrentETLJob().getLoadID();
            this.cachePersistence = EngineConstants.LOAD_PERSISTENCE;
        }
        else if (tmp.equalsIgnoreCase("STATIC")) {
            this.cachePersistence = EngineConstants.STATIC_PERSISTENCE;
            this.mCachePersistenceID = null;
        }
        else
            throw new KETLThreadException("PERSISTENCE has to be either JOB,LOAD or STATIC", this);

        this.mCacheSize = NumberFormatter.convertToBytes(XMLHelper.getAttributeAsString(this.getXMLConfig()
                .getAttributes(), "CACHESIZE", null));

        if (this.mCacheSize == -1)
            this.mCacheSize = minSize;
        if (this.mCacheSize < minSize) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                    "Cache cannot be less than 64kb, defaulting to 64kb");
            this.mCacheSize = minSize;
        }

        if (this.mKeys == 0) {
            throw new KETLThreadException("No keys have been specified", this);
        }

        if (this.mKeys > 4) {
            ResourcePool
                    .LogMessage(this, ResourcePool.INFO_MESSAGE,
                            "Currently lookups are limited to no more than 4 keys, unless you use a an array object to represent the compound key");
        }

        this.mLookup = ((KETLJob) this.getJobExecutor().getCurrentETLJob()).registerLookupWriteLock(this.getName(),
                this, this.cachePersistence);
        this.lookupLocked = true;

    }

    /** The m lookup. */
    private PersistentMap mLookup;

    /**
     * Sets the serial key start value.
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    private void setSerialKeyStartValue() throws KETLThreadException {

        this.dedupeCounter = this.getJobExecutor().getCurrentETLJob().getCounter(
                this.getName() + this.getJobExecutionID() + this.mstrTableName);

        synchronized (this.dedupeCounter) {
            if (this.dedupeCounter.value() == 0) {

                String sql = this.getStepTemplate(this.mDBType, "MAXSURROGATEKEY", true);
                sql = EngineConstants.replaceParameterV2(sql, "TABLENAME", this.mstrSchemaName + this.mstrTableName);
                sql = EngineConstants.replaceParameterV2(sql, "SKEY", this.mSurrogateKey);

                Statement statement = null;
                try {
                    statement = this.mcDBConnection.createStatement();

                    ResultSet rs = statement.executeQuery(sql);
                    while (rs.next()) {
                        this.dedupeCounter.set(rs.getInt(1));
                    }

                    rs.close();
                } catch (SQLException e) {
                    throw new KETLThreadException(e, this);
                } finally {

                    if (statement != null)
                        try {
                            statement.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                }
            }
        }
    }

    /**
     * Gets the all indexes.
     * 
     * @param string the string
     * 
     * @return the all indexes
     * 
     * @throws SQLException the SQL exception
     * @throws KETLThreadException the KETL thread exception
     */
    private void getAllIndexes(String string) throws SQLException, KETLThreadException {

        if (this.mManageIndexes == false)
            return;

        ResultSet indexRs = this.mcDBConnection.getMetaData().getIndexInfo(null,
                this.mstrSchemaName.equals("") ? null : this.mstrSchemaName, this.mstrTableName, false, true);
        ArrayList indexList = new ArrayList();
        while (indexRs.next()) {
            String idxName = indexRs.getString(6);

            if (idxName != null && idxName.equalsIgnoreCase(string) == false)
                indexList.add(idxName);
        }

        indexRs.close();

        if (this.partitionID == 0) {
            String idxEnable = this.getStepTemplate(this.mDBType, "ENABLEINDEX", true);

            if (idxEnable != null && idxEnable.length() > 0) {
                for (Object o : indexList) {
                    this.mIndexEnableList.add(EngineConstants.replaceParameterV2(idxEnable, "INDEXNAME", (String) o));
                }
            }
        }

        if (this.partitionID == 0) {
            String idxDisable = this.getStepTemplate(this.mDBType, "DISABLEINDEX", true);

            this.mIndexDisableList.add("alter session set skip_unusable_indexes=true");
            if (idxDisable != null && idxDisable.length() > 0) {
                for (Object o : indexList) {
                    this.mIndexDisableList.add(EngineConstants.replaceParameterV2(idxDisable, "INDEXNAME", (String) o));
                }
            }
        }
    }

    /**
     * Instantiate helper.
     * 
     * @param hdl the hdl
     * 
     * @return the JDBC item helper
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    abstract protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException;

    /**
     * Sets the DB case.
     * 
     * @param pStr the str
     * 
     * @return the string
     */
    private String setDBCase(String pStr) {

        if (pStr == null)
            return null;

        switch (this.mDBCase) {
        case LOWER_CASE:
            return pStr.toLowerCase();

        case MIXED_CASE:
            return pStr;

        case UPPER_CASE:
            return pStr.toUpperCase();
        }

        return pStr;
    }

    /** The dedupe counter. */
    private SharedCounter dedupeCounter = null;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
     */
    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {

        int res = 1;
        try {
            int cols = this.miFieldPopulationOrder.length;
            for (int i = 1; i <= cols; i++) {
                if (i == cols) {
                    this.stmt.setParameterFromClass(i, Integer.class, this.dedupeCounter.increment(1),
                            this.maxCharLength, null);
                }
                else {
                    Class datumClass;
                    ETLInPort port = this.mInPorts[this.miFieldPopulationOrder[i - 1]];
                    int idx = -1;
                    if (port.isConstant())
                        datumClass = port.getPortClass();
                    else {
                        idx = port.getSourcePortIndex();
                        datumClass = pExpectedDataTypes[idx];
                    }

                    try {
                        this.stmt.setParameterFromClass(i, datumClass, port.isConstant() ? port.getConstantValue()
                                : pInputRecords[idx], this.maxCharLength, port.getXMLConfig());
                    } catch (ClassCastException e1) {
                        throw new KETLWriteException("Error with port "
                                + port.mstrName
                                + " expected datatype "
                                + datumClass.getCanonicalName()
                                + " incoming datatype was "
                                + (port.isConstant() ? port.getPortClass() : pInputRecords[idx].getClass()
                                        .getCanonicalName()));
                    }

                }

            }

            if (this.mBatchData) {
                this.stmt.addBatch();
                this.logBatch(pInputRecords);

                this.mBatchCounter++;
            }
            else {
                res = this.stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new KETLWriteException(e);
        }

        return res;
    }

    /** The m batch log. */
    ArrayList mBatchLog = new ArrayList();
    
    /** The record num batch start. */
    int recordNumBatchStart;

    /**
     * Log batch.
     * 
     * @param inputRecords the input records
     */
    private void logBatch(Object[] inputRecords) {
        this.mBatchLog.add(inputRecords);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.DBConnection#getConnection()
     */
    public Connection getConnection() {
        return this.mcDBConnection;
    }

    /** The mi retry batch. */
    int miRetryBatch = 0;

    /** The m incremental commit. */
    boolean mIncrementalCommit = true;

    /**
     * Prepare statement wrapper.
     * 
     * @param Connection the connection
     * @param sql the sql
     * @param jdbcHelper the jdbc helper
     * 
     * @return the statement wrapper
     * 
     * @throws SQLException the SQL exception
     */
    abstract StatementWrapper prepareStatementWrapper(Connection Connection, String sql, JDBCItemHelper jdbcHelper)
            throws SQLException;

    /**
     * Retry batch.
     * 
     * @return the int
     * 
     * @throws KETLWriteException the KETL write exception
     */
    private int retryBatch() throws KETLWriteException {
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                "Retrying records in batch, to identify invalid records");
        int result = 0;
        for (int r = 0; r < this.miRetryBatch; r++) {
            int errorCount = 0, submitted = 0;

            // reset statement as some drivers fail after a failure has occured
            try {
                this.stmt.close();
                this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement,
                        this.jdbcHelper);
            } catch (SQLException e) {
                throw new KETLWriteException(e);
            }
            for (int x = 0; x < this.mBatchLog.size(); x++) {
                Object[] record = (Object[]) this.mBatchLog.get(x);

                if (this.mFailedBatchElements.contains(x)) {
                    try {
                        if (this.mbReinitOnError) {
                            this.stmt.close();
                            this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement,
                                    this.jdbcHelper);
                        }

                        int cols = this.miFieldPopulationOrder.length;
                        for (int i = 1; i <= cols; i++) {
                            if (i == cols) {
                                this.stmt.setParameterFromClass(i, Integer.class, this.dedupeCounter.increment(1),
                                        this.maxCharLength, null);
                            }
                            else {
                                Class datumClass;
                                ETLInPort port = this.mInPorts[this.miFieldPopulationOrder[i - 1]];
                                int idx = -1;
                                if (port.isConstant())
                                    datumClass = port.getPortClass();
                                else {
                                    idx = port.getSourcePortIndex();
                                    datumClass = this.getExpectedDataTypes()[idx];
                                }

                                this.stmt.setParameterFromClass(i, datumClass, port.isConstant() ? port
                                        .getConstantValue() : record[idx], this.maxCharLength, port.getXMLConfig());
                            }
                        }

                        submitted++;
                        this.stmt.executeUpdate();
                        result++;
                        this.mFailedBatchElements.remove(x);
                        if (this.mIncrementalCommit)
                            this.mcDBConnection.commit();

                    } catch (SQLException e) {
                        errorCount++;
                        if (r == this.miRetryBatch - 1)
                            this.incrementErrorCount(new KETLWriteException("Record " + (this.miInsertCount + x + 1)
                                    + " failed to submit, " + e.toString(), e), record, this.miInsertCount + x + 1);

                        try {
                            this.stmt.close();
                            this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement,
                                    this.jdbcHelper);
                        } catch (SQLException e1) {
                            throw new KETLWriteException(e1);
                        }

                    }
                }
            }

            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Batch retry attempt " + (r + 1) + " of "
                    + this.miRetryBatch + ", Records resubmitted: " + submitted + ", errors: " + errorCount);
        }

        return result;

    }

    /** The m low memory threashold. */
    long mLowMemoryThreashold = -1;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.WriterBatchManager#finishBatch(int)
     */
    public int finishBatch(int len) throws KETLWriteException {
        int result = 0;
        try {
            if (this.mBatchData
                    && (this.mBatchCounter >= this.miCommitSize
                            || (this.mBatchCounter > 0 && this.isMemoryLow(this.mLowMemoryThreashold)) || (len == BatchManager.LASTBATCH && this.mBatchCounter > 0))) {
                boolean errorsOccured = false;
                Savepoint savepoint = null;
                try {

                    if (this.supportsSetSavepoint) {
                        savepoint = this.mcDBConnection.setSavepoint();
                    }

                    Exception e1 = null;
                    int[] res = null;
                    try {
                        res = this.stmt.executeBatch();

                        if (this.supportsReleaseSavepoint && savepoint != null) {
                            this.mcDBConnection.releaseSavepoint(savepoint);
                        }

                    } catch (BatchUpdateException e) {
                        if (savepoint != null)
                            this.mcDBConnection.rollback(savepoint);
                        else
                            res = e.getUpdateCounts();
                        e1 = e;
                        errorsOccured = true;
                    }

                    if (errorsOccured && res == null) {
                        for (int i = 0; i < this.mBatchLog.size(); i++) {
                            if (this.miRetryBatch == 0)
                                this.incrementErrorCount(e1 == null ? new KETLWriteException("Failed to submit record "
                                        + (i + 1 + this.miInsertCount)) : new KETLWriteException(e1),
                                        (Object[]) this.mBatchLog.get(i), i + 1 + this.miInsertCount);
                            else
                                this.mFailedBatchElements.add(i);
                        }
                    }
                    else {
                        int rLen = res.length;
                        for (int i = 0; i < rLen; i++) {
                            if (res[i] == Statement.EXECUTE_FAILED) {
                                this.mFailedBatchElements.add(i);
                                if (this.miRetryBatch == 0)
                                    this.incrementErrorCount(e1 == null ? new KETLWriteException(
                                            "Failed to submit record " + (i + 1 + this.miInsertCount))
                                            : new KETLWriteException(e1), (Object[]) this.mBatchLog.get(rLen), i + 1
                                            + this.miInsertCount);
                            }
                            else {
                                result += res[i] >= 0 ? res[i] : 1;
                            }
                        }
                    }
                } catch (SQLException e) {
                    throw new KETLWriteException(e);
                }

                if (errorsOccured && this.miRetryBatch > 0) {
                    result = this.retryBatch();
                }

                this.clearBatchLogBatch();

                this.miInsertCount += this.mBatchCounter;
                this.mBatchCounter = 0;

                if (this.mIncrementalCommit)
                    this.mcDBConnection.commit();
                this.executePostBatchStatements();
                this.firePreBatch = true;

            }
            else if (this.mBatchData == false) {
                if (this.mIncrementalCommit)
                    this.mcDBConnection.commit();
            }

        } catch (SQLException e) {
            try {
                // StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
                // mStatementSeperator, StatementManager.END, this, true);
            } catch (Exception e1) {

            }

            KETLWriteException e1 = new KETLWriteException(e);
            this.incrementErrorCount(e1, null, 1);

            throw e1;
        }

        return result;
    }

    /** The m failed batch elements. */
    private Set mFailedBatchElements = new HashSet();

    /**
     * Clear batch log batch.
     */
    private void clearBatchLogBatch() {
        this.mBatchLog.clear();
        this.mFailedBatchElements.clear();
    }

    /** The supports release savepoint. */
    boolean supportsSetSavepoint = false, supportsReleaseSavepoint = false;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.WriterBatchManager#initializeBatch(java.lang.Object[][], int)
     */
    public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {
        try {
            if (this.firePreBatch && this.mBatchData) {
                this.executePreBatchStatements();
                this.recordNumBatchStart = this.getRecordsProcessed();
                this.firePreBatch = false;
            }

        } catch (SQLException e) {
            throw new KETLWriteException(e);
        }
        return data;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePostStatements()
     */
    public void executePostStatements() throws SQLException {
        this.setWaiting("post statements to run");
        StatementManager.executeStatements(this.msPostLoadSQL, this.mcDBConnection, this.mStatementSeperator,
                StatementManager.END, this, false);

        if (this.miAnalyzePos != -1) {
            // remove analyzes they should only happen once
            this.msPostLoadSQL[this.miAnalyzePos] = null;
            this.miAnalyzePos = -1;
        }

        if (this.isLastThreadToEnterCompletePhase()) {
            // wait for all other threads to complete
            this.setWaiting("all other threads in group to complete");
            while (this.getThreadManager().countOfStepThreadsAlive(this) > 1) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new KETLError(e);
                }
            }

            this.setWaiting("indexes to rebuild");
            StatementManager.executeStatements(this.mIndexEnableList.toArray(), this.mcDBConnection,
                    this.mStatementSeperator, StatementManager.END, this, false);

            this.setWaiting("lookup to seed");

            try {
                this.seedSCDLookup();
            } catch (Exception e) {
                e.printStackTrace();
                SQLException e1 = new SQLException(e.getMessage());
                e.setStackTrace(e.getStackTrace());
                throw e1;
            }

        }
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
        this.setWaiting(null);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
     */
    public void executePreStatements() throws SQLException {
        this.setWaiting("pre statements to run");
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
        StatementManager.executeStatements(this.msPreLoadSQL, this.mcDBConnection, this.mStatementSeperator,
                StatementManager.START, this, false);

        if (this.isFirstThreadToEnterInitializePhase()) {
            this.setWaiting("indexes to be disabled");
            StatementManager.executeStatements(this.mIndexDisableList.toArray(), this.mcDBConnection,
                    this.mStatementSeperator, StatementManager.END, this, false);
        }
        this.setWaiting(null);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePostBatchStatements()
     */
    public void executePostBatchStatements() throws SQLException {
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
     */
    public void executePreBatchStatements() throws SQLException {
        this.setWaiting("pre batch statements to run");
        StatementManager.executeStatements(this, this, "PREBATCHSQL");
        this.setWaiting(null);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new JDBCETLInPort(this, srcStep);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {
        try {

            if (this.lookupLocked)
                ((KETLJob) this.getJobExecutor().getCurrentETLJob()).releaseLookupWriteLock(this.getName(), this);

            if (this.mcDBConnection != null && this.mIncrementalCommit == false && success == false
                    && this.getRecordsProcessed() > 0) {
                this.mcDBConnection.rollback();
            }

        } catch (SQLException e) {
            ResourcePool.LogException(e, this);
        }
        try {

            if (this.stmt != null)
                this.stmt.close();
        } catch (SQLException e) {
            ResourcePool.LogException(e, this);
        }
        if (this.mcDBConnection != null) {
            if (success == false && this.debug() == false) {
                try {
                    StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
                            this.mStatementSeperator, StatementManager.END, this, true);
                } catch (Exception e1) {
                }
            }
            ResourcePool.releaseConnection(this.mcDBConnection);
        }

        if (this.cachePersistence == EngineConstants.JOB_PERSISTENCE) {
            ((KETLJob) this.getJobExecutor().getCurrentETLJob()).deleteLookup(this.getName());
        }
    }

    /**
     * Sets the all columns.
     * 
     * @param msAllColumns the new all columns
     */
    void setAllColumns(String msAllColumns) {
        this.msAllColumns = msAllColumns;
    }

    /**
     * Gets the all columns.
     * 
     * @return the all columns
     */
    String getAllColumns() {
        return this.msAllColumns;
    }

    /**
     * Sets the insert values.
     * 
     * @param msInsertValues the new insert values
     */
    void setInsertValues(String msInsertValues) {
        this.msInsertValues = msInsertValues;
    }

    /**
     * Gets the insert values.
     * 
     * @return the insert values
     */
    String getInsertValues() {
        return this.msInsertValues;
    }

    /**
     * Put SCD key.
     * 
     * @param key the key
     * @param effDt the eff dt
     * @param expDt the exp dt
     * @param skeys the skeys
     */
    private void putSCDKey(Object[] key, ArrayList effDt, ArrayList expDt, ArrayList skeys) {
        this.mLookup.put(key, new Object[] { new SCDValue((java.util.Date[]) effDt.toArray(new java.util.Date[effDt
                .size()]), (java.util.Date[]) expDt.toArray(new java.util.Date[expDt.size()]), (Integer[]) skeys
                .toArray(new Integer[skeys.size()])) });
    }

    /** The types. */
    private Class[] types;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.LookupCreatorImpl#getLookup()
     */
    public PersistentMap getLookup() {

        this.types = new Class[this.mKeys];
        Class[] values = new Class[1];
        String[] valueFields = new String[1];
        for (ETLInPort element : this.mInPorts) {
            JDBCETLInPort port = (JDBCETLInPort) element;

            if (port.mKey != -1) {
                this.types[port.mKey] = port.getPortClass();
            }
        }

        // store 2 arrays one with date one with surrogate key
        values[0] = SCDValue.class;
        valueFields[0] = "SCDValue";

        String lookupClass = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "LOOKUPCLASS",
                EngineConstants.getDefaultLookupClass());

        try {
            return EngineConstants.getInstanceOfPersistantMap(lookupClass, this.getName(), this.mCacheSize,
                    this.mCachePersistenceID, EngineConstants.CACHE_PATH, this.types, values, valueFields,
                    this.cachePersistence == EngineConstants.JOB_PERSISTENCE ? true : false);
        } catch (Throwable e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                    "Lookup cache creation failed, trying again, check stack trace");
            e.printStackTrace();
            try {
                return EngineConstants.getInstanceOfPersistantMap(lookupClass, this.getName(), this.mCacheSize,
                        this.mCachePersistenceID, EngineConstants.CACHE_PATH, this.types, values, valueFields,
                        this.cachePersistence == EngineConstants.JOB_PERSISTENCE ? true : false);
            } catch (Throwable e1) {

                e1.printStackTrace();
                throw new KETLError("LOOKUPCLASS " + lookupClass + " could not be found: " + e.getMessage(), e);
            }
        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.lookup.LookupCreatorImpl#swichToReadOnlyMode()
     */
    public PersistentMap swichToReadOnlyMode() {
        this.mLookup.switchToReadOnlyMode();
        return this.mLookup;
    }

}
