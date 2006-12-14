/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

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
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;

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
abstract public class DatabaseELTWriter extends ETLWriter implements DefaultWriterCore, DBConnection,
        WriterBatchManager, PrePostSQL {

    public DatabaseELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    }

    class Index {

        ArrayList mColumns = new ArrayList();
        String mName;
        boolean mNonUnique;
    }

    class IndexColumn {

        boolean mAscending;
        String mColumn;
        short mPosition;
    }

    public static final String ALTERNATE_INSERT_VALUE = "ALTERNATE_INSERT_VALUE";
    public static final String ALTERNATE_UPDATE_VALUE = "ALTERNATE_UPDATE_VALUE";
    private static final String ALTERNATE_VALUE_SUB = "${PARAM}";
    public static final String BATCH_ATTRIB = "BATCHDATA";
    public static final String HANDLER_ATTRIB = "HANDLER";
    private static final int BULK = 2;
    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    public static final String COMPARE_ATTRIB = "COMPARE";
    public static final String INSERT_ATTRIB = "INSERT";
    public static final String MAXTRANSACTIONSIZE_ATTRIB = "MAXTRANSACTIONSIZE";
    static final int LOWER_CASE = 0;
    static final int MIXED_CASE = 2;
    public static final String PK_ATTRIB = "PK";
    private static final int ROLL = 0;
    public static final String SCHEMA_ATTRIB = "SCHEMA";

    public static final String SEQUENCE_ATTRIB = "SEQUENCE";
    public static final String SK_ATTRIB = "SK";
    static final int SWAP_PARTITION = 0;
    static final int SWAP_TABLE = 1;
    public static final String TABLE_ATTRIB = "TABLE";
    public static final String TYPE_ATTRIB = "TYPE";
    public static final String STREAM_ATTRIB = "STREAMCHANGES";
    public static final String UPDATE_ATTRIB = "UPDATE";
    public static final String IGNOREINVALIDCOLUMNS_ATTRIB = "IGNOREINVALIDCOLUMNS";
    static final int UPPER_CASE = 1;
    private static final int UPSERT = 1;
    private static final String HANDLE_DUPLICATES_ATTRIB = "HANDLEDUPLICATES";
    DatabaseColumnDefinition[] madcdColumns = null;
    String[] maOtherColumns = null;
    boolean mBatchData = true;
    boolean mStreamChanges = true;
    protected Connection mcDBConnection;
    int mDBCase = -1;
    String mDBType = null;
    boolean mDontCompoundStatements = false;
    int miCommitSize;
    private int[] miFieldPopulationOrder;
    int miInsertCount = 0;
    int miMaxTransactionSize = -1;
    int miReplaceTechnique = SWAP_TABLE;
    private boolean mPrimaryKeySpecified = false;
    private String msAllColumns;
    private String msInBatchSQLStatement = null;
    private String msInsertValues;
    String msJoin;
    private boolean mSourceKeySpecified = false;
    private Object[] msPostBatchSQL = null;
    private Object[] msPostLoadSQL = null;
    private Object[] msPreLoadSQL = null;
    private String msJoinColumns;
    private JDBCItemHelper jdbcHelper;

    String msTempTableName = null;

    String mstrSchemaName = null;

    String mstrTableName = null;

    String msUpdateColumns;
    String msUpdateTriggers;

    int mType = -1;

    ArrayList mUsedConnections = new ArrayList();

    HashMap mvColumnIndex = new HashMap();

    Vector mvColumns = new Vector(); // for building the column list and later converting it into the array

    private String strDriverClass = null;

    private String strPassword = null;

    private String strPreSQL = null;

    private String strURL = null;

    private String strUserName = null;
    private String msInsertSourceColumns;
    private int miAnalyzePos = -1;

    public class JDBCETLInPort extends ETLInPort {

        class JDBCDatabaseColumnDefinition extends DatabaseColumnDefinition {

            @Override
            public Class getSourceClass() {
                return getPortClass();
            }

            public JDBCDatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType) {
                super(pNode, pColumnName, pDataType);
            }
            
        }
        DatabaseColumnDefinition dcdNewColumn;

        @Override
        public int initialize(Node xmlNode) throws ClassNotFoundException, KETLThreadException {

            super.initialize(xmlNode);

            // Create a new column definition with the default properties...
            dcdNewColumn = new JDBCDatabaseColumnDefinition(xmlNode, "", 0);

            NamedNodeMap attr = xmlNode.getAttributes();
            // Get the column's target name...
            dcdNewColumn.setColumnName(this.getPortName());

            dcdNewColumn.setAlternateInsertValue(XMLHelper.getAttributeAsString(attr, ALTERNATE_INSERT_VALUE, null));
            dcdNewColumn.setAlternateUpdateValue(XMLHelper.getAttributeAsString(attr, ALTERNATE_UPDATE_VALUE, null));

            // Find out what the upsert flags are for this input...
            if (XMLHelper.getAttributeAsBoolean(attr, PK_ATTRIB, false)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.PRIMARY_KEY);
                mPrimaryKeySpecified = true;
            }

            // Source key
            if (XMLHelper.getAttributeAsBoolean(attr, SK_ATTRIB, false)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY);
                mSourceKeySpecified = true;
            }

            // Insert field
            if (XMLHelper.getAttributeAsBoolean(attr, INSERT_ATTRIB, true)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN);
            }

            // Update field
            if (XMLHelper.getAttributeAsBoolean(attr, UPDATE_ATTRIB, true)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_COLUMN);
            }

            // Compare field, drives updates
            if (XMLHelper.getAttributeAsBoolean(attr, COMPARE_ATTRIB, true)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN);
            }
            
            dcdNewColumn.exists = false;
            // It's ok if not specified
            mvColumns.add(dcdNewColumn);
            mvColumnIndex.put(dcdNewColumn.getColumnName(null, mDBCase), dcdNewColumn);

            return 0;
        }

        public JDBCETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    abstract protected String buildInBatchSQL(String pTable) throws Exception;

    private Object[] buildPostBatchSQL() throws Exception {

        // UPDATE TABLE OR DELETE ROWS
        // INSERT NEW ROWS OR DELETE ROWS
        ArrayList sql = new ArrayList();

        if (this.mType == UPSERT) {
            if (this.mStreamChanges)
                getUpsertSQL(sql);
        }
        return sql.toArray();

    }

    private void getUpsertSQL(ArrayList sql) throws KETLThreadException, SQLException {

        if (this.mStreamChanges == false) {
            String template = this.getStepTemplate(mDBType, "CREATEINDEX", true);

            template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrSchemaName
                    + this.msTempTableName);
            template = EngineConstants.replaceParameterV2(template, "INDEXNAME", this.getUniqueObjectName("idx"));
            template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.msJoinColumns);

            sql.add(template);
        }

        String sqlToExecute = this.getStepTemplate(mDBType, "ANALYZETABLE", true);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mstrSchemaName
                + this.msTempTableName);
        sql.add(sqlToExecute);

        miAnalyzePos = sql.indexOf(sqlToExecute);

        if (this.mHandleDuplicateKeys) {
            sqlToExecute = this.getStepTemplate(mDBType, "DELETEDUPLICATES", true);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME", "aSub");
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                    + this.msTempTableName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "ALIASNAME", this.mstrTableName);
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DEDUPECOLUMN", "seqcol");
            sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "KEYS", this.msJoinColumns);
            sql.add(sqlToExecute);

        }

        if (this.mbReplaceMode) {
            // delete from target rows coming in

            sqlToExecute = this.getStepTemplate(mDBType, "DELETESTATICROWS", true);
            if (sqlToExecute != null && sqlToExecute.equals("") == false) {
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NAMEDSOURCECOLUMNS",
                        this.msTempTableName + "."
                                + this.getAllColumns().replace(",", "," + this.msTempTableName + "."));
                sqlToExecute = EngineConstants
                        .replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                        + this.msTempTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "FIRSTSK", mFirstSK);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "KEYS", this.msJoinColumns);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCEKEYS", this.msTempTableName
                        + "." + this.msJoinColumns.replace(",", "," + this.msTempTableName + "."));
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
                        this.mstrSchemaName + this.mstrTableName);
                sql.add(sqlToExecute);
            }

            sqlToExecute = this.getStepTemplate(mDBType, "DELETETARGET", true);
            if (sqlToExecute != null && sqlToExecute.equals("") == false) {
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                        + this.msTempTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
                        this.mstrSchemaName + this.mstrTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "KEYS", this.msJoinColumns);
                sql.add(sqlToExecute);
            }

            sqlToExecute = this.getStepTemplate(mDBType, "INSERTTARGET", true);
            if (sqlToExecute != null && sqlToExecute.equals("") == false) {
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCECOLUMNS", this.getAllColumns());
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTCOLUMNS",
                        this.msInsertSourceColumns);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTDESTINATIONCOLUMNS",
                        this.msInsertColumns);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                        + this.msTempTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
                        this.mstrSchemaName + this.mstrTableName);
                sql.add(sqlToExecute);
            }

        }
        else {
            sqlToExecute = this.getStepTemplate(mDBType, "UPDATEPOSTBATCH", true);
            if (sqlToExecute != null && sqlToExecute.equals("") == false) {
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
                sqlToExecute = EngineConstants
                        .replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                        + this.msTempTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
                        this.mstrSchemaName + this.mstrTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
                sql.add(sqlToExecute);
            }

            sqlToExecute = this.getStepTemplate(mDBType, "INSERTPOSTBATCH", true);
            if (sqlToExecute != null && sqlToExecute.equals("") == false) {
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCECOLUMNS", this.getAllColumns());
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NAMEDSOURCECOLUMNS",
                        this.msTempTableName + "."
                                + this.getAllColumns().replace(",", "," + this.msTempTableName + "."));
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTCOLUMNS",
                        this.msInsertSourceColumns);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
                sqlToExecute = EngineConstants
                        .replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTDESTINATIONCOLUMNS",
                        this.msInsertColumns);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
                        + this.msTempTableName);
                sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
                        this.mstrSchemaName + this.mstrTableName);
                sql.add(sqlToExecute);
            }
        }
        sqlToExecute = this.getStepTemplate(mDBType, "TRUNCATETABLE", true);
        sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mstrSchemaName
                + this.msTempTableName);
        sql.add(sqlToExecute);
    }

    private ArrayList mIndexEnableList = new ArrayList();
    private ArrayList mIndexDisableList = new ArrayList();

    private Object[] buildPostLoadSQL() throws Exception {

        ArrayList sql = new ArrayList();
        try {

            switch (this.mType) {

            case ROLL:
                String sh = this.setDBCase(XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
                        SCHEMA_ATTRIB, null));

                ResultSet rs = this.mcDBConnection.getMetaData()
                        .getIndexInfo(null, sh, this.mstrTableName, false, true);
                HashMap hm = new HashMap();
                while (rs.next()) {
                    String idxName = rs.getString(6);
                    if (idxName == null)
                        idxName = "NULL";
                    Index idx = (Index) hm.get(idxName);
                    if (idx == null) {
                        idx = new Index();
                        idx.mName = idxName;
                        idx.mNonUnique = rs.getBoolean(4);
                        hm.put(idxName, idx);
                    }

                    IndexColumn col = new IndexColumn();
                    if (rs.getString(10) != null && rs.getString(10).equalsIgnoreCase("D"))
                        col.mAscending = false;
                    col.mColumn = rs.getString(9);
                    col.mPosition = rs.getShort(8);
                    idx.mColumns.add(col);
                }

                String tableName = getUniqueObjectName(this.mstrTableName);
                String indexName = getUniqueObjectName("i");
                StringBuffer sb = new StringBuffer();
                StringBuffer si = new StringBuffer();
                StringBuffer ssk = new StringBuffer();
                StringBuffer spk = new StringBuffer();
                StringBuffer addPK = new StringBuffer();

                for (int i = 0; i < madcdColumns.length; i++) {
                    if (sb.length() > 0) {
                        sb.append(",\n\t");
                    }

                    String prim = "a.", sec = "b.";
                    if (madcdColumns[i].hasProperty(DatabaseColumnDefinition.PRIMARY_KEY)
                            || madcdColumns[i].hasProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY)) {
                        prim = "b.";
                        sec = "a.";
                    }
                    sb.append(coalesce((madcdColumns[i].getAlternateUpdateValue() == null ? prim
                            + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase) : madcdColumns[i]
                            .getAlternateUpdateValue().replace(ALTERNATE_VALUE_SUB,
                                    prim + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase))),
                            (madcdColumns[i].getAlternateInsertValue() == null ? sec
                                    + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase) : madcdColumns[i]
                                    .getAlternateInsertValue().replace(ALTERNATE_VALUE_SUB,
                                            sec + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase))))
                            + " AS " + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));

                }

                String keyColumns = sb.toString();

                sb = new StringBuffer();

                for (int i = 0; i < maOtherColumns.length; i++) {
                    if (i > 0) {
                        sb.append(",\n\t");
                    }

                    sb.append("b." + maOtherColumns[i]);
                }

                String otherColumns = sb.toString();

                if (keyColumns.length() > 0 && otherColumns.length() > 0)
                    otherColumns = "," + otherColumns;

                sb = new StringBuffer();

                for (int i = 0; i < madcdColumns.length; i++) {
                    if (madcdColumns[i].hasProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY)) {
                        if (sb.length() > 0) {
                            sb.append(" AND \n\t");
                            si.append(',');
                            ssk.append(',');
                            addPK.append(',');
                        }

                        sb.append("a." + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase) + " = b."
                                + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        si.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        ssk.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        addPK.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                    }
                }

                // CREATE TABLE ${NEWTABLENAME} AS SELECT ${KEYCOLUMNS} ${OTHERCOLUMNS} FROM ${STGTABLE} as a full outer
                // join ${ORIGTABLE} as b on (${JOIN})
                String rollSQL = this.getStepTemplate(mDBType, "MERGETONEWTABLE", true);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "NEWTABLENAME", this.mstrSchemaName + tableName);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "KEYCOLUMNS", keyColumns);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "OTHERCOLUMNS", otherColumns);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "STGTABLE", this.mstrSchemaName
                        + this.msTempTableName);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "ORIGTABLE", this.mstrSchemaName
                        + this.mstrTableName);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "JOIN", sb.toString());

                for (int i = 0; i < madcdColumns.length; i++) {
                    if (madcdColumns[i].hasProperty(DatabaseColumnDefinition.PRIMARY_KEY)) {
                        if (spk.length() > 0) {
                            spk.append(',');
                        }

                        spk.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                    }
                }

                // add source unique key end to source table
                sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(EngineConstants
                        .replaceParameterV2(this.getStepTemplate(mDBType, "CREATEUNIQUEINDEX", true), "INDEXNAME",
                                indexName), "TABLENAME", this.mstrSchemaName + this.msTempTableName), "COLUMNS", si
                        .toString()));
                sql.add(rollSQL);
                sql.add(EngineConstants.replaceParameterV2(this.getStepTemplate(mDBType, "DROPTABLE", true),
                        "TABLENAME", this.mstrSchemaName + this.msTempTableName));
                // source primary key index
                if (this.mPrimaryKeySpecified) {
                    sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(EngineConstants
                            .replaceParameterV2(this.getStepTemplate(mDBType, "CREATEUNIQUEINDEX", true), "INDEXNAME",
                                    getUniqueObjectName("PK_" + tableName)), "TABLENAME", this.mstrSchemaName
                            + tableName), "COLUMNS", spk.toString()));
                    // add primary key
                    sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(this.getStepTemplate(
                            mDBType, "ADDPRIMARYKEY", true), "TABLENAME", this.mstrSchemaName + tableName), "COLUMNS",
                            addPK.toString()));
                }
                if (this.mSourceKeySpecified) {
                    // source uniqe key
                    sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(EngineConstants
                            .replaceParameterV2(this.getStepTemplate(mDBType, "CREATEUNIQUEINDEX", true), "INDEXNAME",
                                    getUniqueObjectName("SK_" + tableName)), "TABLENAME", this.mstrSchemaName
                            + tableName), "COLUMNS", ssk.toString()));
                }

                switch (miReplaceTechnique) {
                case SWAP_PARTITION:
                    ;

                case SWAP_TABLE:
                    String template = this.getStepTemplate(mDBType, "RENAMETABLE", true);
                    sql.add(StatementManager.COMMIT);
                    sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(template,
                            "TABLENAME", this.mstrSchemaName + this.mstrTableName), "NEWTABLENAME", this
                            .getUniqueObjectName("prev_" + this.mstrTableName)));
                    sql.add(StatementManager.COMMIT);
                    sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(template,
                            "TABLENAME", this.mstrSchemaName + tableName), "NEWTABLENAME", this.mstrTableName));
                }
                break;
            case UPSERT:
                if (this.mStreamChanges == false)
                    getUpsertSQL(sql);
                rollSQL = this.getStepTemplate(mDBType, "DROPTABLE", true);
                rollSQL = EngineConstants.replaceParameterV2(rollSQL, "TABLENAME", this.mstrSchemaName
                        + this.msTempTableName);

                sql.add(rollSQL);
                break;
            }

            return sql.toArray();
        } catch (SQLException e) {
            ResourcePool.LogException(e, this);

            return null;
        }
    }

    private Object[] getFailureCleanupLoadSQL() throws Exception {

        ArrayList sql = new ArrayList();

        switch (this.mType) {
        case ROLL:
        case UPSERT:
            sql.add(EngineConstants.replaceParameterV2(this.getStepTemplate(mDBType, "DROPTABLE", true), "TABLENAME",
                    this.mstrSchemaName + this.msTempTableName));
            break;
        }

        return sql.toArray();

    }

    /**
     * @throws Exception
     */
    private Object[] buildPreLoadSQL() throws Exception {

        ArrayList sql = new ArrayList();

        if (this.mType == UPSERT) {

            String template = this.getStepTemplate(mDBType, "CREATETABLE", true);

            template = EngineConstants.replaceParameterV2(template, "NEWTABLENAME", this.mstrSchemaName
                    + this.msTempTableName);
            template = EngineConstants.replaceParameterV2(template, "SOURCETABLENAME", this.mstrSchemaName
                    + this.mstrTableName);
            template = EngineConstants.replaceParameterV2(template, "SOURCECOLUMNS", this.getAllColumns());

            template = EngineConstants.replaceParameterV2(template, "DEDUPECOLUMN",
                    this.mHandleDuplicateKeys ? ",1 as seqcol" : "");

            sql.add(template);

            if (this.mStreamChanges) {
                template = this.getStepTemplate(mDBType, "CREATEINDEX", true);

                template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrSchemaName
                        + this.msTempTableName);
                template = EngineConstants.replaceParameterV2(template, "INDEXNAME", this.getUniqueObjectName("idx"));
                template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.msJoinColumns);

                sql.add(template);
            }
        }

        return sql.toArray();
    }

    private String coalesce(String arg1, String arg2) throws Exception {
        return EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(this.getStepTemplate(mDBType,
                "COALESCE", true), "ARG1", arg1), "ARG2", arg2);
    }

    StatementWrapper stmt;
    private int maxCharLength;
    protected int mBatchCounter;
    protected boolean firePreBatch;
    private boolean mbReinitOnError;

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     * @throws KETLThreadException
     */
    public int complete() throws KETLThreadException {
        int res = super.complete();

        try {
            stmt.close();
            stmt = null;
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
                            mStatementSeperator, StatementManager.END, this, true);
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

    private void getColumnDataTypes() throws SQLException {
        ResultSet rs = this.mcDBConnection.getMetaData().getColumns(null,
                XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), SCHEMA_ATTRIB, null),
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
        
        if(found == false)
            throw new SQLException("Target table " + this.mstrTableName + " was not found");

    }

    static private int mTempTableFeed = 0;

    /**
     * @throws SQLException
     */
    private String getUniqueObjectName(String pPrefix) throws SQLException {
        boolean notFound = true;
        String res = null;
        int x = 0;

        // check for temp table existance
        while (notFound) {
            res = this.setDBCase(pPrefix + Integer.toString(x++) + "_" + mTempTableFeed++);
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

    private String idQuote;
    private boolean idQuoteEnabled = false;
    protected boolean mHandleDuplicateKeys = false;
    private String mStatementSeperator;
    private String msBestJoin;
    private boolean mbIgnoreInvalidColumns;
    private String msInsertColumns;
    private boolean mbReplaceMode;
    private String mFirstSK;
    private boolean mManageIndexes;

    protected String getIDQuote() {
        if (this.idQuoteEnabled)
            return idQuote;

        return null;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param nConfig DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws KETLThreadException
     */
    public int initialize(Node nConfig) throws KETLThreadException {
        int res = super.initialize(nConfig);

        if (res != 0) {
            return res;
        }

        // Get the attributes
        NamedNodeMap nmAttrs = nConfig.getAttributes();

        // Pull the parameters from the list...
        strUserName = this.getParameterValue(0, USER_ATTRIB);
        strPassword = this.getParameterValue(0, PASSWORD_ATTRIB);
        strURL = this.getParameterValue(0, URL_ATTRIB);
        strDriverClass = this.getParameterValue(0, DRIVER_ATTRIB);
        strPreSQL = this.getParameterValue(0, PRESQL_ATTRIB);

        this.mbReinitOnError = XMLHelper.getAttributeAsBoolean(nmAttrs, "RECONNECTONERROR", true);
        this.mbReplaceMode = XMLHelper.getAttributeAsBoolean(nmAttrs, "REPLACEROWS", false);

        this.mStreamChanges = XMLHelper.getAttributeAsBoolean(nmAttrs, STREAM_ATTRIB, this.mStreamChanges);
        this.mManageIndexes = XMLHelper.getAttributeAsBoolean(nmAttrs, "MANAGEINDEXES", false);

        this.mbIgnoreInvalidColumns = XMLHelper.getAttributeAsBoolean(nmAttrs, IGNOREINVALIDCOLUMNS_ATTRIB, false);
        String tmpType = XMLHelper.getAttributeAsString(nmAttrs, TYPE_ATTRIB, "BULK");
        this.mBatchData = XMLHelper.getAttributeAsBoolean(nmAttrs, BATCH_ATTRIB, this.mBatchData);
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Duplicates keys will be "
                + (this.mHandleDuplicateKeys ? "handled" : "not be handled"));

        this.idQuoteEnabled = XMLHelper.getAttributeAsBoolean(nmAttrs, "IDQUOTE", false);
        String hdl = XMLHelper.getAttributeAsString(nmAttrs, HANDLER_ATTRIB, null);

        this.jdbcHelper = instantiateHelper(hdl);

        if (tmpType.equalsIgnoreCase("ROLL"))
            this.mType = ROLL;
        else if (tmpType.equalsIgnoreCase("UPSERT")) {
            this.mType = UPSERT;
            this.mHandleDuplicateKeys = XMLHelper.getAttributeAsBoolean(nmAttrs, HANDLE_DUPLICATES_ATTRIB,
                    this.mHandleDuplicateKeys);
        }
        else if (tmpType.equalsIgnoreCase("BULK"))
            this.mType = BULK;

        try {
            this.mcDBConnection = ResourcePool.getConnection(strDriverClass, strURL, strUserName, strPassword,
                    strPreSQL, true);

            this.mDBType = this.mcDBConnection.getMetaData().getDatabaseProductName();
            this.mUsedConnections.add(this.mcDBConnection);

            DatabaseMetaData md = this.mcDBConnection.getMetaData();

            idQuote = md.getIdentifierQuoteString();
            if (idQuote == null || idQuote.equals(" "))
                idQuote = "";

            if (md.storesUpperCaseIdentifiers()) {
                this.mDBCase = UPPER_CASE;
            }
            else if (md.storesLowerCaseIdentifiers()) {
                this.mDBCase = LOWER_CASE;
            }
            else if (md.storesMixedCaseIdentifiers()) {
                this.mDBCase = MIXED_CASE;

            }
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        // Pull the name of the table to be written to...
        this.mstrTableName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, TABLE_ATTRIB, null));
        this.mstrSchemaName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, SCHEMA_ATTRIB, null));

        if (this.mstrSchemaName == null)
            this.mstrSchemaName = "";
        else
            this.mstrSchemaName = this.mstrSchemaName + ".";

        // Pull the commit size...
        this.miCommitSize = XMLHelper.getAttributeAsInt(nmAttrs, COMMITSIZE_ATTRIB, 20000);
        this.mLowMemoryThreashold = this.miCommitSize * 100 * this.mInPorts.length;
        this.miMaxTransactionSize = XMLHelper.getAttributeAsInt(nmAttrs, MAXTRANSACTIONSIZE_ATTRIB, -1);
        this.mStatementSeperator = this.getStepTemplate(this.mDBType, "STATEMENTSEPERATOR", true);
        if (this.mStatementSeperator != null && this.mStatementSeperator.length() == 0)
            this.mStatementSeperator = null;

        // Convert the vector we've been building into a more common array...
        madcdColumns = (DatabaseColumnDefinition[]) mvColumns.toArray(new DatabaseColumnDefinition[0]);

        // get column datatype from the database
        try {
            getColumnDataTypes();

            int joinKey = -1, bestJoinKey = -1;
            if (this.mPrimaryKeySpecified) {
                joinKey = DatabaseColumnDefinition.PRIMARY_KEY;
                bestJoinKey = joinKey;
            }
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

            ArrayList fieldPopulationOrder = new ArrayList();

            int cntJoinColumns = 0, cntInsertColumns = 0, cntUpdateTriggers = 0, cntUpdateCols = 0, cntBestJoinColumns = 0, allColumnCount = 0;

            for (int i = 0; i < madcdColumns.length; i++) {

                if (this.mbIgnoreInvalidColumns && madcdColumns[i].exists == false) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Column "
                            + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase) + " not found, skipping");
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
                        updateColumns.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));

                        if (madcdColumns[i].getAlternateUpdateValue() == null) {
                            updateColumns.append(" = ${SOURCETABLENAME}.");
                            updateColumns.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        }
                        else
                            updateColumns.append(" = " + madcdColumns[i].getAlternateUpdateValue());

                    }

                    if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN)) {
                        if (cntUpdateTriggers > 0) {
                            updateTriggers.append(" OR ");
                        }
                        cntUpdateTriggers++;

                        updateTriggers.append("((${DESTINATIONTABLENAME}.");
                        updateTriggers.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        updateTriggers.append(" != ${SOURCETABLENAME}.");
                        updateTriggers.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        updateTriggers.append(") OR (");
                        updateTriggers.append("${DESTINATIONTABLENAME}.");
                        updateTriggers.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is null and ${SOURCETABLENAME}.");
                        updateTriggers.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is not null");
                        updateTriggers.append(") OR (");
                        updateTriggers.append("${DESTINATIONTABLENAME}.");
                        updateTriggers.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is not null and ${SOURCETABLENAME}.");
                        updateTriggers.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                        updateTriggers.append(" is null))");
                    }
                }

                if (bestJoinKey != -1 && this.madcdColumns[i].hasProperty(bestJoinKey)) {

                    if (cntBestJoinColumns > 0) {
                        bestjoin.append("\n\tAND ");
                    }
                    else {
                        mFirstSK = " ${DESTINATIONTABLENAME}."
                                + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase);
                    }

                    cntBestJoinColumns++;

                    bestjoin.append(" ${DESTINATIONTABLENAME}.");
                    bestjoin.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                    bestjoin.append(" = ${SOURCETABLENAME}.");
                    bestjoin.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                }

                if (joinKey != -1 && this.madcdColumns[i].hasProperty(joinKey)) {

                    if (cntJoinColumns > 0) {
                        joinColumns.append(',');
                        join.append("\n\tAND ");
                    }

                    cntJoinColumns++;

                    joinColumns.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                    join.append(" ${DESTINATIONTABLENAME}.");
                    join.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                    join.append(" = ${SOURCETABLENAME}.");
                    join.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                }

                if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.INSERT_COLUMN)) {
                    if (cntInsertColumns > 0) {
                        insertColumns.append(',');
                        insertSourceColumns.append(',');
                    }

                    insertColumns.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));

                    if (madcdColumns[i].getAlternateInsertValue() == null)
                        insertSourceColumns.append("${SOURCETABLENAME}."
                                + madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                    else
                        insertSourceColumns.append(madcdColumns[i].getAlternateInsertValue());

                    cntInsertColumns++;
                }

                if (allColumnCount > 0) {
                    allColumns.append(',');
                    insertValues.append(',');
                }

                allColumnCount++;
                allColumns.append(madcdColumns[i].getColumnName(getIDQuote(), this.mDBCase));
                insertValues.append('?');
                fieldPopulationOrder.add(new Integer(i));

            }

            if (this.mHandleDuplicateKeys) {
                fieldPopulationOrder.add(fieldPopulationOrder.size());
                insertValues.append(",?");
            }

            miFieldPopulationOrder = new int[fieldPopulationOrder.size()];
            for (int i = 0; i < fieldPopulationOrder.size(); i++) {
                miFieldPopulationOrder[i] = ((Integer) fieldPopulationOrder.get(i)).intValue();
            }

            msUpdateColumns = updateColumns.toString();
            setAllColumns(allColumns.toString());
            msJoin = join.toString();
            msBestJoin = bestjoin.toString();
            msInsertColumns = insertColumns.toString();
            this.msInsertSourceColumns = insertSourceColumns.toString();
            setInsertValues(insertValues.toString());
            msJoinColumns = joinColumns.toString();
            msUpdateTriggers = (cntUpdateTriggers == 0 ? "1=0" : updateTriggers.toString());
            this.getAllIndexes(XMLHelper.getAttributeAsString(nmAttrs, "JOININDEX", null));
            
            // if roll then table needs to be created to dump data too
            switch (this.mType) {
            case ROLL:
            case UPSERT:
                if (this.mSourceKeySpecified == false) {
                    ResourcePool
                            .LogMessage(this, ResourcePool.ERROR_MESSAGE,
                                    "Insert type requires source keys to be specified, either specify them or switch to BULK insert");
                    return -1;
                }

                this.msTempTableName = getUniqueObjectName("t");
                this.maOtherColumns = this.getAllOtherTableColumns();
                break;
            case BULK:
                this.msTempTableName = this.mstrTableName;
                break;
            }

            this.msPreLoadSQL = buildPreLoadSQL();
            this.msPostLoadSQL = buildPostLoadSQL();
            this.msInBatchSQLStatement = this.buildInBatchSQL(this.mstrSchemaName + this.msTempTableName);

            if (this.debug())
                ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Insert statement: "
                        + this.msInBatchSQLStatement);
            this.stmt = prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement, this.jdbcHelper);

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
            this.msPostBatchSQL = buildPostBatchSQL();

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
                        mStatementSeperator, StatementManager.END, this, true);
            } catch (Exception e1) {

            }
            throw new KETLThreadException(e, this);
        }

        return 0;
    }

    private void getAllIndexes(String string) throws SQLException, KETLThreadException {

        if (mManageIndexes == false)
            return;

        ResultSet indexRs = this.mcDBConnection.getMetaData().getIndexInfo(null, this.mstrSchemaName,
                this.mstrTableName, false, true);
        ArrayList indexList = new ArrayList();
        while (indexRs.next()) {
            String idxName = indexRs.getString(6);

            if (idxName == null || idxName.equalsIgnoreCase(string) == false)
                indexList.add(idxName);
        }

        indexRs.close();

        if (this.partitionID == 0) {
            String idxEnable = this.getStepTemplate(mDBType, "ENABLEINDEX", true);

            if (idxEnable != null && idxEnable.length() > 0) {
                for (Object o : indexList) {
                    this.mIndexEnableList.add(EngineConstants.replaceParameterV2(idxEnable, "INDEXNAME", (String) o));
                }
            }
        }

        if (this.partitionID == 0) {
            String idxDisable = this.getStepTemplate(mDBType, "DISABLEINDEX", true);

            if (idxDisable != null && idxDisable.length() > 0) {
                for (Object o : indexList) {
                    this.mIndexDisableList.add(EngineConstants.replaceParameterV2(idxDisable, "INDEXNAME", (String) o));
                }
            }
        }
    }

    abstract protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException;

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

    protected int dedupeCounter = 0;

    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {

        int res = 1;
        try {
            int cols = this.miFieldPopulationOrder.length;
            for (int i = 1; i <= cols; i++) {
                if (this.mHandleDuplicateKeys && i == cols) {
                    this.stmt.setParameterFromClass(i, Integer.class, dedupeCounter++, maxCharLength, null);
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
                                : pInputRecords[idx], maxCharLength, port.getXMLConfig());
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
                stmt.addBatch();
                logBatch(pInputRecords);

                this.mBatchCounter++;
            }
            else {
                res = stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new KETLWriteException(e);
        }

        return res;
    }

    ArrayList mBatchLog = new ArrayList();
    int recordNumBatchStart;

    private void logBatch(Object[] inputRecords) {
        mBatchLog.add(inputRecords);
    }

    public Connection getConnection() {
        return this.mcDBConnection;
    }

    int miRetryBatch = 0;

    boolean mIncrementalCommit = true;

    abstract StatementWrapper prepareStatementWrapper(Connection Connection, String sql, JDBCItemHelper jdbcHelper)
            throws SQLException;

    private int retryBatch() throws KETLWriteException {
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                "Retrying records in batch, to identify invalid records");
        int result = 0;
        for (int r = 0; r < this.miRetryBatch; r++) {
            int errorCount = 0, submitted = 0;

            // reset statement as some drivers fail after a failure has occured
            try {
                this.stmt.close();
                this.stmt = prepareStatementWrapper(mcDBConnection, this.msInBatchSQLStatement, this.jdbcHelper);
            } catch (SQLException e) {
                throw new KETLWriteException(e);
            }
            for (int x = 0; x < this.mBatchLog.size(); x++) {
                Object[] record = (Object[]) this.mBatchLog.get(x);

                if (this.mFailedBatchElements.contains(x)) {
                    try {
                        if (this.mbReinitOnError) {
                            this.stmt.close();
                            this.stmt = prepareStatementWrapper(mcDBConnection, this.msInBatchSQLStatement,
                                    this.jdbcHelper);
                        }

                        int cols = this.miFieldPopulationOrder.length;
                        for (int i = 1; i <= cols; i++) {
                            if (this.mHandleDuplicateKeys && i == cols) {
                                this.stmt.setParameterFromClass(i, Integer.class, dedupeCounter++, maxCharLength, null);
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
                                        .getConstantValue() : record[idx], maxCharLength, port.getXMLConfig());
                            }
                        }

                        submitted++;
                        this.stmt.executeUpdate();
                        result++;
                        this.mFailedBatchElements.remove(x);
                        if (mIncrementalCommit)
                            this.mcDBConnection.commit();

                    } catch (SQLException e) {
                        errorCount++;
                        if (r == this.miRetryBatch - 1)
                            this.incrementErrorCount(new KETLWriteException("Record " + (this.miInsertCount + x + 1)
                                    + " failed to submit, " + e.toString(), e), record, this.miInsertCount + x + 1);

                        try {
                            this.stmt.close();
                            this.stmt = prepareStatementWrapper(mcDBConnection, this.msInBatchSQLStatement,
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

    long mLowMemoryThreashold = -1;

    public int finishBatch(int len) throws KETLWriteException {
        int result = 0;
        try {
            if (this.mBatchData
                    && (this.mBatchCounter >= this.miCommitSize
                            || (this.mBatchCounter > 0 && this.isMemoryLow(mLowMemoryThreashold)) || (len == LASTBATCH && this.mBatchCounter > 0))) {
                dedupeCounter = 0;
                boolean errorsOccured = false;
                Savepoint savepoint = null;
                try {

                    if (supportsSetSavepoint) {
                        savepoint = this.mcDBConnection.setSavepoint();
                    }

                    Exception e1 = null;
                    int[] res = null;
                    try {
                        res = stmt.executeBatch();

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
                    result = retryBatch();
                }

                clearBatchLogBatch();

                this.miInsertCount += this.mBatchCounter;
                this.mBatchCounter = 0;

                if (mIncrementalCommit)
                    this.mcDBConnection.commit();
                this.executePostBatchStatements();
                firePreBatch = true;

            }
            else if (this.mBatchData == false) {
                if (mIncrementalCommit)
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

    private Set mFailedBatchElements = new HashSet();

    protected void clearBatchLogBatch() {
        this.mBatchLog.clear();
        this.mFailedBatchElements.clear();
    }

    boolean supportsSetSavepoint = false, supportsReleaseSavepoint = false;

    public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {
        try {
            if (this.firePreBatch && this.mBatchData) {
                this.executePreBatchStatements();
                recordNumBatchStart = this.getRecordsProcessed();
                this.firePreBatch = false;
            }

        } catch (SQLException e) {
            throw new KETLWriteException(e);
        }
        return data;
    }

    public void executePostStatements() throws SQLException {
        this.setWaiting("post statements to run");
        StatementManager.executeStatements(this.msPostLoadSQL, this.mcDBConnection, this.mStatementSeperator,
                StatementManager.END, this, false);

        if (this.mType == UPSERT && miAnalyzePos != -1) {
            // remove analyzes they should only happen once
            this.msPostLoadSQL[miAnalyzePos] = null;
            miAnalyzePos = -1;
        }

        if(this.isLastThreadToEnterCompletePhase()){
            // wait for all other threads to complete
            this.setWaiting("all other threads in group to complete");                
            while(Thread.currentThread().getThreadGroup().activeCount() > 1){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {                        
                       throw new KETLError(e);
                    }
            }
            this.setWaiting("final statements to run");
            
        }
        
        if (this.isLastThreadToEnterCompletePhase()) {
            this.setWaiting("indexes to rebuild");
            StatementManager.executeStatements(this.mIndexDisableList.toArray(), this.mcDBConnection,
                    this.mStatementSeperator, StatementManager.END, this, false);
        }
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
        this.setWaiting(null);
    }

    public void executePreStatements() throws SQLException {
        this.setWaiting("pre statements to run");
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
        StatementManager.executeStatements(this.msPreLoadSQL, this.mcDBConnection, this.mStatementSeperator,
                StatementManager.START, this, false);

        if (this.isFirstThreadToEnterInitializePhase()) {
            this.setWaiting("indexes to be disabled");
            StatementManager.executeStatements(this.mIndexEnableList.toArray(), this.mcDBConnection,
                    this.mStatementSeperator, StatementManager.END, this, false);
        }
        this.setWaiting(null);
    }

    public void executePostBatchStatements() throws SQLException {
        this.setWaiting("post batch statements to run");
        StatementManager.executeStatements(msPostBatchSQL, this.mcDBConnection, mStatementSeperator,
                StatementManager.END, this, false);
        StatementManager.executeStatements(this, this, "POSTBATCHSQL");
        this.setWaiting(null);
    }

    public void executePreBatchStatements() throws SQLException {
        this.setWaiting("pre batch statements to run");
        StatementManager.executeStatements(this, this, "PREBATCHSQL");
        this.setWaiting(null);
    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new JDBCETLInPort(this, srcStep);
    }

    @Override
    protected void close(boolean success) {
        try {

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
                            mStatementSeperator, StatementManager.END, this, true);
                } catch (Exception e1) {
                }
            }
            ResourcePool.releaseConnection(this.mcDBConnection);
        }
    }

    void setAllColumns(String msAllColumns) {
        this.msAllColumns = msAllColumns;
    }

    String getAllColumns() {
        return msAllColumns;
    }

    void setInsertValues(String msInsertValues) {
        this.msInsertValues = msInsertValues;
    }

    String getInsertValues() {
        return msInsertValues;
    }

}
