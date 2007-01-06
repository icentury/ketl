/*
 * Created on Jul 13, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.transformation;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.SharedCounter;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.JDBCStatementWrapper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLJob;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.lookup.LookupCreatorImpl;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.TransformBatchManager;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.etl.util.XMLHelper;

// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

public class DimensionTransformation extends ETLTransformation implements DBConnection, TransformBatchManager,
        PrePostSQL, LookupCreatorImpl {

    class DimensionETLInPort extends ETLInPort {

        DatabaseColumnDefinition mColumn = null;
        boolean sk = false, insert = false, update = false, compare = false, isColumn = false, effectiveDate = false;
        int skColIndex = -1;

        public DimensionETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        @Override
        public int initialize(Node xmlNode) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlNode);
            if (res != 0)
                return res;

            DatabaseColumnDefinition dcdNewColumn;

            // Create a new column definition with the default properties...
            dcdNewColumn = new DatabaseColumnDefinition(xmlNode, "", 0);

            NamedNodeMap attr = xmlNode.getAttributes();
            // Get the column's target name...
            dcdNewColumn.setColumnName(this.getPortName());

            dcdNewColumn.setAlternateInsertValue(XMLHelper.getAttributeAsString(attr, ALTERNATE_INSERT_VALUE, null));
            dcdNewColumn.setAlternateUpdateValue(XMLHelper.getAttributeAsString(attr, ALTERNATE_UPDATE_VALUE, null));

            this.effectiveDate = XMLHelper.getAttributeAsBoolean(attr, EFFECTIVE_DATE_ATTRIB, false);

            if (effectiveDate && ((Element) xmlNode).hasAttribute("DATATYPE") == false)
                ((Element) xmlNode).setAttribute("DATATYPE", "DATE");

            if (effectiveDate) {
                if (effectiveDatePort == null)
                    effectiveDatePort = this;
                else
                    throw new KETLThreadException("Only one effective date port is allowed", this);
            }

            // Find out what the upsert flags are for this input...

            // Source key
            int skIdx = XMLHelper.getAttributeAsInt(attr, SK_ATTRIB, -1);
            if (skIdx != -1) {
                if (skIdx < 1)
                    throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
                            + " KEY order starts at 1, invalid value of " + skIdx, this);

                dcdNewColumn.setProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY);
                sk = true;
                skColIndex = skIdx - 1;
                mSKColCount++;
            }

            // Insert field
            if (XMLHelper.getAttributeAsBoolean(attr, INSERT_ATTRIB, false)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN);
                insert = true;
            }

            // Update field
            if (XMLHelper.getAttributeAsBoolean(attr, UPDATE_ATTRIB, false)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_COLUMN);
                update = true;
            }

            // Compare field, drives updates
            if (XMLHelper.getAttributeAsBoolean(attr, COMPARE_ATTRIB, false)) {
                dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN);
                compare = true;
            }

            if (sk || insert || update || compare)
                mColumn = dcdNewColumn;

            return 0;
        }

    }

    class DimensionETLOutPort extends ETLOutPort {

        int outColIndex = -1;

        boolean pk = false;
        boolean expiration_dt = false;
        boolean dirtyFlag = false;

        public DimensionETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {
            if (pk) {
                return this.getCodeGenerationReferenceObject() + "[" + getUsedPortIndex(pkPort) + "] =  (("
                        + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).getPK(pInputRecords);";
            }
            else
                return super.generateCode(portReferenceIndex);
        }

        @Override
        public ETLPort getAssociatedInPort() throws KETLThreadException {
            if (pk)
                return null;

            return super.getAssociatedInPort();
        }

        @Override
        public String getCode() throws KETLThreadException {
            if (pk)
                return "";
            return super.getCode();
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            this.pk = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), PK_ATTRIB, false);
            this.expiration_dt = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), EXPIRATION_DATE_ATTRIB,
                    false);
            this.dirtyFlag = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), DIRTY_FLAG_ATTRIB, false);

            if (pk && ((Element) xmlConfig).hasAttribute("DATATYPE") == false)
                ((Element) xmlConfig).setAttribute("DATATYPE", Integer.class.getCanonicalName());

            if (expiration_dt && ((Element) xmlConfig).hasAttribute("DATATYPE") == false)
                ((Element) xmlConfig).setAttribute("DATATYPE", "DATE");

            if (dirtyFlag && ((Element) xmlConfig).hasAttribute("DATATYPE") == false)
                ((Element) xmlConfig).setAttribute("DATATYPE", "BOOLEAN");

            if (pk) {
                outColIndex = mPKColCount++;
            }

            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            if (pk && pkPort == null) {
                pkPort = this;
            }
            else if (pk && pkPort != null) {
                throw new KETLThreadException("Only one primary key port is allowed", this);
            }

            if (dirtyFlag) {
                if (dirtyFlagPort == null)
                    dirtyFlagPort = this;
                else
                    throw new KETLThreadException("Only one dirty flag port is allowed", this);
            }

            if (expiration_dt) {
                if (expirationDatePort == null)
                    expirationDatePort = this;
                else
                    throw new KETLThreadException("Only one expiration date port is allowed", this);
            }
            return 0;

        }

    }

    public static final String ALTERNATE_INSERT_VALUE = "ALTERNATE_INSERT_VALUE";
    public static final String ALTERNATE_UPDATE_VALUE = "ALTERNATE_UPDATE_VALUE";
    public static final String BATCH_ATTRIB = "BATCHDATA";
    private static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    public static final String COMPARE_ATTRIB = "COMPARE";
    public static final String HANDLER_ATTRIB = "HANDLER";
    public static final String INSERT_ATTRIB = "INSERT";
    private static final String KEY_TABLE_ATTRIB = "KEYTABLE";
    private static final String SCD_ATTRIB = "SCD";
    private static final int KEY_TABLE_ONLY = 1;
    private static final int LOAD_KEY_TABLE_DIMENSION = 0;
    private static final int LOWER_CASE = 0;
    public static final String MAXTRANSACTIONSIZE_ATTRIB = "MAXTRANSACTIONSIZE";
    private static final int MIXED_CASE = 2;
    public static final String PK_ATTRIB = "PK";
    public static final String EFFECTIVE_DATE_ATTRIB = "EFFECTIVEDATE";
    public static final String DIRTY_FLAG_ATTRIB = "DIRTYFLAG";
    public static final String EXPIRATION_DATE_ATTRIB = "EXPIRATIONDATE";
    private static final String SCHEMA_ATTRIB = "SCHEMA";
    public static final String SK_ATTRIB = "SK";
    private static final String TABLE_ATTRIB = "TABLE";
    private static final int TABLE_ONLY = 2;
    public static final String UPDATE_ATTRIB = "UPDATE";
    private static final int UPPER_CASE = 1;

    private int cachePersistence;
    private boolean firePreBatch;
    private String idQuote;

    private boolean idQuoteEnabled;
    private JDBCItemHelper jdbcHelper;

    private boolean lookupPendingLoad = true;

    private int maxCharLength;

    private int mBatchCounter;

    private boolean mBatchData = true;

    ArrayList mBatchLog = new ArrayList();

    private boolean mbReinitOnError;

    private Integer mCachePersistenceID;

    private int mCacheSize;

    private Connection mcDBConnection;

    private int mDBCase = -1;
    private String mDBType;

    private Set mFailedBatchElements = new HashSet();

    private int miCommitSize;

    private ETLPort[] miFieldPopulationOrder;

    private int miInsertCount;

    private int miMode;
    private boolean mIncrementalCommit;
    private int miRetryBatch;

    private SharedCounter mKeySource;

    private PersistentMap mLookup;

    private String msAllColumns;
    private String msInBatchSQLStatement;

    private String msInsertValues;
    public int mSKColCount = 0, mPKColCount = 0;
    private String msKeyTableAllColumns;
    private String msKeyTableInsertValues;

    private String mstrKeyTableName;

    private String mstrPrimaryKeyColumns;

    private String mstrSchemaName;

    private String mstrSourceKeyColumns;

    private String mstrTableName;
    private List mUsedConnections = new ArrayList();

    DimensionETLOutPort pkPort = null, dirtyFlagPort = null, expirationDatePort = null;
    DimensionETLInPort effectiveDatePort = null;

    int recordNumBatchStart;

    Object[] skData;

    private int[] skIndx;

    private StatementWrapper stmt;

    private String strDriverClass = null;

    private String strPassword = null;

    private String strPreSQL = null;
    private String strURL = null;
    private String strUserName = null;
    private boolean supportsReleaseSavepoint;

    private boolean supportsSetSavepoint;
    private boolean mAllowInsert;

    public DimensionTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    }

    protected String buildInBatchSQL() throws KETLThreadException {

        /*
         * <GROUP NAME="ORACLE"> <TEMPLATE NAME="MULTIINSERTWRAPPER">insert all ${STATEMENT} select ${VALUES} from dual</TEMPLATE>
         * <TEMPLATE NAME="INSERT">into ${SCHEMANAME}.${TABLENAME}(${DESTINATIONCOLUMNS}) VALUES(${NAMEDVALUES})</TEMPLATE>
         * </GROUP>
         */
        boolean multiStatement = this.msKeyTableAllColumns != null && this.msAllColumns != null;

        String template = null;
        if (this.msKeyTableAllColumns != null) {
            template = this.getStepTemplate(mDBType, multiStatement ? "MULTIINSERT" : "INSERT", true);

            template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrKeyTableName);
            template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);
            template = EngineConstants.replaceParameterV2(template, "DESTINATIONCOLUMNS", this.getKeyTableAllColumns());
            template = EngineConstants.replaceParameterV2(template, "VALUES", this.getKeyTableInsertValues());
            template = EngineConstants.replaceParameterV2(template, "NAMEDVALUES", this.getKeyTableAllColumns());
        }
        if (this.msAllColumns != null) {
            template = multiStatement ? template
                    + (this.getStepTemplate(mDBType, "STATEMENTSEPERATOR", true) == null ? "" : this.getStepTemplate(
                            mDBType, "STATEMENTSEPERATOR", true)) : template;
            template = template + this.getStepTemplate(mDBType, multiStatement ? "MULTIINSERT" : "INSERT", true);

            template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrTableName);
            template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);
            template = EngineConstants.replaceParameterV2(template, "DESTINATIONCOLUMNS", this.getAllColumns());
            template = EngineConstants.replaceParameterV2(template, "VALUES", this.getInsertValues());
            template = EngineConstants.replaceParameterV2(template, "NAMEDVALUES", this.getAllColumns());
        }

        if (multiStatement) {
            String wrapper = this.getStepTemplate(mDBType, "MULTIINSERTWRAPPER", true);
            template = EngineConstants.replaceParameterV2(wrapper, "STATEMENT", template);
            template = EngineConstants.replaceParameterV2(template, "VALUES", this.getInsertValues());
        }
        return template;
    }

    private void clearBatchLogBatch() {
        this.mBatchLog.clear();
        this.mFailedBatchElements.clear();
    }

    @Override
    protected void close(boolean success) {

        try {

        	if(lookupLocked){
        		// release any write lock
                ((KETLJob) this.getJobExecutor().getCurrentETLJob()).releaseLookupWriteLock(this.getName(), this);
        	}
        	
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
        if (this.mcDBConnection != null)
            ResourcePool.releaseConnection(this.mcDBConnection);

        if (this.cachePersistence == EngineConstants.JOB_PERSISTENCE) {
            ((KETLJob) this.getJobExecutor().getCurrentETLJob()).deleteLookup(this.getName());
        }

    }

    private boolean lookupLocked = false;
    
    @Override
    public int complete() throws KETLThreadException {
        int res = super.complete();
        /*
         * try { stmt.close(); stmt = null; } catch (Exception e) { ResourcePool.LogException(e, this); }
         */
        if (res < 0)
            ResourcePool
                    .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error during final batch, see previous messages");
        else {
            try {
                this.executePostStatements();
            } catch (Exception e) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Running post load " + e.getMessage());
                res = -6;
            }
        }

        // submit lookup for use
        if(lookupLocked){
        		// release any write lock
                ((KETLJob) this.getJobExecutor().getCurrentETLJob()).releaseLookupWriteLock(this.getName(), this);
        }
        
        return res;
    }

    final private Integer createNewSurrogateKey(Object[] pInputRecords) throws KETLTransformException {

        Integer data = this.mKeySource.increment(1);

        try {
            for (int i = 0; i < this.miFieldPopulationOrder.length; i++) {
                ETLPort idx = this.miFieldPopulationOrder[i];

                if (idx == this.pkPort)
                    this.stmt.setParameterFromClass(i + 1, Integer.class, data, maxCharLength, this.pkPort
                            .getXMLConfig());
                else {
                    ETLInPort inport = (ETLInPort) idx;

                    this.stmt.setParameterFromClass(i + 1, inport.getPortClass(), inport.isConstant() ? inport
                            .getConstantValue() : pInputRecords[inport.getSourcePortIndex()], maxCharLength, inport
                            .getXMLConfig());
                }
            }

            if (this.mBatchData) {
                stmt.addBatch();
                logBatch(pInputRecords);

                this.mBatchCounter++;
            }
            else {
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new KETLTransformException(e);
        }

        /*
         * if (mSKColCount == 1) putKeyObjectDataObject(pInputRecords, data); else
         */
        try {
            putKeyArrayDataArray(pInputRecords, data);
        } catch (Error e) {
            throw new KETLTransformException(e.getMessage());
        }
        return data;

    }

    public void executePostBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTBATCHSQL");
    }

    public void executePostStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
    }

    public void executePreBatchStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PREBATCHSQL");
    }

    public void executePreStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
    }

    public Object[][] finishBatch(Object[][] data, int len) throws KETLTransformException {
        int result = 0;
        try {
            if (this.mBatchData
                    && (this.mBatchCounter >= this.miCommitSize || (len == LASTBATCH && this.mBatchCounter > 0))) {
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
                                this.incrementErrorCount(e1 == null ? new KETLTransformException(
                                        "Failed to submit record " + (i + 1 + this.miInsertCount))
                                        : new KETLTransformException(e1), (Object[]) this.mBatchLog.get(i), i + 1
                                        + this.miInsertCount);
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
                                    this.incrementErrorCount(e1 == null ? new KETLTransformException(
                                            "Failed to submit record " + (i + 1 + this.miInsertCount))
                                            : new KETLTransformException(e1), (Object[]) this.mBatchLog.get(rLen), i
                                            + 1 + this.miInsertCount);
                            }
                            else {
                                result += res[i] >= 0 ? res[i] : 1;
                            }
                        }
                    }
                } catch (SQLException e) {
                    throw new KETLTransformException(e);
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
            throw new KETLTransformException(e);
        }
        return data;
    }

    String getAllColumns() {
        return msAllColumns;
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        return this.mcDBConnection;
    }

    String getInsertValues() {
        return msInsertValues;
    }

    String getKeyTableAllColumns() {
        return this.msKeyTableAllColumns;
    }

    String getKeyTableInsertValues() {
        return this.msKeyTableInsertValues;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.writer.LookupCreatorImpl#getLookup()
     */
    public PersistentMap getLookup() {

        Class[] types = new Class[mSKColCount];
        Class[] values = new Class[mPKColCount];
        String[] valueFields = new String[mPKColCount];
        for (int i = 0; i < this.mInPorts.length; i++) {
            DimensionETLInPort port = (DimensionETLInPort) this.mInPorts[i];

            if (port.skColIndex != -1)
                types[port.skColIndex] = port.getPortClass();
        }

        for (int i = 0; i < this.mOutPorts.length; i++) {
            DimensionETLOutPort port = (DimensionETLOutPort) this.mOutPorts[i];

            if (port.outColIndex != -1) {
                values[port.outColIndex] = port.getPortClass();
                valueFields[port.outColIndex] = port.mstrName;

            }

        }

        String lookupClass = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "LOOKUPCLASS",
                EngineConstants.getDefaultLookupClass());

        try {
            return EngineConstants.getInstanceOfPersistantMap(lookupClass, this.getName(), mCacheSize,
                    this.mCachePersistenceID, EngineConstants.CACHE_PATH, types, values, valueFields,
                    cachePersistence == EngineConstants.JOB_PERSISTENCE ? true : false);
        } catch (Throwable e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                    "Lookup cache creation failed, trying again, check stack trace");
            e.printStackTrace();

            try {
                return EngineConstants.getInstanceOfPersistantMap(lookupClass, this.getName(), mCacheSize,
                        this.mCachePersistenceID, EngineConstants.CACHE_PATH, types, values, valueFields,
                        cachePersistence == EngineConstants.JOB_PERSISTENCE ? true : false);
            } catch (Throwable e1) {

                e1.printStackTrace();
                throw new KETLError("LOOKUPCLASS " + lookupClass + " could not be found: " + e.getMessage(), e);
            }

        }

    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new DimensionETLInPort(this, srcStep);
    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new DimensionETLOutPort(this, srcStep);
    }

    final public Integer getPK(Object[] pInputRecords) throws KETLTransformException {
        // lookup value in index
        Integer res;
        try {
            res = this.getSurrogateKey(pInputRecords);
        } catch (Error e) {
            throw new KETLTransformException(e.getMessage());
        }
        // if not found then create new value in index
        if (res == null && mAllowInsert)
            return this.createNewSurrogateKey(pInputRecords);

        return res;
    }

    private String getPrimaryKeyColumns() {
        return idQuote + this.pkPort.mstrName + idQuote;
    }

    private String getSourceKeyColumns() {
        String res = null;
        for (int i = 0; i < this.mInPorts.length; i++) {
            if (((DimensionETLInPort) this.mInPorts[i]).sk) {
                res = (res == null ? ((DimensionETLInPort) this.mInPorts[i]).mColumn.getColumnName(idQuote,
                        this.mDBCase) : res + ","
                        + ((DimensionETLInPort) this.mInPorts[i]).mColumn.getColumnName(idQuote, this.mDBCase));
            }
        }
        return res;
    }

    final private Integer getSurrogateKey(Object[] pInputRecords) {

        for (int i = 0; i < this.mSKColCount; i++)
            skData[i] = pInputRecords[skIndx[i]];

        Object res = this.mLookup.get(skData, null);
        return (Integer) res;
    }

    private int mSCD;

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        this.mKeySource = this.getJobExecutor().getCurrentETLJob().getCounter(this.getName(),
                this.pkPort == null ? int.class : this.pkPort.getPortClass());

        // Pull the parameters from the list...
        strUserName = this.getParameterValue(0, USER_ATTRIB);
        strPassword = this.getParameterValue(0, PASSWORD_ATTRIB);
        strURL = this.getParameterValue(0, URL_ATTRIB);
        strDriverClass = this.getParameterValue(0, DRIVER_ATTRIB);
        strPreSQL = this.getParameterValue(0, PRESQL_ATTRIB);
        NamedNodeMap nmAttrs = xmlConfig.getAttributes();

        int minSize = NumberFormatter.convertToBytes(EngineConstants.getDefaultCacheSize());
        cachePersistence = EngineConstants.JOB_PERSISTENCE;

        mSCD = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), SCD_ATTRIB, 1);

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Slowly changing dimension mode = " + mSCD);

        String tmp = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "PERSISTENCE", null);
        if (tmp == null || tmp.equalsIgnoreCase("JOB")) {
            this.mCachePersistenceID = ((Long) this.getJobExecutionID()).intValue();
            cachePersistence = EngineConstants.JOB_PERSISTENCE;
        }
        else if (tmp.equalsIgnoreCase("LOAD")) {
            this.mCachePersistenceID = this.mkjExecutor.getCurrentETLJob().getLoadID();
            cachePersistence = EngineConstants.LOAD_PERSISTENCE;
        }
        else if (tmp.equalsIgnoreCase("STATIC")) {
            cachePersistence = EngineConstants.STATIC_PERSISTENCE;
            this.mCachePersistenceID = null;
        }
        else
            throw new KETLThreadException("PERSISTENCE has to be either JOB,LOAD or STATIC", this);

        mCacheSize = NumberFormatter.convertToBytes(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                "CACHESIZE", null));

        if (mCacheSize == -1)
            mCacheSize = minSize;
        if (mCacheSize < minSize) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                    "Cache cannot be less than 64kb, defaulting to 64kb");
            mCacheSize = minSize;
        }

        if (this.mSKColCount > 6) {
            ResourcePool
                    .LogMessage(this, ResourcePool.INFO_MESSAGE,
                            "Currently lookups are limited to no more than 4 keys, unless you use a an array object to represent the compound key");
        }
        else if (this.mSKColCount < 1)
            throw new KETLThreadException("Source key has not been specified e.g. SK=\"1\"", this);

        this.mLookup = ((KETLJob) this.getJobExecutor().getCurrentETLJob()).registerLookupWriteLock(this.getName(),
                this, cachePersistence);
        this.lookupLocked = true;

        try {
            this.mcDBConnection = ResourcePool.getConnection(strDriverClass, strURL, strUserName, strPassword,
                    strPreSQL, true);

            this.mUsedConnections.add(this.mcDBConnection);

            DatabaseMetaData md = this.mcDBConnection.getMetaData();

            this.mDBType = md.getDatabaseProductName();
            this.maxCharLength = md.getMaxCharLiteralLength();
            this.supportsSetSavepoint = md.supportsSavepoints();
            this.mBatchData = XMLHelper.getAttributeAsBoolean(nmAttrs, BATCH_ATTRIB, this.mBatchData);
            this.idQuoteEnabled = XMLHelper.getAttributeAsBoolean(nmAttrs, "IDQUOTE", false);

            String hdl = XMLHelper.getAttributeAsString(nmAttrs, HANDLER_ATTRIB, null);
            this.jdbcHelper = instantiateHelper(hdl);

            if (md.storesUpperCaseIdentifiers()) {
                this.mDBCase = UPPER_CASE;
            }
            else if (md.storesLowerCaseIdentifiers()) {
                this.mDBCase = LOWER_CASE;
            }
            else if (md.storesMixedCaseIdentifiers()) {
                this.mDBCase = MIXED_CASE;

            }

            // Pull the name of the table to be written to...
            this.mstrTableName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, TABLE_ATTRIB, null));
            this.mstrSchemaName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, SCHEMA_ATTRIB, null));
            boolean namedValueList = false;

            tmp = XMLHelper.getAttributeAsString(nmAttrs, "MODE", "BOTH");
            if (tmp.equalsIgnoreCase("BOTH")) {
                this.miMode = LOAD_KEY_TABLE_DIMENSION;
                namedValueList = Boolean.parseBoolean(this.getStepTemplate(this.mDBType, "NAMEDVALUELIST", true));
            }
            else if (tmp.equalsIgnoreCase("KEYTABLEONLY")) {
                this.miMode = KEY_TABLE_ONLY;
            }
            else if (tmp.equalsIgnoreCase("TABLEONLY"))
                this.miMode = TABLE_ONLY;
            else
                throw new KETLThreadException(
                        "Invalid MODE, valid values are BOTH (default), KEYTABLEONLY and TABLEONLY", this);

            if (this.idQuoteEnabled) {
                idQuote = md.getIdentifierQuoteString();
                if (idQuote == null || idQuote.equals(" "))
                    idQuote = "";
            }
            else {
                idQuote = "";
            }

            ResultSet rsDBResultSet = md.getTables(null, this.mstrSchemaName, this.mstrTableName, null);

            boolean tableFound = false;
            while (rsDBResultSet.next()) {
                tableFound = true;
                if (this.mstrSchemaName == null)
                    this.mstrSchemaName = rsDBResultSet.getString("TABLE_SCHEM");
            }
            rsDBResultSet.close();

            if (tableFound == false) {
                throw new KETLThreadException("Dimension table does not exists, or could not be found - "
                        + this.mstrSchemaName + "." + this.mstrTableName, this);
            }

            StringBuffer allColumns = new StringBuffer();
            StringBuffer insertValues = new StringBuffer();

            ArrayList fieldPopulationOrder = new ArrayList();

            int cnt;

            if (this.miMode == LOAD_KEY_TABLE_DIMENSION || this.miMode == KEY_TABLE_ONLY) {
                this.mstrKeyTableName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, KEY_TABLE_ATTRIB,
                        this.mstrTableName + "_KEY"));
                cnt = 0;
                StringBuilder keyTableAllColumns = new StringBuilder();
                StringBuilder keyTableInsertValues = new StringBuilder();
                if (this.pkPort != null) {
                    keyTableAllColumns.append(idQuote + pkPort.mstrName + idQuote);

                    keyTableInsertValues.append('?');
                    if (namedValueList == false)
                        fieldPopulationOrder.add(pkPort);

                    cnt++;
                }

                for (int i = 0; i < this.mInPorts.length; i++) {
                    DimensionETLInPort port = (DimensionETLInPort) this.mInPorts[i];
                    if (port.mColumn != null && port.sk) {
                        if (cnt > 0) {
                            keyTableAllColumns.append(',');
                            keyTableInsertValues.append(',');
                        }
                        keyTableAllColumns.append(port.mColumn.getColumnName(idQuote, this.mDBCase));
                        keyTableInsertValues.append('?');
                        if (namedValueList == false)
                            fieldPopulationOrder.add(port);
                        cnt++;
                    }
                }
                setKeyTableAllColumns(keyTableAllColumns.toString());
                setKeyTableInsertValues(keyTableInsertValues.toString());
            }

            if (this.miMode == LOAD_KEY_TABLE_DIMENSION || this.miMode == TABLE_ONLY) {

                cnt = 0;
                if (this.pkPort != null) {
                    allColumns.append(idQuote + pkPort.mstrName + idQuote);
                    if (namedValueList)
                        insertValues.append("? as " + idQuote + pkPort.mstrName + idQuote);
                    else
                        insertValues.append('?');
                    fieldPopulationOrder.add(pkPort);
                    cnt++;
                }

                for (int i = 0; i < this.mInPorts.length; i++) {
                    DimensionETLInPort port = (DimensionETLInPort) this.mInPorts[i];
                    if (port.mColumn != null) {
                        if (cnt > 0) {
                            allColumns.append(',');
                            insertValues.append(',');
                        }
                        allColumns.append(port.mColumn.getColumnName(idQuote, this.mDBCase));
                        if (namedValueList)
                            insertValues.append("? as " + port.mColumn.getColumnName(idQuote, this.mDBCase));
                        else
                            insertValues.append('?');

                        fieldPopulationOrder.add(port);
                        cnt++;
                    }
                }
                setAllColumns(allColumns.toString());
                setInsertValues(insertValues.toString());
            }

            miFieldPopulationOrder = new ETLPort[fieldPopulationOrder.size()];
            fieldPopulationOrder.toArray(this.miFieldPopulationOrder);

            this.msInBatchSQLStatement = this.buildInBatchSQL();

            this.stmt = prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement, this.jdbcHelper);

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

            this.miRetryBatch = XMLHelper.getAttributeAsInt(nmAttrs, "RETRYBATCH", 1);

            this.mbReinitOnError = XMLHelper.getAttributeAsBoolean(nmAttrs, "RECONNECTONERROR", true);

            this.mIncrementalCommit = XMLHelper.getAttributeAsBoolean(nmAttrs, "INCREMENTALCOMMIT", true);

            if (this.mIncrementalCommit == false && this.supportsSetSavepoint == false) {
                throw new KETLThreadException(
                        "Incremental commit cannot be disabled for database's that do not support savepoints", this);
            }

        } catch (Exception e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, e.toString());
            return -1;
        }

        this.mAllowInsert = XMLHelper.getAttributeAsBoolean(nmAttrs, "INSERT", true);

        // Pull the commit size...
        this.miCommitSize = XMLHelper.getAttributeAsInt(nmAttrs, COMMITSIZE_ATTRIB, this.batchSize);
        // this.miMaxTransactionSize = XMLHelper.getAttributeAsInt(nmAttrs, MAXTRANSACTIONSIZE_ATTRIB, -1);

        this.mstrPrimaryKeyColumns = this.getPrimaryKeyColumns();
        this.mstrSourceKeyColumns = this.getSourceKeyColumns();

        try {
            if (this.miMode == LOAD_KEY_TABLE_DIMENSION || this.miMode == KEY_TABLE_ONLY) {
                prepareForKeyTable();
            }

            this.executePreStatements();
        } catch (SQLException e) {
            throw new KETLThreadException(e, this);
        }

        this.skData = new Object[this.mSKColCount];
        this.skIndx = new int[this.mSKColCount];

        for (int i = 0; i < this.mInPorts.length; i++)
            if (((DimensionETLInPort) this.mInPorts[i]).skColIndex != -1)
                this.skIndx[((DimensionETLInPort) this.mInPorts[i]).skColIndex] = this.mInPorts[i].getSourcePortIndex();

        return 0;
    }

    public Object[][] initializeBatch(Object[][] data, int len) throws KETLTransformException {
        try {
            if (this.lookupPendingLoad) {
                this.lookupPendingLoad = false;
                this.seedLookup();
            }
            if (this.firePreBatch && this.mBatchData) {
                this.executePreBatchStatements();
                recordNumBatchStart = this.getRecordsProcessed();
                this.firePreBatch = false;
            }

        } catch (Exception e) {
            throw new KETLTransformException(e);
        }
        return data;
    }

    protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException {
        if (hdl == null)
            return new JDBCItemHelper();
        else {
            try {
                Class cl = Class.forName(hdl);
                return (JDBCItemHelper) cl.newInstance();
            } catch (Exception e) {
                throw new KETLThreadException("HANDLER class not found", e, this);
            }
        }
    }

    private void logBatch(Object[] inputRecords) {
        mBatchLog.add(inputRecords);
    }

    private void prepareForKeyTable() throws KETLThreadException {
        String template = null;

        synchronized (this.mKeySource) {
            try {
                Statement mStmt = this.mcDBConnection.createStatement();

                template = this.getStepTemplate(mDBType, "CHECKFORKEYTABLE", true);
                template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrKeyTableName);
                template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);

                boolean exists = false;
                try {
                    ResultSet rs = mStmt.executeQuery(template);
                    while (rs.next()) {
                        exists = true;
                    }

                    rs.close();
                } catch (Exception e) {
                    mStmt.getConnection().rollback();
                }

                if (exists == false) {
                    template = this.getStepTemplate(mDBType, "CREATEKEYTABLE", true);

                    template = EngineConstants.replaceParameterV2(template, "KEYTABLENAME", this.mstrKeyTableName);

                    template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrTableName);
                    template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);

                    template = EngineConstants.replaceParameterV2(template, "PK_COLUMNS", this.mstrPrimaryKeyColumns);
                    template = EngineConstants.replaceParameterV2(template, "SK_COLUMNS", this.mstrSourceKeyColumns);

                    mStmt.executeUpdate(template);

                    template = this.getStepTemplate(mDBType, "CREATEKEYTABLEPKINDEX", true);
                    template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.mstrPrimaryKeyColumns);
                    template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrKeyTableName);
                    template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);

                    mStmt.executeUpdate(template);

                    template = this.getStepTemplate(mDBType, "CREATEKEYTABLESKINDEX", true);
                    template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.mstrSourceKeyColumns);
                    template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrKeyTableName);
                    template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);

                    mStmt.executeUpdate(template);
                }

                // load key table values into cache
            } catch (SQLException e) {
                throw new KETLThreadException("Error executing statement " + template, e, this);
            }
        }
    }

    StatementWrapper prepareStatementWrapper(Connection Connection, String sql, JDBCItemHelper jdbcHelper)
            throws SQLException {
        return JDBCStatementWrapper.prepareStatement(Connection, sql, jdbcHelper);
    }

    private void putKeyArrayDataArray(Object[] o, Integer data) {
        Object[] elements = new Object[this.mSKColCount];
        for (int i = 0; i < this.mSKColCount; i++) {
            elements[i] = o[this.skIndx[i]];
        }

        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, java.util.Arrays.toString(elements));
        this.mLookup.put(elements, new Object[] { data });
    }

    private int retryBatch() throws KETLTransformException {
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
                throw new KETLTransformException(e);
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
                        for (int i = 0; i < this.miFieldPopulationOrder.length; i++) {
                            ETLPort idx = this.miFieldPopulationOrder[i];

                            if (idx == this.pkPort) {
                                try {
                                    this.stmt.setParameterFromClass(i + 1, Integer.class, this.getSurrogateKey(record),
                                            maxCharLength, this.pkPort.getXMLConfig());
                                } catch (Error e) {
                                    throw new KETLTransformException(e.getMessage());
                                }
                            }
                            else {
                                ETLInPort inport = (ETLInPort) idx;

                                this.stmt.setParameterFromClass(i + 1, inport.getPortClass(),
                                        inport.isConstant() ? inport.getConstantValue() : record[inport
                                                .getSourcePortIndex()], maxCharLength, inport.getXMLConfig());
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
                            this.incrementErrorCount(new KETLTransformException("Record "
                                    + (this.miInsertCount + x + 1) + " failed to submit, " + e.toString(), e), record,
                                    this.miInsertCount + x + 1);

                        try {
                            this.stmt.close();
                            this.stmt = prepareStatementWrapper(mcDBConnection, this.msInBatchSQLStatement,
                                    this.jdbcHelper);
                        } catch (SQLException e1) {
                            throw new KETLTransformException(e1);
                        }

                    }
                }
            }

            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Batch retry attempt " + (r + 1) + " of "
                    + this.miRetryBatch + ", Records resubmitted: " + submitted + ", errors: " + errorCount);
        }

        return result;

    }

    protected void seedLookup() throws KETLThreadException {
    	
        
        this.setWaiting("lookup to seed");
        String template = this.getStepTemplate(mDBType, "SEEDLOOKUP", true);
        if (this.msKeyTableAllColumns != null) {

            template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrKeyTableName);
            template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);

        }
        if (this.msAllColumns != null) {
            template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrTableName);
            template = EngineConstants.replaceParameterV2(template, "SCHEMANAME", this.mstrSchemaName);

        }

        template = EngineConstants.replaceParameterV2(template, "PKCOLUMNS", this.getPrimaryKeyColumns());
        template = EngineConstants.replaceParameterV2(template, "SKCOLUMNS", this.getSourceKeyColumns());
        template = EngineConstants.replaceParameterV2(template, "PARTITIONS", Integer.toString(this.partitions));
        template = EngineConstants.replaceParameterV2(template, "PARTITIONID", Integer.toString(this.partitionID));

        Statement mStmt = null;
        try {

            mStmt = this.mcDBConnection.createStatement();

            ResultSet rs = mStmt.executeQuery(template);
            Object[] key = new Object[this.mInPorts.length];

            int x = 0;
            while (rs.next()) {
                for (int i = 0; i < this.mSKColCount; i++) {
                    Object data = this.jdbcHelper.getObjectFromResultSet(rs, i + 2,
                            this.getExpectedInputDataTypes()[this.skIndx[i]], this.maxCharLength);
                    if (data == null)
                        throw new KETLTransformException("NULL values are not allowed in the key table, check table "
                                + this.mstrKeyTableName + " for errors");
                    key[this.skIndx[i]] = data;

                }
                Integer sk = rs.getInt(1);

                if (sk > this.mKeySource.value()) {
                    this.mKeySource.set(sk);
                }

                /*
                 * if (mSKColCount == 1) putKeyObjectDataObject(key, sk); else
                 */
                try {
                    putKeyArrayDataArray(key, sk);
                } catch (Error e) {
                    throw new KETLTransformException(e);
                }
                if (++x % 20000 == 0)
                    this.setWaiting("lookup to seed. " + x + " records loaded");

            }

            this.setWaiting(null);
            rs.close();
            
            this.setWaiting("maximum surrogate value");
			String maxStatement = "select max(${PKCOLUMNS}) from ${SCHEMANAME}.${TABLENAME}";
			if (this.mstrKeyTableName != null) {
				template = EngineConstants.replaceParameterV2(maxStatement,
						"TABLENAME", this.mstrKeyTableName);
				template = EngineConstants.replaceParameterV2(template,
						"SCHEMANAME", this.mstrSchemaName);
				template = EngineConstants.replaceParameterV2(template,
						"PKCOLUMNS", this.getPrimaryKeyColumns());

				rs = mStmt.executeQuery(template);
				while (rs.next()) {
					Integer sk = rs.getInt(1);

					if (sk > this.mKeySource.value()) {
						this.mKeySource.set(sk);
					}
				}
				rs.close();
			}

			if (this.mstrTableName != null) {
				template = EngineConstants.replaceParameterV2(maxStatement,
						"TABLENAME", this.mstrTableName);
				template = EngineConstants.replaceParameterV2(template,
						"SCHEMANAME", this.mstrSchemaName);
				template = EngineConstants.replaceParameterV2(template,
						"PKCOLUMNS", this.getPrimaryKeyColumns());

				rs = mStmt.executeQuery(template);
				while (rs.next()) {
					Integer sk = rs.getInt(1);

					if (sk > this.mKeySource.value()) {
						this.mKeySource.set(sk);
					}
				}
				rs.close();
			}
        	
            mStmt.close();
        } catch (Exception e) {
            if (mStmt != null)
                try {
                    mStmt.close();
                } catch (Exception e1) {
                }
            throw new KETLThreadException(e, this);
        }

    }

    void setAllColumns(String msAllColumns) {
        this.msAllColumns = msAllColumns;
    }

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
        case -1:
            return pStr;
        }

        return pStr;
    }

    void setInsertValues(String msInsertValues) {
        this.msInsertValues = msInsertValues;
    }

    void setKeyTableAllColumns(String msAllColumns) {
        this.msKeyTableAllColumns = msAllColumns;
    }

    void setKeyTableInsertValues(String msInsertValues) {
        this.msKeyTableInsertValues = msInsertValues;
    }

    public PersistentMap swichToReadOnlyMode() {
        this.mLookup.switchToReadOnlyMode();
        return this.mLookup;
    }

}
