/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.reader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

/**
 * <p>
 * Title: JDBCReader
 * </p>
 * <p>
 * Description: Read an object array or ResultRecord from a JDBC datasource, based on ETLReader
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * Produces an output TABLE SCHEMA OWNER COLUMN DATATYPE LENGTH DOV % NULL VALUES DISTINCT
 * 
 * @author Brian Sullivan
 * @version 1.0
 */
public class JDBCSSAScanner extends ETLReader implements DefaultReaderCore, DBConnection {

    public JDBCSSAScanner(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    public static final String SQL_ATTRIB = "SQL"; // REMOVE
    private Connection mcDBConnection;
    String mstrDefaultSQL = null;
    String mstrExecutingSQL = null;
    public int mintFetchSize = -1;
    String mstrCatalog;
    static private String MAX_DOV_SIZE = "MAXDOVSIZE";
    private int mMaxDOVSize = 16;
    String mstrSchemaPattern;
    String mstrTableNamePattern;
    private ArrayList mTableList = null;

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new SSAOutPort(this, srcStep);
    }

    class SSAOutPort extends ETLOutPort {

        @Override
        public Class getPortClass() {
            // TODO Auto-generated method stub
            return super.getPortClass();
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {

            String type = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "TYPE", null);
            if (type != null) {
                if (type.equalsIgnoreCase("TABLE_NAME") || type.equalsIgnoreCase("TABLE_SCHEMA")
                        || type.equalsIgnoreCase("TABLE_TYPE") || type.equalsIgnoreCase("TABLE_CAT")
                        || type.equalsIgnoreCase("COLUMN_NAME") || type.equalsIgnoreCase("TYPE_NAME")
                        || type.equalsIgnoreCase("COLUMN_DEF") || type.equalsIgnoreCase("REMARKS")
                        || type.equalsIgnoreCase("IS_NULLABLE") || type.equalsIgnoreCase("MAX_VALUE")
                        || type.equalsIgnoreCase("MIN_VALUE") || type.equalsIgnoreCase("DOV")
                        || type.equalsIgnoreCase("SAMPLE") || type.equalsIgnoreCase("COLUMNERRORMESSAGE")
                        || type.equalsIgnoreCase("TABLEERRORMESSAGE") || type.equalsIgnoreCase("TABLE_COMPLETENESS")) {
                    type = "STRING";
                }
                else {
                    type = "INTEGER";
                }
                ((Element) xmlConfig).setAttribute("DATATYPE", type);
            }

            return super.initialize(xmlConfig);

        }

        public SSAOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);

        }

        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }

    }

    private String idQuote;

    public ArrayList getTables() {
        // if connection open then close it and open next
        if (mcDBConnection != null) {
            // return connection to resourcepool
            ResourcePool.releaseConnection(this.mcDBConnection);
        }

        if (this.maParameters.size() == 0) {
            // return null as no more parameter sets left
            return null;
        }

        if (openConnection(this.getParameterValue(0, DRIVER_ATTRIB), this.getParameterValue(0, URL_ATTRIB), this
                .getParameterValue(0, USER_ATTRIB), this.getParameterValue(0, PASSWORD_ATTRIB)) == null) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not establish database connection");

            return null;
        }

        try {

            this.mDBType = this.mcDBConnection.getMetaData().getDatabaseProductName();
        } catch (Exception e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Unable to get database type from metadata: "
                    + e.toString());

            return null;
        }

        ResultSet mrsDBResultSet = null;

        // remove parameter set
        this.maParameters.remove(0);

        ArrayList res = new ArrayList();
        // Run the query...
        try {
            DatabaseMetaData md = mcDBConnection.getMetaData();
            mrsDBResultSet = md.getTables(mstrCatalog, mstrSchemaPattern, mstrTableNamePattern, null);
            idQuote = md.getIdentifierQuoteString();
            if (idQuote == null || idQuote.equals(" ")) {
                idQuote = "";
            }

            while (mrsDBResultSet.next()) {
                Table cTable = new Table();

                cTable.execute = res.size() % this.partitions == this.partitionID;
                String type = mrsDBResultSet.getString("TABLE_TYPE");
                if (type.contains("TABLE") || type.contains("VIEW")) {

                    cTable.mSchema = mrsDBResultSet.getString("TABLE_SCHEM");
                    cTable.mTableName = mrsDBResultSet.getString("TABLE_NAME");
                    cTable.mType = mrsDBResultSet.getString("TABLE_TYPE");

                    cTable.mCatalog = mrsDBResultSet.getString("TABLE_CAT");

                    if (cTable.mSchema != null)
                        cTable.mFullTableAddress = idQuote + cTable.mSchema + idQuote + "." + idQuote
                                + cTable.mTableName + idQuote;
                    else if (cTable.mSchema == null && cTable.mCatalog != null)
                        cTable.mFullTableAddress = idQuote + cTable.mCatalog + idQuote + "." + idQuote
                                + cTable.mTableName + idQuote;
                    else
                        cTable.mFullTableAddress = idQuote + cTable.mTableName + idQuote;
                    res.add(cTable);
                }
            }

            mrsDBResultSet.close();
        } catch (Exception e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Unable to get table information from metadata: "
                    + e.toString());

            return null;
        }

        // Return the actual db result set, in case the caller wants to do
        // something without our conversion to the ResultRecord...
        return res;
    }

    boolean rowExists = false;

    class Table {

        String mTableName, mSchema, mType, mCatalog;
        int mRowCount;
        boolean execute;

        String mFullTableAddress;
        ResultSet mColumns;
        ArrayList mColumnList = new ArrayList();
        ArrayList mErrorMessage = new ArrayList();
        public String completeness;
    }

    class Column {

        String mNullable, mName, mDTypeName, mRemarks, mDefaultValue;
        int mDType, mColSize, mDecDigits, mRadixPrec, mCharOctetLength, mColPosition;
        Table mTable;
        String mMinValue, mMaxValue;
        int mDistinctVals;
        int mNullValues;
        String mSample;
        String mDOV;
        ArrayList mErrorMessage = new ArrayList();
    }

    private Table mCurrentTable = null;
    private String mDBType;

    private Table getNextTable() throws Exception {

        if (this.mTableList.size() == 0)
            return null;

        Table cTable = (Table) this.mTableList.remove(0);

        if (cTable.execute == false)
            return getNextTable();

        cTable.mRowCount = this.getRowCount(cTable);

        ResultSet rs = null;
        try {
            rs = mcDBConnection.getMetaData().getColumns(cTable.mCatalog, cTable.mSchema, cTable.mTableName, "%");
        } catch (SQLException e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not scan table: "
                    + cTable.mFullTableAddress + ", cause " + e.getMessage());
            return getNextTable();
        }
        String mColSQL = this.getStepTemplate(mDBType, "COLUMNSTATS", true);
        String mColLOBSQL = this.getStepTemplate(mDBType, "COLUMNLOBSTATS", true);

        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            Column col = new Column();
            col.mTable = cTable;
            col.mName = rs.getString("COLUMN_NAME");
            col.mDType = rs.getInt("DATA_TYPE");
            try {
                col.mDTypeName = rs.getString("TYPE_NAME");
                col.mColSize = rs.getInt("COLUMN_SIZE");
                col.mDecDigits = rs.getInt("DECIMAL_DIGITS");
                col.mRadixPrec = rs.getInt("NUM_PREC_RADIX");
                col.mCharOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
                col.mColPosition = rs.getInt("ORDINAL_POSITION");
                col.mDefaultValue = rs.getString("COLUMN_DEF");
                col.mRemarks = rs.getString("REMARKS");
                col.mNullable = rs.getString("IS_NULLABLE");
                cTable.mColumnList.add(col);
            } catch (Exception e) {
                // skip errors in oracle driver
            }
            if (cTable.mColumnList.size() > 1)
                sb.append(',');

            switch (col.mDType) {
            case java.sql.Types.CLOB:
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.JAVA_OBJECT:
            case java.sql.Types.OTHER:
            case java.sql.Types.REF:
                sb.append(EngineConstants.replaceParameterV2(mColLOBSQL, "COL", idQuote + col.mName + idQuote));
                break;

            default:
                sb.append(EngineConstants.replaceParameterV2(mColSQL, "COL", idQuote + col.mName + idQuote));
            }

        }

        rs.close();

        if (cTable.mColumnList.size() == 0) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not scan table "
                    + cTable.mFullTableAddress + ", cause \"No columns found\"");
            return getNextTable();
        }

        cTable.completeness = this.getCompleteness(cTable);

        String query = this.getStepTemplate(mDBType, "COLUMNQUERY", true);

        query = EngineConstants.replaceParameterV2(query, "TABLE", cTable.mFullTableAddress);
        query = EngineConstants.replaceParameterV2(query, "COL", sb.toString());

        Statement s = null;

        try {
            s = mcDBConnection.createStatement();

            rs = s.executeQuery(query);

            if (rs.next()) {
                int pos = 1;
                for (int i = 0; i < cTable.mColumnList.size(); i++) {
                    Column col = (Column) cTable.mColumnList.get(i);
                    col.mMinValue = rs.getString(pos++);
                    col.mMaxValue = rs.getString(pos++);
                    col.mDistinctVals = rs.getInt(pos++);
                    if (rs.wasNull())
                        col.mDistinctVals = -1;
                    col.mNullValues = rs.getInt(pos++);
                }
            }

            rs.close();
        } catch (SQLException e) {
            cTable.mErrorMessage.add("Unable to retrieve column info for table '" + cTable.mFullTableAddress + "': "
                    + e.toString());
            try {
                this.mcDBConnection.rollback();
            } catch (Exception e1) {

            }
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException x) {
                }
            }
        }

        return cTable;
    }

    private String getDOV(Column col) throws Exception {

        String sql = this.getStepTemplate(mDBType, "DOV", true);

        sql = EngineConstants.replaceParameterV2(sql, "COL", idQuote + col.mName + idQuote);
        sql = EngineConstants.replaceParameterV2(sql, "TABLE", col.mTable.mFullTableAddress);

        Statement s = null;
        ResultSet rs = null;

        try {
            s = mcDBConnection.createStatement();

            rs = s.executeQuery(sql);

            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                if (sb.length() > 0)
                    sb.append(", ");
                sb.append(rs.getString(1));
                sb.append(" (" + rs.getInt(2) + ")");
            }
            return sb.toString();
        } catch (SQLException e) {
            col.mErrorMessage.add("Unable to retrieve dov for column " + col.mName + " in table"
                    + col.mTable.mFullTableAddress + " " + e.toString());
            try {
                this.mcDBConnection.rollback();
            } catch (Exception e1) {
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException x) {
                }
            }

            if (s != null) {
                try {
                    s.close();
                } catch (SQLException x) {
                }
            }
        }
        return null;
    }

    private String getSample(Column col) throws Exception {

        String sql = this.getStepTemplate(mDBType, "SAMPLE", true);

        sql = EngineConstants.replaceParameterV2(sql, "COL", idQuote + col.mName + idQuote);
        sql = EngineConstants.replaceParameterV2(sql, "TABLE", col.mTable.mFullTableAddress);

        Statement s = null;
        ResultSet rs = null;

        try {
            s = mcDBConnection.createStatement();

            s.setFetchSize(10);
            rs = s.executeQuery(sql);
            rs.setFetchSize(10);
            StringBuilder sb = new StringBuilder();
            int items = 0;
            while (rs.next() && items++ < 10) {
                if (sb.length() > 0)
                    sb.append(", ");
                sb.append(rs.getString(1));
            }
            return sb.toString();
        } catch (SQLException e) {
            col.mErrorMessage.add("ERROR: Unable to retrieve sample for column " + col.mName + " in table"
                    + col.mTable.mFullTableAddress + " " + e.toString());
            try {
                this.mcDBConnection.rollback();
            } catch (Exception e1) {
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException x) {
                }
            }

            if (s != null) {
                try {
                    s.close();
                } catch (SQLException x) {
                }
            }
        }
        return null;
    }

    // Returns -1 on error.
    int getRowCount(Table tbl) {
        int iRowCount = -1;

        Statement s = null;
        ResultSet rs = null;

        try {
            s = mcDBConnection.createStatement();

            rs = s.executeQuery("select count(*) from " + tbl.mFullTableAddress);

            if (rs.next()) {
                iRowCount = rs.getInt(1);
            }
        } catch (SQLException e) {
            tbl.mErrorMessage.add("ERROR: Unable to retrieve row count for table '" + tbl.mFullTableAddress + "': "
                    + e.toString());
            try {
                this.mcDBConnection.rollback();
            } catch (Exception e1) {
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException x) {
                }
            }

            if (s != null) {
                try {
                    s.close();
                } catch (SQLException x) {
                }
            }
        }

        return iRowCount;
    }

    // Returns -1 on error.
    String getCompleteness(Table tbl) throws KETLThreadException {

        String sql = this.getStepTemplate(mDBType, "COMPLETENESS", true);
        String colExp = this.getStepTemplate(mDBType, "COMPLETENESSCOLEXPRESSION", true);

        sql = EngineConstants.replaceParameterV2(sql, "COLCOUNT", Integer.toString(tbl.mColumnList.size()));
        sql = EngineConstants.replaceParameterV2(sql, "TABLE", tbl.mFullTableAddress);

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < tbl.mColumnList.size(); i++) {
            Column cl = (Column) tbl.mColumnList.get(i);

            if (i > 0)
                sb.append(" + ");
            sb.append(EngineConstants.replaceParameterV2(colExp, "COL", idQuote + cl.mName + idQuote));
        }

        sql = EngineConstants.replaceParameterV2(sql, "COLS", sb.toString());

        Statement s = null;
        ResultSet rs = null;

        try {
            s = mcDBConnection.createStatement();

            rs = s.executeQuery(sql);

            sb = new StringBuffer();
            int pct = 0;
            while (rs.next()) {
                for (int i = 1; i <= 10; i++) {
                    if (sb.length() > 0)
                        sb.append(", ");
                    sb.append(pct + "-" + (pct + 10) + "% = " + rs.getInt(i));
                    pct += 10;
                }
            }
            return sb.toString();
        } catch (SQLException e) {
            return "Unable to retrieve completeness for table" + tbl.mFullTableAddress + " " + e.toString();

        } finally {
            try {
                this.mcDBConnection.rollback();
            } catch (Exception e1) {
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException x) {
                }
            }

            if (s != null) {
                try {
                    s.close();
                } catch (SQLException x) {
                }
            }
        }

    }

    public int initialize(Node xmlConfig) throws KETLThreadException {
        int iReturnVal = 0;

        if ((iReturnVal = super.initialize(xmlConfig)) != 0) {
            return iReturnVal;
        }

        // Pull the catalog name...
        mstrCatalog = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "CATALOG", null);

        // Pull the schema pattern...
        mstrSchemaPattern = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "SCHEMA", null);

        // Pull the table pattern...
        mstrTableNamePattern = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "TABLE", "%");

        this.mMaxDOVSize = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), MAX_DOV_SIZE, this.mMaxDOVSize);

        return 0;
    }

    // Opens a new connection with the given information.
    public Connection openConnection(String strDriverClass, String strURL, String strUserName, String strPassword) {
        try {
            mcDBConnection = ResourcePool.getConnection(strDriverClass, strURL, strUserName, strPassword, null, true);

            return mcDBConnection;
        } catch (Exception e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Unable to connect to host '" + strURL + "': "
                    + e.toString());

            mcDBConnection = null;

            return null;
        }
    }

    public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException {

        if (this.mTableList == null) {
            if (this.partitionID == 0)
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Getting list of objects to scan");
            this.mTableList = this.getTables();
            if (this.partitionID == 0)
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                        "List of tables fetched, Total number of objects "
                                + (this.mTableList == null ? 0 : this.mTableList.size()));
        }

        if (mCurrentTable == null) {
            try {
                mCurrentTable = getNextTable();

                if (mCurrentTable == null)
                    return COMPLETE;

                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Scanning table "
                        + this.mCurrentTable.mFullTableAddress);

            } catch (Exception e) {
                throw new KETLReadException(e);
            }
        }

        Column col = (Column) mCurrentTable.mColumnList.remove(0);
        if (mCurrentTable.mColumnList.size() == 0)
            this.mCurrentTable = null;

        int pos = 0;
        for (int i = 0; i < this.mOutPorts.length; i++) {
            if (this.mOutPorts[i].isUsed()) {
                if (this.mOutPorts[i].isConstant())
                    pResultArray[pos++] = this.mOutPorts[i].getConstantValue();
                else {
                    String type = XMLHelper.getAttributeAsString(this.mOutPorts[i].getXMLConfig().getAttributes(),
                            "TYPE", null);
                    if (type != null) {
                        if (type.equalsIgnoreCase("TABLE_NAME")) {
                            pResultArray[pos++] = (col.mTable.mTableName);
                        }
                        else if (type.equalsIgnoreCase("TABLE_SCHEMA")) {
                            pResultArray[pos++] = (col.mTable.mSchema);
                        }
                        else if (type.equalsIgnoreCase("TABLE_TYPE")) {
                            pResultArray[pos++] = (col.mTable.mType);
                        }
                        else if (type.equalsIgnoreCase("TABLE_CAT")) {
                            pResultArray[pos++] = (col.mTable.mCatalog);
                        }
                        else if (type.equalsIgnoreCase("TABLE_COMPLETENESS")) {
                            pResultArray[pos++] = (col.mTable.completeness);
                        }
                        else if (type.equalsIgnoreCase("TYPE_NAME")) {
                            pResultArray[pos++] = (col.mDTypeName);
                        }
                        else if (type.equalsIgnoreCase("ROW_COUNT")) {
                            pResultArray[pos++] = (col.mTable.mRowCount);
                        }
                        else if (type.equalsIgnoreCase("DATA_TYPE")) {
                            pResultArray[pos++] = (col.mDType);
                        }
                        else if (type.equalsIgnoreCase("COLUMN_NAME")) {
                            pResultArray[pos++] = (col.mName);
                        }
                        else if (type.equalsIgnoreCase("CHAR_OCTET_LENGTH")) {
                            pResultArray[pos++] = (col.mCharOctetLength);
                        }
                        else if (type.equalsIgnoreCase("ORDINAL_POSITION")) {
                            pResultArray[pos++] = (col.mColPosition);
                        }
                        else if (type.equalsIgnoreCase("COLUMN_SIZE")) {
                            pResultArray[pos++] = (col.mColSize);
                        }
                        else if (type.equalsIgnoreCase("DECIMAL_DIGITS")) {
                            pResultArray[pos++] = (col.mDecDigits);
                        }
                        else if (type.equalsIgnoreCase("COLUMN_DEF")) {
                            pResultArray[pos++] = (col.mDefaultValue);
                        }
                        else if (type.equalsIgnoreCase("REMARKS")) {
                            pResultArray[pos++] = (col.mRemarks);
                        }
                        else if (type.equalsIgnoreCase("IS_NULLABLE")) {
                            pResultArray[pos++] = (col.mNullable);
                        }
                        else if (type.equalsIgnoreCase("NUM_PREC_RADIX")) {
                            pResultArray[pos++] = (col.mRadixPrec);
                        }
                        else if (type.equalsIgnoreCase("NULL_VALUES")) {
                            pResultArray[pos++] = (col.mNullValues);
                        }
                        else if (type.equalsIgnoreCase("DISTINCT_VALUES")) {
                            pResultArray[pos++] = (col.mDistinctVals);
                        }
                        else if (type.equalsIgnoreCase("MAX_VALUE")) {
                            pResultArray[pos++] = (col.mMaxValue);
                        }
                        else if (type.equalsIgnoreCase("MIN_VALUE")) {
                            pResultArray[pos++] = (col.mMinValue);
                        }
                        else if (type.equalsIgnoreCase("DOV")) {
                            if (col.mDistinctVals <= this.mMaxDOVSize)
                                try {
                                    pResultArray[pos++] = (getDOV(col));
                                } catch (Exception e) {
                                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, e.toString());
                                }
                            else
                                pResultArray[pos++] = null;
                        }
                        else if (type.equalsIgnoreCase("SAMPLE")) {
                            try {
                                pResultArray[pos++] = (getSample(col));
                            } catch (Exception e) {
                                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, e.toString());
                            }
                        }
                        else if (type.equalsIgnoreCase("COLUMNERRORMESSAGE")) {
                            StringBuilder sb = new StringBuilder();
                            for (Object o : col.mErrorMessage) {
                                if (sb.length() > 0)
                                    sb.append('\n');
                                sb.append(o.toString());

                            }
                            pResultArray[pos++] = (sb.toString());
                        }
                        else if (type.equalsIgnoreCase("TABLEERRORMESSAGE")) {
                            StringBuilder sb = new StringBuilder();
                            for (Object o : col.mTable.mErrorMessage) {
                                if (sb.length() > 0)
                                    sb.append('\n');
                                sb.append(o.toString());

                            }
                            pResultArray[pos++] = (sb.toString());
                        }
                    }
                }
            }
        }
        return 1;
    }

    public Connection getConnection() {
        return this.mcDBConnection;
    }

    @Override
    protected void close(boolean success) {
        if (this.mcDBConnection != null) {
            ResourcePool.releaseConnection(this.mcDBConnection);
            this.mcDBConnection = null;
        }
    }
}
