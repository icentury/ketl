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
package com.kni.etl.ketl.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.DataItemHelper;
import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.SQLQuery;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.qa.QAEventGenerator;
import com.kni.etl.ketl.qa.QAForJDBCReader;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
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
 * </p>.
 * 
 * @author Brian Sullivan
 * @version 1.0
 */
public class JDBCReader extends ETLReader implements DefaultReaderCore, QAForJDBCReader, DBConnection, PrePostSQL {

    /**
     * The Class JDBCReaderETLOutPort.
     */
    public class JDBCReaderETLOutPort extends ETLOutPort {

        /**
         * Instantiates a new JDBC reader ETL out port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public JDBCReaderETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#containsCode()
         */
        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            return 0;
        }

    }

    /** The Constant ColPrecision. */
    final static int ColPrecision = 1;
    
    /** The Constant ColScale. */
    final static int ColScale = 2;
    
    /** The Constant ColType. */
    final static int ColType = 0;
    
    /** The Constant PRESQL_ATTRIB. */
    public static final String PRESQL_ATTRIB = "PRESQL";
    
    /** The Constant SQL_SAMPLE_ATTRIB. */
    public static final String SQL_SAMPLE_ATTRIB = "SQLSAMPLE";
    
    /** The jdbc helper. */
    private JDBCItemHelper jdbcHelper;
    
    /** The max char length. */
    private int maxCharLength = 0;
    
    /** The mb parameters resolved. */
    boolean mbParametersResolved = false;
    
    /** The mc DB connection. */
    private Connection mcDBConnection;

    /** The m col metadata. */
    int mColMetadata[][];

    /** The mint fetch size. */
    public int mintFetchSize = -1;

    /** The mrs DB result set. */
    private ResultSet mrsDBResultSet;

    /** The m SQL sample. */
    String mSQLSample = "";

    /** The m SQL statements. */
    ArrayList mSQLStatements;
    
    /** The m stmt. */
    private Statement mStmt;
    
    /** The mstr default SQL. */
    String mstrDefaultSQL;
    
    /** The mstr executing SQL. */
    String mstrExecutingSQL;

    /** The row is valid. */
    private boolean rowIsValid;
    
    /** The m fetch size. */
    private int mFetchSize;

    /**
     * Instantiates a new JDBC reader.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public JDBCReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        
        
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
        
       
    }

    /**
     * Instantiate helper.
     * 
     * @param pXMLConfig the XML config
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    private void instantiateHelper(Node pXMLConfig) throws KETLThreadException {
        if(this.jdbcHelper != null) return;
        
        String hdl = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), "HANDLER", null);
        
        if (hdl == null)
            this.jdbcHelper = new JDBCItemHelper();
        else {
            try {
                Class cl = Class.forName(hdl);
                this.jdbcHelper = (JDBCItemHelper) cl.newInstance();
            } catch (Exception e) {
                throw new KETLThreadException("HANDLER class not found", e, this);
            }
        }
    }
    
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLReader#alwaysOverrideOuts()
     */
    @Override
    protected boolean alwaysOverrideOuts() {
        return true;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {
        if (this.mcDBConnection != null) {
            ResourcePool.releaseConnection(this.mcDBConnection);
            this.mcDBConnection = null;
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePostBatchStatements()
     */
    public void executePostBatchStatements() throws SQLException {
        throw new RuntimeException("Post batch not implemented");
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePostStatements()
     */
    public void executePostStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
     */
    public void executePreBatchStatements() throws SQLException {
        throw new RuntimeException("Pre batch not implemented");
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
     */
    public void executePreStatements() throws SQLException {
        StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.DBConnection#getConnection()
     */
    public Connection getConnection() {
        return this.mcDBConnection;
    }

    /**
     * Gets the connection.
     * 
     * @param paramList the param list
     * 
     * @return the connection
     * 
     * @throws SQLException the SQL exception
     * @throws ClassNotFoundException the class not found exception
     */
    private Connection getConnection(int paramList) throws SQLException, ClassNotFoundException {
        if (this.mcDBConnection != null)
            ResourcePool.releaseConnection(this.mcDBConnection);
        this.mcDBConnection = ResourcePool.getConnection(this.getParameterValue(paramList, DBConnection.DRIVER_ATTRIB), this
                .getParameterValue(paramList, DBConnection.URL_ATTRIB), this.getParameterValue(paramList, DBConnection.USER_ATTRIB), this
                .getParameterValue(paramList, DBConnection.PASSWORD_ATTRIB), this.getParameterValue(paramList, JDBCReader.PRESQL_ATTRIB), true);

        return this.mcDBConnection;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.qa.QAForJDBCReader#getConnnection(int)
     */
    public Connection getConnnection(int pos) {
        return this.mcDBConnection;
    }

    /**
     * Gets the java type.
     * 
     * @param pSQLType the SQL type
     * @param pLength the length
     * @param pPrecision the precision
     * @param pScale the scale
     * 
     * @return the java type
     */
    private String getJavaType(int pSQLType, int pLength, int pPrecision, int pScale) {

       
            return this.jdbcHelper.getJavaType(pSQLType, pLength, pPrecision, pScale);
       
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLOutPort getNewOutPort(com.kni.etl.ketl.ETLStep srcStep) {
        return new JDBCReaderETLOutPort(this, this);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.DefaultReaderCore#getNextRecord(java.lang.Object[], java.lang.Class[], int)
     */
    public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException {

        try {
            // cycle through parameters fetching resultSets
            while ((this.mSQLStatements.size() > 0) && (this.mrsDBResultSet == null)) {
                if (this.runSQL() == false) {
                    throw new KETLReadException("Step halted, problems getting resultset, see previous errors.");
                }

                // if statement found pull first row
                if (this.mrsDBResultSet != null) {
                    this.rowIsValid = this.mrsDBResultSet.next();

                    // if row was nothing then get next resultSet
                    // this will protect against empty resultsets
                    if (this.rowIsValid == false) {
                        if (this.mrsDBResultSet != null) {
                            this.mrsDBResultSet.close();
                        }

                        this.mrsDBResultSet = null;

                        this.mStmt.close();
                        this.executePostStatements();
                        // return connection to resourcepool
                        ResourcePool.releaseConnection(this.mcDBConnection);

                        // get next resultset
                        // if none return null
                    }

                }
            }

            // If there are no results, then just return null...
            if (this.mrsDBResultSet == null) {
                // fetch resultset from pool of connection threads
                return DefaultReaderCore.COMPLETE;
            }

            // Convert next record into ResultRecord and pass back...
            if (this.rowIsValid) {
                int iColumnCount;
                // cache the metadata, as som jdbc drivers are slow!
                if (this.mColMetadata == null) {
                    iColumnCount = this.mrsDBResultSet.getMetaData().getColumnCount();
                    this.mColMetadata = new int[iColumnCount][3];
                    for (int i = 0; i < iColumnCount; i++) {
                        this.mColMetadata[i][JDBCReader.ColType] = this.mrsDBResultSet.getMetaData().getColumnType(i + 1);

                        this.mColMetadata[i][JDBCReader.ColPrecision] = this.mrsDBResultSet.getMetaData().getPrecision(i + 1);
                        this.mColMetadata[i][JDBCReader.ColScale] = this.mrsDBResultSet.getMetaData().getScale(i + 1);
                    }
                }
                else {
                    iColumnCount = this.mColMetadata.length;
                }

                int pos = 0;

                for (int i = 0; i < iColumnCount; i++) {
                    // Use DataItem helper class to map to best possible data type fro SQL datatype.
                    if (this.mOutPorts[i].isUsed()) {
                        try {
                            pResultArray[pos] = this.jdbcHelper.getObjectFromResultSet(this.mrsDBResultSet, i + 1,
                                    pExpectedDataTypes[pos++], this.maxCharLength);
                        } catch (Exception e) {
                            throw new KETLReadException("Exception with out " + this.mOutPorts[i].mstrName + ": "
                                    + e.getMessage(), e);
                        }
                    }
                }

                this.rowIsValid = this.mrsDBResultSet.next();
            }

            if (this.rowIsValid == false) {
                // if last row has been processed make sure cursor is closed.
                try {
                    if (this.mrsDBResultSet != null) {
                        this.mrsDBResultSet.close();
                    }

                    this.mStmt.close();

                    // return connection to resourcepool
                    ResourcePool.releaseConnection(this.mcDBConnection);
                } catch (SQLException e) {
                    // Closeout error.
                    ResourcePool.LogException(e, this);
                }

                this.mrsDBResultSet = null;
            }
        } catch (Exception e) {
            // return connection to resourcepool
            if (this.mStmt != null)
                try {
                    this.mStmt.close();
                } catch (Exception e1) {
                    ResourcePool.LogException(e, this);
                }

            if (e instanceof SQLException) {
                SQLException eSQL = (SQLException) e;

                while (eSQL != null && eSQL.getNextException() != eSQL) {
                    eSQL = eSQL.getNextException();
                    if (eSQL != null)
                        ResourcePool.LogException(e, this);
                }
            }

            if (this.mcDBConnection != null)
                ResourcePool.releaseConnection(this.mcDBConnection);

            if (e instanceof KETLReadException)
                throw (KETLReadException) e;
            throw new KETLReadException(e);
        }

        return 1;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLStep#getQAClass(java.lang.String)
     */
    @Override
    public String getQAClass(String strQAType) {
        if (strQAType.equalsIgnoreCase(QAEventGenerator.SIZE_TAG)) {
            return QAForJDBCReader.QA_SIZE_CLASSNAME;
        }

        return super.getQAClass(strQAType);
    }

    /**
     * Gets the SQL.
     * 
     * @return Returns the mstrDefaultSQL.
     */
    public String getSQL() {
        return this.mstrDefaultSQL;
    }

    /**
     * Gets the SQL statement array.
     * 
     * @return the SQL statement array
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    private ArrayList getSQLStatementArray() throws KETLThreadException {
        this.mSQLStatements = new ArrayList();

        Node[] nodes = XMLHelper.getElementsByName(this.getXMLConfig(), "IN", "*", "*");

        if (nodes != null) {
            for (Node element : nodes) {
                String sql = XMLHelper.getTextContent(element);
                if (sql != null && sql.equals("") == false) {
                    sql = sql.trim();
                    if (sql.startsWith("\"") && sql.endsWith("\"")) {
                        sql = sql.substring(1, sql.length() - 1);
                    }

                    if (sql.length() > 0)
                        this.mSQLStatements.add(new SQLQuery(sql, 0,
                                this.mSQLStatements.size() % this.partitions == this.partitionID));
                }
            }
        }

        for (int i = 0; i < this.maParameters.size(); i++) {
            String sql = this.getParameterValue(i, ETLStep.SQL_ATTRIB);

            if (sql != null) {
                this.mSQLStatements.add(new SQLQuery(null, i, this.mSQLStatements.size() % this.partitions == this.partitionID));
            }
        }

        return this.mSQLStatements;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.qa.QAForJDBCReader#getSQLStatements()
     */
    public SQLQuery[] getSQLStatements() throws KETLThreadException {
        ArrayList sql = this.getSQLStatementArray();

        if ((sql != null) && (sql.size() > 0)) {

            for (int pos = 0; pos < sql.size(); pos++) {
                SQLQuery p = (SQLQuery) sql.get(pos);

                if (p.getParameterListID() != -1 && p.getSQL() == null)
                    p.setSQL(this.getParameterValue(p.getParameterListID(), ETLStep.SQL_ATTRIB));

            }
        }

        SQLQuery[] res = new SQLQuery[sql.size()];
        sql.toArray(res);
        return res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node xmlSourceNode) throws KETLThreadException {
        int res = super.initialize(xmlSourceNode);

        if (res != 0)
            return res;

        if (this.mbParametersResolved == false) {
            this.resolveParameters();
        }

        this.mFetchSize = XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), "FETCHSIZE", this.batchSize * 2);
        this.mSQLSample = XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(), JDBCReader.SQL_SAMPLE_ATTRIB, "");
        
        this.instantiateHelper(xmlSourceNode);
        
        // remove the queries not destined for this partition
        Object[] items = this.mSQLStatements.toArray();
        this.mSQLStatements.clear();
        for (Object element : items) {
            if (((SQLQuery) element).executeQuery())
                this.mSQLStatements.add(element);
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLReader#overrideOuts() Determine outs from query.
     */
    @Override
    protected void overrideOuts() throws KETLThreadException {
        super.overrideOuts();
        
        this.instantiateHelper(this.getXMLConfig());

        if (this.mbParametersResolved == false) {
            this.resolveParameters();
        }

        SQLQuery[] aSQLStatement;
        String sql = "N/A";

        // if inferred then base outputs on SQL statement being run

        aSQLStatement = this.getSQLStatements();

        if (aSQLStatement == null || aSQLStatement.length == 0)
            throw new KETLThreadException("ERROR: No SQL statements found", this);

        if (JDBCReader.outsResolved(this.getXMLConfig())) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Outputs resolved in another partition");
            return;
        }

        if (aSQLStatement.length > 0) {

            if (aSQLStatement.length > 1)
                ResourcePool
                        .LogMessage(this, ResourcePool.INFO_MESSAGE,
                                "Multiple SQL statements have been found, the first one will be used to derive the record structure");

            try {
                Connection mcDBConnection;
                try {
                    mcDBConnection = this.getConnection(aSQLStatement[0].getParameterListID());
                } catch (ClassNotFoundException e) {
                    throw new KETLThreadException(e, this);
                }

                String mDBType = this.mcDBConnection.getMetaData().getDatabaseProductName();

                sql = this.getStepTemplate(mDBType, "GETCOLUMNS", true);
                sql = EngineConstants.replaceParameterV2(sql, "QUERY", aSQLStatement[0].getSQL());

                PreparedStatement mStmt = mcDBConnection.prepareStatement(sql);

                mStmt.execute();

                // Log executing sql to feed result record object with single object reference
                ResultSetMetaData rm = mStmt.getMetaData();

                int cols = rm.getColumnCount();

                Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), ETLStep.OUT_TAG, "*", "*");

                String channel = "DEFAULT";
                if (XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "OUTSYNTAX", "")
                        .equalsIgnoreCase("INFERRED")) {
                    if (nl != null) {
                        for (Node element : nl) {
                            channel = XMLHelper.getAttributeAsString(element.getAttributes(), "CHANNEL", channel);
                            this.getXMLConfig().removeChild(element);
                        }
                    }

                    for (int i = 1; i <= cols; i++) {
                        Element newOut = this.getXMLConfig().getOwnerDocument().createElement(ETLStep.OUT_TAG);

                        newOut.setAttribute("NAME", rm.getColumnName(i));
                        newOut.setAttribute("DATATYPE", this.getJavaType(rm.getColumnType(i), rm.getColumnDisplaySize(i), rm
                                .getPrecision(i), rm.getScale(i)));
                        newOut.setAttribute("CHANNEL", channel);
                        this.getXMLConfig()
                                .appendChild(this.getXMLConfig().getOwnerDocument().importNode(newOut, true));
                        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Inferring port "
                                + XMLHelper.getAttributeAsString(newOut.getAttributes(), "NAME", "N/A") + " as "
                                + newOut.getAttribute("DATATYPE"));
                    }
                }
                else {
                    if (nl.length != cols)
                        throw new KETLThreadException("Output columns does not match number of columns in source query", this);

                    for (int i = 0; i < cols; i++) {
                        String type = this.getJavaType(rm.getColumnType(i + 1), rm.getColumnDisplaySize(i + 1), rm
                                .getPrecision(i + 1), rm.getScale(i + 1));
                        String definedType = XMLHelper.getAttributeAsString(nl[i].getAttributes(), "DATATYPE", null);

                        // update type
                        if (definedType != null) {
                            int id = DataItemHelper.getDataTypeIDbyName(definedType);

                            if (id != -1)
                                definedType = DataItemHelper.getClassForDataType(id).getCanonicalName();
                        }

                        if (definedType != null && definedType.equals(type) == false)
                            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Port "
                                    + XMLHelper.getAttributeAsString(nl[i].getAttributes(), "NAME", "N/A")
                                    + " datatype doesn't correspond with the reported database type " + type);
                        if (definedType == null)
                            ((Element) nl[i]).setAttribute("DATATYPE", type);
                    }
                }

                // Close open resources
                if (mStmt != null) {
                    mStmt.close();
                }

                ResourcePool.releaseConnection(mcDBConnection);
                this.mcDBConnection = null;
            } catch (SQLException e1) {
                throw new KETLThreadException("Problem executing SQL \"" + sql + "\"- " + e1.getMessage(), this);
            }
        }

    }

    /**
     * Outs resolved.
     * 
     * @param config the config
     * 
     * @return true, if successful
     */
    private static synchronized boolean outsResolved(Element config) {
        boolean tmp = XMLHelper.getAttributeAsBoolean(config.getAttributes(), "OUTSYNTAXRESOLVED", false);
        if (tmp == false) {
            config.setAttribute("OUTSYNTAXRESOLVED", "TRUE");
            return tmp;
        }
        return tmp;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.qa.QAForJDBCReader#releaseConnnection(java.sql.Connection)
     */
    public void releaseConnnection(Connection cn) {
        throw new RuntimeException("QA Not implemented");
    }

    /**
     * Resolve parameters.
     * 
     * @return the int
     */
    int resolveParameters() {
        Node node;
        NamedNodeMap nmAttrs;

        this.mbParametersResolved = true;

        // Get the attributes
        nmAttrs = this.getXMLConfig().getAttributes();

        if (nmAttrs == null) {
            return 2;
        }

        // BRIAN: WE NEED TO BE ABLE TO POOL THESE AND KNOW IF ONE IS ALREADY OPEN!!
        // First pull SQL while we're at this node level...
        node = nmAttrs.getNamedItem(ETLStep.SQL_ATTRIB);

        if (node != null) {
            // SQL should be specified in IN parameter, but let's allow it for backwards compatibilty...
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                    "SQL attribute in XML configuration for JDBCReader is deprecated.  Use first <IN> tag instead.");
            this.mstrDefaultSQL = node.getNodeValue();
        }

        if (this.maParameters == null) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                    "No complete parameter sets found, check that the following exist:\n" + this.getRequiredTagsMessage());

            return 4;
        }

        return 0;
    }

    /**
     * Run SQL.
     * 
     * @return true, if successful
     */
    public boolean runSQL() {
        // if connection open then close it and open next
        if (this.mcDBConnection != null) {
            // close any open statements
            if (this.mStmt != null) {
                try {
                    if (this.mrsDBResultSet != null) {
                        this.mrsDBResultSet.close();
                    }

                    this.mStmt.close();
                } catch (SQLException e1) {
                    ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Statement failed to close: "
                            + e1.getMessage());
                }
            }

            // return connection to resourcepool
            ResourcePool.releaseConnection(this.mcDBConnection);
        }

        if (this.mSQLStatements.size() == 0) {
            // return null as no more parameter sets left
            return true;
        }

        // allow the user to specify the sql in the parameter list if need be
        // if found use it else use first IN
        String strSQLToExecute = null;
        int paramList = 0;

        if ((this.mSQLStatements != null) && (this.mSQLStatements.size() > 0)) {
            SQLQuery p = (SQLQuery) this.mSQLStatements.remove(0);

            strSQLToExecute = p.getSQL() + " " + this.mSQLSample;

        }

        try {
            this.mcDBConnection = this.getConnection(paramList);

            this.mStmt = null;
            this.mrsDBResultSet = null;

            // Run the query...
            this.executePreStatements();
            this.mStmt = this.mcDBConnection.createStatement();

            this.mStmt.setFetchSize(this.mFetchSize);
            this.mStmt.setFetchDirection(ResultSet.FETCH_FORWARD);

            // Log executing sql to feed result record object with single object reference
            this.mstrExecutingSQL = strSQLToExecute;
            this.setWaiting("source query to execute");
            this.mrsDBResultSet = this.getResultSet(this.mStmt,strSQLToExecute);
            this.setWaiting(null);
            this.mColMetadata = null;
            this.maxCharLength = this.mcDBConnection.getMetaData().getMaxCharLiteralLength();

            // add these to speed up fetch
            try {
                if ((this.mrsDBResultSet.getType() != ResultSet.TYPE_FORWARD_ONLY)
                        && (this.mrsDBResultSet.getFetchDirection() != ResultSet.FETCH_FORWARD)) {
                    this.mrsDBResultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
                }
            } catch (SQLException e1) {
                ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                        "Fetch direction manipulation not supported fully: " + e1.getMessage());
            }

            // If fetchsize left blank then let JDBC driver choose
            if (this.mintFetchSize != -1) {
                this.mrsDBResultSet.setFetchSize(this.mintFetchSize);
            }
        } catch (Exception e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Unable to execute query '" + strSQLToExecute
                    + "': " + e.toString());

            return false;
        }

        // Return the actual db result set, in case the caller wants to do
        // something without our conversion to the ResultRecord...
        return true;
    }

    /**
     * Gets the result set.
     * 
     * @param pStatement the statement
     * @param pSQLToExecute the SQL to execute
     * 
     * @return the result set
     * 
     * @throws SQLException the SQL exception
     */
    protected ResultSet getResultSet(Statement pStatement,String pSQLToExecute) throws SQLException {
        return pStatement.executeQuery(pSQLToExecute);
    }
}
