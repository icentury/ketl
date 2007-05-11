package com.kni.etl;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;

/**
 * Insert the type's description here. Creation date: (5/3/2002 6:26:39 PM)
 * 
 * @author: Administrator
 */
public class SQLJob extends ETLJob implements DBConnection {

    /**
     * @throws Exception
     */
    public SQLJob() throws Exception {
        super();
    }

    static final String AUTOCOMMIT = "AUTOCOMMIT"; // defaults to false
    static final String MAXROWS = "MAXROWS";
    int mMaxRows = 10000;
    protected java.sql.ResultSet rsResultSet = null;
    protected int iUpdateCount = -1;
    public int miDatabaseMaxStatements = EngineConstants.MAX_STATEMENTS_PER_CONNECTION;
    protected boolean bAutoCommit = false;
    Node mSQLNode;
    static final String WRITEBACK_PARAMETER = "WRITEBACK_PARAMETER";

    public boolean autoCommit() {
        return this.bAutoCommit;
    }

    @Override
    protected Node setChildNodes(Node pParentNode) {
        if (this.mSQLNode == null) {
            Node e = pParentNode.getOwnerDocument().createElement("SQL");
            Element x = pParentNode.getOwnerDocument().createElement("STATEMENT");
            x.setAttribute(SQLJob.AUTOCOMMIT, "FALSE");
            x.setAttribute(SQLJob.MAXROWS, Integer.toString(this.getMaxRows()));
            e.appendChild(x);
            x.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction(false).toString()));
            pParentNode.appendChild(e);

            return e;
        }

        Node e = pParentNode.getOwnerDocument().importNode(this.mSQLNode, true);
        pParentNode.appendChild(e);

        return e;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 2:28:24 PM)
     */
    @Override
    public void cleanup() {
        // If we still have a ResultSet open, we should close it...
        if (this.rsResultSet != null) {
            // NOTE (B. Sullivan, 2002.05.09): we should probably get the Statement object and close it as well,
            // but if we ever open up the SQLJob to use PreparedStatements, then it gets messy...
            // We don't notice any leftovers on the database (ie, open cursors) in Oracle, so we're going to
            // leave this out for now.
            try {
                this.rsResultSet.getStatement().close();
                this.rsResultSet.close();
            } catch (Exception e) {
                // Just eat the exception
            }
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 12:06:44 PM)
     */
    @Override
    protected void finalize() throws Throwable {
        this.cleanup();

        // It's good practice to call the superclass's finalize() method,
        // even if you know there is not one currently defined...
        super.finalize();
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:56:56 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getDatabaseDriverClass() {
        return (String) this.getGlobalParameter(DBConnection.DRIVER_ATTRIB);
    }

    /**
     * Insert the method's description here. Creation date: (2002.06.24 2:55:30 PM)
     * 
     * @return int
     */
    public int getDatabaseMaxStatements() {
        return this.miDatabaseMaxStatements;
    }

    public int getMaxRows() {
        return this.mMaxRows;
    }

    public void setMaxRows(int pMaxRows) {
        this.mMaxRows = pMaxRows;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getDatabasePassword() {
        return (String) this.getGlobalParameter(DBConnection.PASSWORD_ATTRIB);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getPreSQL() {
        return (String) this.getGlobalParameter(DBConnection.PRESQL_ATTRIB);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:00:10 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getDatabaseURL() {
        return (String) this.getGlobalParameter(DBConnection.URL_ATTRIB);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:34 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getDatabaseUser() {
        return (String) this.getGlobalParameter(DBConnection.USER_ATTRIB);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:47:45 PM)
     * 
     * @return java.sql.ResultSet
     */
    public java.sql.ResultSet getResultSet() {
        return this.rsResultSet;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 4:46:12 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getSQL() throws Exception {
        return this.resolveParameters((String) this.getAction(true));
    }

    /**
     * @param pText
     * @return
     * @throws Exception
     */
    public String resolveParameters(String pText) throws Exception {
        String[] strParms = EngineConstants.getParametersFromText(pText);
        if (strParms != null) {
            for (String element : strParms) {
                String parmValue = (String) this.getGlobalParameter(element);

                if (parmValue != null) {
                    pText = EngineConstants.replaceParameter(pText, element, parmValue);
                }
                else {
                    throw new Exception("Parameter " + element + " can not be found in parameter list");
                }
            }
        }
        return pText;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:53:09 PM)
     * 
     * @return int
     */
    public int getUpdateCount() {
        return this.iUpdateCount;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:56:56 PM)
     * 
     * @param newDriverClass java.lang.String
     */
    public void setDatabaseDriverClass(java.lang.String newDatabaseDriverClass) {
        this.setGlobalParameter(DBConnection.DRIVER_ATTRIB, newDatabaseDriverClass);
    }

    /**
     * Insert the method's description here. Creation date: (2002.06.24 2:55:30 PM)
     * 
     * @param newMaxStatements int
     */
    public void setDatabaseMaxStatements(int newDatabaseMaxStatements) {
        this.miDatabaseMaxStatements = newDatabaseMaxStatements;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
     * 
     * @param newDatabasePassword java.lang.String
     */
    public void setDatabasePassword(java.lang.String newDatabasePassword) {
        this.setGlobalParameter(DBConnection.PASSWORD_ATTRIB, newDatabasePassword);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:00:10 PM)
     * 
     * @param newDatabaseURL java.lang.String
     */
    public void setDatabaseURL(java.lang.String newDatabaseURL) {
        this.setGlobalParameter(DBConnection.URL_ATTRIB, newDatabaseURL);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:34 PM)
     * 
     * @param newDatabaseUser java.lang.String
     */
    public void setDatabaseUser(java.lang.String newDatabaseUser) {
        this.setGlobalParameter(DBConnection.USER_ATTRIB, newDatabaseUser);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:47:45 PM)
     * 
     * @param newResult java.sql.ResultSet
     */
    public void setResultSet(java.sql.ResultSet newResult) {
        this.rsResultSet = newResult;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 4:46:12 PM)
     * 
     * @param newSQL java.lang.String
     */
    public void setSQL(String newSQL) throws Exception {
        this.setAction(newSQL);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:53:09 PM)
     * 
     * @param newUpdateCount int
     */
    public void setUpdateCount(int newUpdateCount) {
        this.iUpdateCount = newUpdateCount;
    }

    /**
     * Returns a String that represents the value of this object.
     * 
     * @return a string representation of the receiver
     */
    @Override
    public String toString() {
        return (String) this.getAction(true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLJob#setAction(java.lang.Object)
     */
    @Override
    public void setAction(Object oAction) throws Exception {
        super.setAction(oAction);

        String sAction = (String) this.getAction(false);

        if (sAction == "" || sAction.indexOf("<SQL") == -1) {
            this.mSQLNode = null;
            return;
        }

        DocumentBuilder builder = null;
        Document xmlDOM = null;
        DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

        // Build a DOM out of the XML string...
        try {

            builder = dmfFactory.newDocumentBuilder();
            xmlDOM = builder.parse(new InputSource(new StringReader((String) this.getAction(false))));

            NodeList nl = xmlDOM.getElementsByTagName("SQL");

            if ((nl == null) || (nl.getLength() == 0)) {
                this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
                this.getStatus().setErrorMessage("Error reading job XML: no SQL specified.");

                return;
            }

            if (nl.getLength() > 1) {
                this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
                this.getStatus().setErrorMessage("Error reading job XML: more than 1 SQL top node specified.");

                return;
            }

            this.mSQLNode = nl.item(0);
        } catch (org.xml.sax.SAXParseException e) {
            this.mSQLNode = null;
        } catch (Exception e) {
            this.getStatus().setErrorCode(ETLJobStatus.PENDING_CLOSURE_FAILED); // BRIAN: NEED TO SET UP KETL JOB ERROR
            // CODES
            this.getStatus().setErrorMessage("Error reading job XML: " + e.getMessage());

            return;
        }
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        return ResourcePool.getConnection(this.getDatabaseDriverClass(), this.getDatabaseURL(), this.getDatabaseUser(),
                this.getDatabasePassword(), this.getPreSQL(), true);
    }

}
