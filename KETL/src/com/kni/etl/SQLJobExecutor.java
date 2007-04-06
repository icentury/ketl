/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.util.XMLHelper;

public class SQLJobExecutor extends ETLJobExecutor {

    private SQLJobMonitor monitor;

    private class SQLJobMonitor extends Thread {

        boolean alive = true;        
        SQLJob currentJob = null;
        public Statement stmt = null;;

        @Override
        public void run() {
            try {
                while (alive) {

                    if (stmt != null && currentJob != null && currentJob.isCancelled()) {
                        try {
                            stmt.cancel();
                            currentJob.cancelSuccessfull(true);
                        } catch (SQLException e) {
                            ResourcePool.LogException(e, this);
                        }

                    }
                    Thread.sleep(500);

                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    /**
     * SQLJobExecutor constructor comment.
     */
    public SQLJobExecutor() {
        super();       
    }

    Connection dbConnection = null;

    private long wrapExecution(Statement stmt, String curSQL, boolean debug) throws SQLException {
        long start = (System.currentTimeMillis() - 1);
        if (debug)
            ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Executing statement: " + curSQL);

        this.monitor.stmt = stmt;
        try {
            stmt.execute(curSQL);
        } finally {
            this.monitor.stmt = null;
        }
        if (debug)
            ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Execution time(Seconds): "
                    + (((double) (System.currentTimeMillis() - start)) / (double) 1000));

        return System.currentTimeMillis() - start;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:37:52 PM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    protected boolean executeJob(ETLJob ejJob) {
        this.monitor = new SQLJobMonitor();
        this.monitor.start();
        SQLJob sjJob;
        ETLStatus jsJobStatus;
        String curSQL = null;

        // Only accept SQL jobs...
        if ((ejJob instanceof SQLJob) == false) {
            return false;
        }

        sjJob = (SQLJob) ejJob;
        this.monitor.currentJob = sjJob;
        jsJobStatus = sjJob.getStatus();

        // Get a connection to our database...
        try {
            dbConnection = ResourcePool.getConnection(sjJob.getDatabaseDriverClass(), sjJob.getDatabaseURL(), sjJob
                    .getDatabaseUser(), sjJob.getDatabasePassword(), sjJob.getPreSQL(), true);
        } catch (Exception e) {
            jsJobStatus.setErrorCode(1); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES
            jsJobStatus.setErrorMessage("Error connecting to database: " + e.getMessage());
        }

        // If we had an error connecting, return failure...
        if (dbConnection == null) {
            return false;
        }

        int statement = -1;

        // Run the SQL job
        try {
            long start = (System.currentTimeMillis() - 1);
            
            Statement stmt = dbConnection.createStatement();

            boolean defaultDebug = sjJob.mSQLNode == null ? false : XMLHelper.getAttributeAsBoolean(sjJob.mSQLNode
                    .getAttributes(), "DEBUG", false);

            if (sjJob.mSQLNode == null) {
                // a little safety measure, defaults to 10000 or specified value
                this.getStatus().setExtendedMessage("Running statement 1 of 1");

                stmt.setMaxRows(sjJob.getMaxRows());
                curSQL = sjJob.getSQL();
                long executionTime = wrapExecution(stmt, curSQL, false);
                curSQL = null;
                sjJob.setResultSet(stmt.getResultSet());
                sjJob.setUpdateCount(stmt.getUpdateCount());
                sjJob.getStatus().setStats(statement,stmt.getUpdateCount(),executionTime);
            }
            else {
                NodeList nl = sjJob.mSQLNode.getChildNodes();

                if ((nl == null) || (nl.getLength() == 0)) {
                    jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES
                    jsJobStatus.setErrorMessage("Error running SQL: no sql statements specified in XML");
                }
                else {
                    ArrayList nodes = new ArrayList();
                    for (statement = 0; statement < nl.getLength(); statement++) {
                        Node n = nl.item(statement);

                        if (n.getNodeType() == Node.ELEMENT_NODE) {
                            nodes.add(n);
                        }
                    }

                    statement = 0;
                    for (Object o : nodes) {
                        Node n = (Node) o;
                        statement++;
                        this.getStatus().setExtendedMessage(
                                "Running statement " + (nodes.indexOf(o) + 1) + " of " + nodes.size());

                        boolean autoCommit = XMLHelper
                                .getAttributeAsBoolean(n.getAttributes(), SQLJob.AUTOCOMMIT, true);
                        if (autoCommit != dbConnection.getAutoCommit()) {
                            dbConnection.setAutoCommit(autoCommit);
                        }

                        sjJob.setMaxRows(XMLHelper.getAttributeAsInt(n.getAttributes(), SQLJob.MAXROWS, sjJob
                                .getMaxRows()));

                        stmt.setMaxRows(sjJob.getMaxRows());

                        String paramName = XMLHelper.getAttributeAsString(n.getAttributes(),
                                SQLJob.WRITEBACK_PARAMETER, null);
                        boolean critical = XMLHelper.getAttributeAsBoolean(n.getAttributes(), "CRITICAL", true);
                        boolean debug = XMLHelper.getAttributeAsBoolean(n.getAttributes(), "DEBUG", defaultDebug);

                        curSQL = sjJob.resolveParameters(XMLHelper.getTextContent(n));
                        try {
                            long executionTime = wrapExecution(stmt, curSQL, debug);

                            curSQL = null;
                            if (paramName != null) {

                                String paramListName = XMLHelper.getAttributeAsString(n.getAttributes(),
                                        EngineConstants.PARAMETER_LIST, null);

                                if (paramListName == null)
                                    throw new Exception(
                                            "For parameter list name must be specified for writeback,add tag "
                                                    + EngineConstants.PARAMETER_LIST);

                                ResultSet rs = stmt.getResultSet();
                                int cols = rs.getMetaData().getColumnCount();

                                if (cols != 1) {
                                    throw new Exception(
                                            "For parameter writeback only one column is expected, the following SQL returns more than one column -"
                                                    + n.getFirstChild().getNodeValue());
                                }

                                while (rs.next()) {
                                    String paramValue = rs.getString(1);

                                    if (paramValue == null) {
                                        ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                                                "Parameter write back resulted in setting parameter " + paramName
                                                        + " to NULL");
                                    }

                                    Metadata md = ResourcePool.getMetadata();

                                    if (md == null) {
                                        throw new Exception(
                                                "Parameter writeback failed as metadata could not be connected to");
                                    }

                                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                                    DocumentBuilder builder = factory.newDocumentBuilder();

                                    Document document = builder.newDocument(); // Create from whole cloth
                                    Element paramList = document.createElement(EngineConstants.PARAMETER_LIST);
                                    Element parameter = document.createElement(EngineConstants.PARAMETER);
                                    document.appendChild(paramList);
                                    paramList.appendChild(parameter);
                                    parameter.setAttribute(ETLStep.NAME_ATTRIB, paramName);
                                    parameter.setTextContent(paramValue);
                                    paramList.setAttribute(ETLStep.NAME_ATTRIB, paramListName);
                                    md.importParameterList(paramList);

                                    break;
                                }

                                rs.close();

                            }
                            else if (nl.getLength() == 1) {
                                sjJob.setResultSet(stmt.getResultSet());
                            }

                            sjJob.setUpdateCount(sjJob.getUpdateCount() + stmt.getUpdateCount());
                            sjJob.getStatus().setStats(statement,stmt.getUpdateCount(),executionTime);
                        } catch (SQLException e) {
                            if (critical)
                                throw e;
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "IGNORE:"
                                    + e.getMessage());
                            try {
                                stmt.getConnection().commit();
                            } catch (Exception e1) {
                            }

                        }
                    }

                }

                this.getStatus().setExtendedMessage("Effected rows " + sjJob.getUpdateCount());
            }

            ResourcePool.releaseConnection(dbConnection);
            
            sjJob.getStatus().setStats(sjJob.getUpdateCount(),System.currentTimeMillis() - start);
            dbConnection = null;
        } catch (Exception e) {
            jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES

            String msg;
            if (statement == -1) {
                msg = "Error running SQL: ";
            }
            else {
                msg = "Error running SQL Statement: " + statement;
            }

            if (curSQL != null)
                msg += "\n" + curSQL;

            while (e != null) {
                msg += "\t" + e.getMessage() + "\n";
                if (e instanceof SQLException) {
                    if (e != ((SQLException) e).getNextException())
                        e = ((SQLException) e).getNextException();
                }
                else
                    e = null;
            }

            jsJobStatus.setErrorMessage(msg);
            if (dbConnection != null) {
                try {
                    if (dbConnection.getAutoCommit() == false) {
                        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                                "Rolling back transaction due to error");
                        dbConnection.rollback();
                    }
                } catch (SQLException e1) {
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Rollback possibly failed, "
                            + e1.getMessage());
                }
                ResourcePool.releaseConnection(dbConnection);
                dbConnection = null;
            }

            return false;
        } finally {
            this.monitor.currentJob = null;
            this.monitor.stmt = null;
            this.monitor.alive = false;
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:55:23 AM)
     */
    protected boolean initialize() {
        // No need to do anything here.
        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 2:52:39 PM)
     * 
     * @return boolean
     * @param jJob com.kni.etl.ETLJob
     */
    public boolean supportsJobType(ETLJob jJob) {
        // Only accept SQL jobs...
        return (jJob instanceof SQLJob);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:56:15 AM)
     */
    protected boolean terminate() {
        // release our database connections...
        if (dbConnection != null) {
            return ResourcePool.releaseConnection(this.dbConnection);
        }

        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLJobExecutor#getNewJob()
     */
    @Override
    protected ETLJob getNewJob() throws Exception {
        return new SQLJob();
    }

    public static void main(String[] args) {
        ETLJobExecutor.execute(args, new SQLJobExecutor(), true);
    }
}
