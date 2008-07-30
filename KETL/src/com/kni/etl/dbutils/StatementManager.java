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
package com.kni.etl.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class StatementManager.
 */
public class StatementManager {

    /** The Constant COMMIT. */
    public static final String COMMIT = "${COMMIT}";
    
    /** The Constant END. */
    public static final int END = 0;
    
    /** The Constant START. */
    public static final int START = 1;
    
    /** The Constant ANYTIME. */
    public static final int ANYTIME = 2;

    /**
     * Wrap execution.
     * 
     * @param parent the parent
     * @param stmt the stmt
     * @param curSQL the cur SQL
     * 
     * @throws SQLException the SQL exception
     */
    private static void wrapExecution(Object parent, Statement stmt, String curSQL) throws SQLException {
        long start = (System.currentTimeMillis() - 1);
        boolean debug = false;
        if (parent instanceof ETLStep) {
            ETLStep core = (ETLStep) parent;
            debug = core.debug();
        }
        if (debug)
            ResourcePool.LogMessage(parent, ResourcePool.DEBUG_MESSAGE, "Executing statement: " + curSQL);

        stmt.executeUpdate(curSQL);

        if (debug)
            ResourcePool.LogMessage(parent, ResourcePool.DEBUG_MESSAGE, "Execution time(Seconds): "
                    + (((double) (System.currentTimeMillis() - start)) / (double) 1000));
    }

    /**
     * Execute statements.
     * 
     * @param arg0 the arg0
     * @param connection the connection
     * @param pStatementSeperator the statement seperator
     * @param pThreadGroupOrder the thread group order
     * @param pParent the parent
     * @param pIgnoreErrors the ignore errors
     * 
     * @throws SQLException the SQL exception
     */
    public static void executeStatements(Object[] arg0, Connection connection, String pStatementSeperator,
            int pThreadGroupOrder, Object pParent, boolean pIgnoreErrors) throws SQLException {
        String sql = null;
        Statement stmt = null;

        boolean autoCommit = connection.getAutoCommit();
        
        try {
            if (pIgnoreErrors) {
                pStatementSeperator = null;
            }

            stmt = connection.createStatement();

            if (pStatementSeperator == null) {
                for (Object element : arg0) {
                    sql = (String) element;
                    if (sql.contains(StatementManager.COMMIT)) {
                        sql = sql.replace(StatementManager.COMMIT, "");
                        try {
                            StatementManager.wrapExecution(pParent, stmt, sql);
                            connection.commit();
                        } catch (SQLException e) {
                            if (!pIgnoreErrors)
                                throw e;
                            connection.rollback();
                        }
                    }
                    else {
                        try {
                            StatementManager.wrapExecution(pParent, stmt, sql);

                        } catch (SQLException e) {
                            if (!pIgnoreErrors)
                                throw e;
                            connection.rollback();
                        }
                    }
                }
            }
            else {
                StringBuilder sb = new StringBuilder();
                for (Object element : arg0) {
                    sql = (String) element;
                    if (sql.contains(StatementManager.COMMIT)) {
                        sql = sql.replace(StatementManager.COMMIT, "");
                        sb.append(sql).append(pStatementSeperator + "\n");
                        sql = sb.toString();
                        StatementManager.wrapExecution(pParent, stmt, sql);

                        sb = new StringBuilder();
                        connection.commit();

                    }
                    else
                        sb.append(sql).append(pStatementSeperator + "\n");

                }

                if (sb.length() > 0) {
                    sql = sb.toString();
                    try {
                        StatementManager.wrapExecution(pParent, stmt, sql);

                    } catch (SQLException e) {
                        if (!pIgnoreErrors)
                            throw e;
                        connection.rollback();
                    }
                }

            }

            connection.commit();
        } catch (SQLException e1) {
            SQLException e = new SQLException("Following SQL failed with message \"" + e1.getMessage() + "\": " + sql);
            e.setNextException(e1);
            throw e;
        } finally {
            
            try {
                connection.setAutoCommit(autoCommit);
            } catch(Exception e){
                ResourcePool.LogException(e, pParent);
            }
            
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        }

    }

    /**
     * Execute statements.
     * 
     * @param step the step
     * @param dbConnection the db connection
     * @param pTag the tag
     * 
     * @return the int
     * 
     * @throws SQLException the SQL exception
     */
    public static int executeStatements(ETLStep step, DBConnection dbConnection, String pTag) throws SQLException {
        return StatementManager.executeStatements(step, dbConnection, pTag, StatementManager.ANYTIME);
    }

    /**
     * Execute statements.
     * 
     * @param step the step
     * @param dbConnection the db connection
     * @param pTag the tag
     * @param pThreadGroupOrder the thread group order
     * 
     * @return the int
     * 
     * @throws SQLException the SQL exception
     */
    public static int executeStatements(ETLStep step, DBConnection dbConnection, String pTag, int pThreadGroupOrder)
            throws SQLException {
        String sql = null;
        boolean autoCommit = false;
        boolean canRun = true;
        if (pThreadGroupOrder == StatementManager.START)
            canRun = step.isFirstThreadToEnterInitializePhase();
        else if (pThreadGroupOrder == StatementManager.END)
            canRun = step.isLastThreadToEnterCompletePhase();

        Statement stmt = null;
        try {
            try {
                autoCommit = dbConnection.getConnection().getAutoCommit();
                stmt = dbConnection.getConnection().createStatement();
            } catch (ClassNotFoundException e2) {
                ResourcePool.LogException(e2, step);
            }

            NodeList nl = step.getXMLConfig().getChildNodes();
            if (nl != null) {
                for (int i = 0; i < nl.getLength(); i++) {

                    Node node;
                    try {
                        node = nl.item(i);
                    } catch (Exception e) {
                        ResourcePool
                                .LogMessage(step, ResourcePool.WARNING_MESSAGE,
                                        "Nodelist gave a null pointer exception skipping node, check for tag's in step "
                                                + pTag);
                        node = null;
                    }
                    if (node != null && node.getNodeName() != null && pTag != null
                            && node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().compareTo(pTag) == 0) {
                        sql = XMLHelper.getTextContent(node);
                        
                        if(sql == null || sql.length()==0){
                            ResourcePool
                            .LogMessage(step, ResourcePool.WARNING_MESSAGE,
                                    pTag + " cannot be empty, possible XML parsing exception");
                        }

                        boolean runQueryPerPartition = XMLHelper.getAttributeAsBoolean(node.getAttributes(),
                                "PERPARTITION", false);
                        boolean autocommit = XMLHelper.getAttributeAsBoolean(node.getAttributes(), "AUTOCOMMIT", false);

                        boolean currentlyAutocommit = stmt.getConnection().getAutoCommit();

                        if (autocommit != currentlyAutocommit) {
                            if (currentlyAutocommit == false)
                                stmt.getConnection().commit();

                            stmt.getConnection().setAutoCommit(autocommit);
                        }

                        if (runQueryPerPartition || canRun) {
                            if (XMLHelper.getAttributeAsBoolean(node.getAttributes(), "CRITICAL", true)) {
                                StatementManager.wrapExecution(step, stmt, sql);
                            }
                            else {
                                try {
                                    StatementManager.wrapExecution(step, stmt, sql);

                                } catch (SQLException e) {
                                    ResourcePool
                                            .LogMessage(step, ResourcePool.INFO_MESSAGE, "IGNORE:" + e.getMessage());
                                    try {
                                        dbConnection.getConnection().commit();
                                    } catch (ClassNotFoundException e1) {
                                    }
                                }
                            }
                        }

                        if (autocommit != currentlyAutocommit) {
                            stmt.getConnection().setAutoCommit(currentlyAutocommit);
                        }
                    }
                }
            }
            try {
                dbConnection.getConnection().commit();
            } catch (ClassNotFoundException e) {
                ResourcePool.LogException(e, step);
            }

        } finally {

            try {
                dbConnection.getConnection().setAutoCommit(autoCommit);
            } catch (Exception e) {
                ResourcePool.LogException(e, step);
            }
            if (stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    ResourcePool.LogMessage(step, ResourcePool.ERROR_MESSAGE, "Error closing statement '"
                            + step.getName() + "' reached - " + e.getMessage());
                    return -1;
                }

        }

        return 0;
    }

}
