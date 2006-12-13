package com.kni.etl.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.util.XMLHelper;

public class StatementManager {

    public static final String COMMIT = "${COMMIT}";
    public static final int END = 0;
    public static final int START = 1;
    public static final int ANYTIME = 2;

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

    public static void executeStatements(Object[] arg0, Connection connection, String pStatementSeperator,
            int pThreadGroupOrder, Object pParent, boolean pIgnoreErrors) throws SQLException {
        String sql = null;
        Statement stmt = null;

        try {
            if (pIgnoreErrors) {
                pStatementSeperator = null;
            }

            stmt = connection.createStatement();

            if (pStatementSeperator == null) {
                for (int i = 0; i < arg0.length; i++) {
                    sql = (String) arg0[i];
                    if (sql.contains(COMMIT)) {
                        sql = sql.replace(COMMIT, "");
                        try {
                            wrapExecution(pParent, stmt, sql);
                            connection.commit();
                        } catch (SQLException e) {
                            if (!pIgnoreErrors)
                                throw e;
                            connection.rollback();
                        }
                    }
                    else {
                        try {
                            wrapExecution(pParent, stmt, sql);

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
                for (int i = 0; i < arg0.length; i++) {
                    sql = (String) arg0[i];
                    if (sql.contains(COMMIT)) {
                        sql = sql.replace(COMMIT, "");
                        sb.append(sql).append(pStatementSeperator + "\n");
                        sql = sb.toString();
                        wrapExecution(pParent, stmt, sql);

                        sb = new StringBuilder();
                        connection.commit();

                    }
                    else
                        sb.append(sql).append(pStatementSeperator + "\n");

                }

                if (sb.length() > 0) {
                    sql = sb.toString();
                    try {
                        wrapExecution(pParent, stmt, sql);

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
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        }

    }

    public static int executeStatements(ETLStep step, DBConnection dbConnection, String pTag) throws SQLException {
        return executeStatements(step, dbConnection, pTag, ANYTIME);
    }

    public static int executeStatements(ETLStep step, DBConnection dbConnection, String pTag, int pThreadGroupOrder)
            throws SQLException {
        String sql = null;

        boolean canRun = true;

        if (pThreadGroupOrder == START)
            canRun = step.isFirstThreadToEnterInitializePhase();
        else if (pThreadGroupOrder == END)
            canRun = step.isLastThreadToEnterCompletePhase();

        Statement stmt = null;
        try {
            try {
                stmt = dbConnection.getConnection().createStatement();
            } catch (ClassNotFoundException e2) {
            }

            NodeList nl = step.getXMLConfig().getChildNodes();
            if (nl != null) {
                for (int i = 0; i < nl.getLength(); i++) {

                    Node node = nl.item(i);

                    if (node != null && node.getNodeName() != null && pTag != null
                            && node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().compareTo(pTag) == 0) {
                        sql = XMLHelper.getTextContent(node);

                        boolean runQueryPerPartition = XMLHelper.getAttributeAsBoolean(node.getAttributes(),
                                "PERPARTITION", false);

                        if (runQueryPerPartition || canRun) {
                            if (XMLHelper.getAttributeAsBoolean(node.getAttributes(), "CRITICAL", true)) {
                                wrapExecution(step, stmt, sql);
                            }
                            else {
                                try {
                                    wrapExecution(step, stmt, sql);

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
                    }
                }
            }
            try {
                dbConnection.getConnection().commit();
            } catch (ClassNotFoundException e) {
            }

        } finally {
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
