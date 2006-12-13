package com.kni.etl.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.w3c.dom.Element;

final public class JDBCStatementWrapper extends StatementWrapper {

    PreparedStatement stmt;
    JDBCItemHelper helper;

    public JDBCStatementWrapper(PreparedStatement stmt,JDBCItemHelper helper) {
        super();
        this.stmt = stmt;
        this.helper = helper;
    }


    @Override
    public void addBatch() throws SQLException {
        stmt.addBatch();
    }

    
    public static StatementWrapper prepareStatement(Connection mcDBConnection, String msInBatchSQLStatement,
            JDBCItemHelper helper) throws SQLException {
        return  new JDBCStatementWrapper(mcDBConnection.prepareStatement(msInBatchSQLStatement),helper);
    }

    @Override
    public void close() throws SQLException {
        stmt.close();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return stmt.executeBatch();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return stmt.executeUpdate();
    }

    @Override
    public void setParameterFromClass(int parameterIndex, Class pClass, Object pDataItem, int maxCharLength,
            Element pXMLConfig) throws SQLException {
        helper.setParameterFromClass(this.stmt, parameterIndex, pClass, pDataItem, maxCharLength, pXMLConfig);

    }

}
