package com.kni.etl.dbutils;

import java.sql.SQLException;

import org.w3c.dom.Element;

abstract public class StatementWrapper {

    abstract public void close() throws SQLException;

    
    abstract public int executeUpdate() throws SQLException;

    abstract public void addBatch() throws SQLException;

    abstract public int[] executeBatch() throws  SQLException;

    abstract public void setParameterFromClass(int parameterIndex, Class pClass, Object pDataItem, int maxCharLength,
            Element pXMLConfig) throws SQLException;

}
