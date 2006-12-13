package com.kni.etl.dbutils;

import java.sql.SQLException;


public interface PrePostSQL {
  
    abstract void executePreStatements() throws SQLException;
    abstract void executePostStatements() throws SQLException;
    abstract void executePreBatchStatements() throws SQLException;
    abstract void executePostBatchStatements() throws SQLException;
    
}
