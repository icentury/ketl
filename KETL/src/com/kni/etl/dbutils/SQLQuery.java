/**
 * 
 */
package com.kni.etl.dbutils;

public class SQLQuery {
    String sql;
    int parameterList = -1;
    boolean execute;
    public SQLQuery(String sql, int parameterList, boolean pExecute) {
        super();
        this.sql = sql;
        this.execute = pExecute;
        this.parameterList = parameterList;
    }
    
    public boolean executeQuery() {
        return this.execute;
    }
    
    public int getParameterListID() {
        return parameterList;
    }
    
    public String getSQL() {
        return sql;
    }    
    
    public void setSQL(String arg0){
        sql = arg0;
    }
}