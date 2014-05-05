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
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.w3c.dom.Element;

// TODO: Auto-generated Javadoc
/**
 * The Class JDBCStatementWrapper.
 */
final public class JDBCStatementWrapper extends StatementWrapper {

    /** The stmt. */
    PreparedStatement stmt;
    
    /** The helper. */
    JDBCItemHelper helper;

    /**
     * Instantiates a new JDBC statement wrapper.
     * 
     * @param stmt the stmt
     * @param helper the helper
     */
    public JDBCStatementWrapper(PreparedStatement stmt,JDBCItemHelper helper) {
        super();
        this.stmt = stmt;
        this.helper = helper;
    }


    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.StatementWrapper#addBatch()
     */
    @Override
    public void addBatch() throws SQLException {
        this.stmt.addBatch();
    }

    
    /**
     * Prepare statement.
     * 
     * @param mcDBConnection the mc DB connection
     * @param msInBatchSQLStatement the ms in batch SQL statement
     * @param helper the helper
     * 
     * @return the statement wrapper
     * 
     * @throws SQLException the SQL exception
     */
    public static StatementWrapper prepareStatement(Connection mcDBConnection, String msInBatchSQLStatement,
            JDBCItemHelper helper) throws SQLException {
        return  new JDBCStatementWrapper(mcDBConnection.prepareStatement(msInBatchSQLStatement),helper);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.StatementWrapper#close()
     */
    @Override
    public void close() throws SQLException {
        this.stmt.close();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.StatementWrapper#executeBatch()
     */
    @Override
    public int[] executeBatch() throws SQLException {
        return this.stmt.executeBatch();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.StatementWrapper#executeUpdate()
     */
    @Override
    public int executeUpdate() throws SQLException {
        return this.stmt.executeUpdate();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.StatementWrapper#setParameterFromClass(int, java.lang.Class, java.lang.Object, int, org.w3c.dom.Element)
     */
    @Override
    public void setParameterFromClass(int parameterIndex, Class pClass, Object pDataItem, int maxCharLength,
            Element pXMLConfig) throws SQLException {
        this.helper.setParameterFromClass(this.stmt, parameterIndex, pClass, pDataItem, maxCharLength, pXMLConfig);

    }

}
