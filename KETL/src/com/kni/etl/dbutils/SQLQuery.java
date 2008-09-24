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

import com.kni.etl.EngineConstants;

// TODO: Auto-generated Javadoc
/**
 * The Class SQLQuery.
 */
public class SQLQuery {
    
    private static final String THIS_GET_PARTITION_ID = "this.getPartitionID()";

	private static final String THIS_GET_PARTITIONS = "this.getPartitions()";

	/** The sql. */
    String sql;
    
    /** The parameter list. */
    int parameterList = -1;
    
    /** The execute. */
    boolean execute;
    
    /**
     * Instantiates a new SQL query.
     * 
     * @param sql the sql
     * @param parameterList the parameter list
     * @param pExecute the execute
     */
    public SQLQuery(String sql, int parameterList, boolean pExecute) {
        super();
        this.sql = sql;
        this.execute = pExecute;
        this.parameterList = parameterList;
        sql.replace(THIS_GET_PARTITIONS,Integer.toString(1)).replace(THIS_GET_PARTITION_ID,Integer.toString(1));
    }
    
    public SQLQuery(String sql, int parameterList, boolean pExecute, int partitions,
			int partitionID) {
    	this.execute = pExecute;
        this.parameterList = parameterList;        
        this.sql = sql.replace(THIS_GET_PARTITIONS,Integer.toString(partitions)).replace(THIS_GET_PARTITION_ID,Integer.toString(partitionID));    	        
	}

	/**
     * Execute query.
     * 
     * @return true, if successful
     */
    public boolean executeQuery() {
        return this.execute;
    }
    
    /**
     * Gets the parameter list ID.
     * 
     * @return the parameter list ID
     */
    public int getParameterListID() {
        return this.parameterList;
    }
    
    /**
     * Gets the SQL.
     * @param partition 
     * @param partitions 
     * 
     * @return the SQL
     */
    public String getSQL() {
    	// special functions
    	return sql;
    }    
    
    /**
     * Sets the SQL.
     * 
     * @param arg0 the new SQL
     */
    public void setSQL(String arg0){
        this.sql = arg0;
    }

	public static boolean containPartitionCode(String sql) {
		return sql.contains(THIS_GET_PARTITIONS) && sql.contains(THIS_GET_PARTITION_ID);		
	}
}