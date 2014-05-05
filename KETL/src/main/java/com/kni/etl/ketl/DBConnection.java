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
package com.kni.etl.ketl;

import java.sql.Connection;
import java.sql.SQLException;

// TODO: Auto-generated Javadoc
/**
 * The Interface DBConnection.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public interface DBConnection {

    /** The Constant DRIVER_ATTRIB. */
    public static final String DRIVER_ATTRIB = "DRIVER";
    
    /** The Constant URL_ATTRIB. */
    public static final String URL_ATTRIB = "URL";
    
    /** The Constant PASSWORD_ATTRIB. */
    public static final String PASSWORD_ATTRIB = "PASSWORD";
    
    /** The Constant USER_ATTRIB. */
    public static final String USER_ATTRIB = "USER";
    
    /** The Constant PRESQL_ATTRIB. */
    public static final String PRESQL_ATTRIB = "PRESQL";
    
    /** The Constant PRESQL. */
    public static final String PRESQL = "PRESQL";
    
    /** The Constant POSTSQL. */
    public static final String POSTSQL = "POSTSQL";
    
    /** The Constant TABLE_ATTRIB. */
    public static final String TABLE_ATTRIB = "TABLE";
    
    /** The Constant SCHEMA_ATTRIB. */
    public static final String SCHEMA_ATTRIB = "SCHEMA";
    
    /** The Constant PK_ATTRIB. */
    public static final String PK_ATTRIB = "PK";
    
    /** The Constant SK_ATTRIB. */
    public static final String SK_ATTRIB = "SK";
    
    /** The Constant INSERT_ATTRIB. */
    public static final String INSERT_ATTRIB = "INSERT";
    
    /** The Constant COMMITSIZE_ATTRIB. */
    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    
    /** The Constant UPDATE_ATTRIB. */
    public static final String UPDATE_ATTRIB = "UPDATE";

    /**
     * Gets the connection.
     * 
     * @return the connection
     * 
     * @throws SQLException the SQL exception
     * @throws ClassNotFoundException the class not found exception
     * @throws  
     */
    public Connection getConnection() throws SQLException, ClassNotFoundException;
}
