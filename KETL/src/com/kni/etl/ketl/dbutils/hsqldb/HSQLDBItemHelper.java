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
package com.kni.etl.ketl.dbutils.hsqldb;

import java.sql.SQLException;

import com.kni.etl.dbutils.JDBCItemHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class HSQLDBItemHelper.
 */
public class HSQLDBItemHelper extends JDBCItemHelper {

    /**
     * Gets the type.
     * 
     * @param cl the cl
     * @param isKey the is key
     * 
     * @return the type
     * 
     * @throws SQLException the SQL exception
     */
    static public String getType(Class cl, boolean isKey) throws SQLException {
        if (cl == Integer.class)
            return "INTEGER";
        if (cl == Double.class)
            return "DOUBLE";
        if (cl == String.class)
            return "VARCHAR";
        if (cl == java.sql.Date.class)
            return "DATE";
        if (cl == java.util.Date.class || cl == java.sql.Timestamp.class)
            return "TIMESTAMP";
        if (cl == java.sql.Time.class)
            return "TIME";
        if (cl == Float.class)
            return "FLOAT";
        if (cl == java.math.BigDecimal.class)
            return "DECIMAL";
        if (cl == Boolean.class)
            return "BOOLEAN";
        if (cl == Byte.class)
            return "TINYINT";
        if (cl == Short.class)
            return "SMALLINT";
        if (cl == Long.class)
            return "BIGINT";
        if (cl == Byte[].class)
            return "BINARY";

        if (isKey)
            throw new SQLException("Type is part of key and cannot be an OBJECT, check type class "
                    + cl.getCanonicalName());
        return "OBJECT";
    }
}