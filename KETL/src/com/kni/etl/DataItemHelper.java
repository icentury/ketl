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
package com.kni.etl;

import java.util.Date;

// TODO: Auto-generated Javadoc
/**
 * The Class DataItemHelper.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class DataItemHelper {

    /** The Constant BOOLEAN. */
    public static final int BOOLEAN = 9;
    
    /** The Constant BYTEARRAY. */
    public static final int BYTEARRAY = 8;
    
    /** The Constant CHAR. */
    public static final int CHAR = 5;
    
    /** The Constant CHARARRAY. */
    public static final int CHARARRAY = 6;
    
    /** The Constant DATE. */
    public static final int DATE = 3;
    
    /** The Constant DOUBLE. */
    public static final int DOUBLE = 7;
    
    /** The Constant FLOAT. */
    public static final int FLOAT = 4;
    
    /** The Constant INTEGER. */
    public static final int INTEGER = 2;
    
    /** The Constant STRING. */
    public static final int STRING = 1;
    
    /** The Constant LONG. */
    public static final int LONG = 10;

    /**
     * Gets the data type I dby name.
     * 
     * @param pDataTypeName the data type name
     * 
     * @return the data type I dby name
     */
    public static final int getDataTypeIDbyName(String pDataTypeName) {
        if (pDataTypeName == null) {
            return DataItemHelper.STRING;
        }

        if (pDataTypeName.equals("FLOAT")) {
            return DataItemHelper.FLOAT;
        }

        if (pDataTypeName.equals("STRING")) {
            return DataItemHelper.STRING;
        }

        if (pDataTypeName.equals("LONG")) {
            return DataItemHelper.LONG;
        }

        if (pDataTypeName.equals("INTEGER")) {
            return DataItemHelper.INTEGER;
        }

        if (pDataTypeName.equals("DATE")) {
            return DataItemHelper.DATE;
        }

        if (pDataTypeName.equals("DOUBLE")) {
            return DataItemHelper.DOUBLE;
        }

        if (pDataTypeName.equals("CHAR")) {
            return DataItemHelper.CHAR;
        }

        if (pDataTypeName.equals("CHARARRAY")) {
            return DataItemHelper.CHARARRAY;
        }

        if (pDataTypeName.equals("BOOLEAN")) {
            return DataItemHelper.BOOLEAN;
        }

        if (pDataTypeName.equals("BYTEARRAY")) {
            return DataItemHelper.BYTEARRAY;
        }
		if (pDataTypeName.equals(byte[].class.getCanonicalName())) {
			return DataItemHelper.BYTEARRAY;
		}

        return -1;
    }

    /**
     * Gets the class for data type.
     * 
     * @param type the type
     * 
     * @return the class for data type
     */
    public final static Class getClassForDataType(int type) {

        switch (type) {
        case FLOAT:
            return Float.class;

        case STRING:
            return String.class;

        case LONG:
            return Long.class;

        case INTEGER:
            return Integer.class;

        case DATE:
            return Date.class;

        case DOUBLE:
            return Double.class;

        case CHAR:
            return Character.class;

        case CHARARRAY:
            return Character[].class;

        case BOOLEAN:
            return Boolean.class;

        case BYTEARRAY:
            return Byte[].class;

        default:
            return null;
        }

    }
}
