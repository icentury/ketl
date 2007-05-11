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

import org.w3c.dom.Node;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company:
 * </p>.
 * 
 * @author unascribed
 * @version 1.0
 */
public class ColumnDefinition {

    /** The SQL data type. */
    public int iSQLDataType;
    
    /** The str column name. */
    private String strColumnName;
    
    /** The precision. */
    public int iSize, iPrecision = -1;
    
    /** The s type name. */
    public String sTypeName;
    
    /** The column properties. */
    protected long mColumnProperties = 0;
    
    /** The config node. */
    Node mConfigNode;

    /**
     * Instantiates a new column definition.
     * 
     * @param pNode the node
     * @param pColumnName the column name
     * @param pDataType the data type
     */
    public ColumnDefinition(Node pNode, String pColumnName, int pDataType) {
        this.iSQLDataType = pDataType;
        this.setColumnName(pColumnName);
        this.mConfigNode = pNode;
    }

    /**
     * Gets the type desc.
     * 
     * @return the type desc
     */
    public String getTypeDesc() {
        if (this.iSize == 0)
            return this.sTypeName;

        if (this.iPrecision > 0)
            return this.sTypeName + "(" + this.iSize + "," + this.iPrecision + ")";
        return this.sTypeName + "(" + this.iSize + ")";
    }

    /**
     * Instantiates a new column definition.
     * 
     * @param pNode the node
     * @param pColumnName the column name
     * @param pDataType the data type
     * @param pBits the bits
     */
    public ColumnDefinition(Node pNode, String pColumnName, int pDataType, long pBits) {
        this(pNode, pColumnName, pDataType);
        this.mColumnProperties = pBits;
    }

    /**
     * Gets the XML config.
     * 
     * @return the XML config
     */
    public Node getXMLConfig() {
        return this.mConfigNode;
    }

    /**
     * Checks for property.
     * 
     * @param pProperty the property
     * 
     * @return true, if successful
     */
    public boolean hasProperty(long pProperty) {
        return (this.mColumnProperties & pProperty) != 0;
    }

    /**
     * Sets the property.
     * 
     * @param pProperty the new property
     */
    public void setProperty(long pProperty) {
        this.mColumnProperties |= pProperty;
    }

    /**
     * Un set property.
     * 
     * @param pProperty the property
     */
    public void unSetProperty(long pProperty) {
        this.mColumnProperties ^= pProperty;
    }

    /**
     * Sets the property.
     * 
     * @param pProperty the property
     * @param pValue the value
     */
    public void setProperty(long pProperty, boolean pValue) {
        if (pValue) {
            this.setProperty(pProperty);
        }
        else {
            this.unSetProperty(pProperty);
        }
    }

    /**
     * Long to binary.
     * 
     * @param i the i
     * 
     * @return the string
     */
    public String longToBinary(long i) {
        String retval = "";
        int mask = 0x80000000;
        int spacemark = 0x08888888;

        while (mask != 0) {
            retval += ((i & mask) != 0) ? "1" : "0";
            mask >>>= 1;
            if ((mask & spacemark) != 0)
                retval += " ";
        }

        return retval;

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return this.longToBinary(this.mColumnProperties) + " - " + this.getColumnName(null, -1);
    }

    /**
     * Sets the column name.
     * 
     * @param strColumnName the new column name
     */
    public void setColumnName(String strColumnName) {
        this.strColumnName = strColumnName;
    }
    
    
    /** The Constant LOWER_CASE. */
    public static final int LOWER_CASE = 0;
    
    /** The Constant MIXED_CASE. */
    public static final int MIXED_CASE = 2;
    
    /** The Constant UPPER_CASE. */
    public static final int UPPER_CASE = 1;
   
    
    /**
     * Sets the DB case.
     * 
     * @param pCase the case
     * @param pStr the str
     * 
     * @return the string
     */
    private static String setDBCase(int pCase, String pStr) {

        if (pStr == null)
            return null;

        switch (pCase) {
        case LOWER_CASE:
            return pStr.toLowerCase();

        case MIXED_CASE:
            return pStr;

        case UPPER_CASE:
            return pStr.toUpperCase();
        }

        return pStr;
    }


    /**
     * Gets the column name.
     * 
     * @param idQuote the id quote
     * @param pDBCase the DB case
     * 
     * @return the column name
     */
    public String getColumnName(String idQuote, int pDBCase) {
        if (idQuote != null)
            return idQuote + ColumnDefinition.setDBCase(pDBCase,this.strColumnName) + idQuote;

        return ColumnDefinition.setDBCase(pDBCase,this.strColumnName);
    }
}
