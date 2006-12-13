/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.dbutils;

import org.w3c.dom.Node;

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
 * </p>
 * 
 * @author unascribed
 * @version 1.0
 */
public class ColumnDefinition {

    public int iSQLDataType;
    private String strColumnName;
    public int iSize, iPrecision = -1;
    public String sTypeName;
    protected long mColumnProperties = 0;
    Node mConfigNode;

    public ColumnDefinition(Node pNode, String pColumnName, int pDataType) {
        this.iSQLDataType = pDataType;
        this.setColumnName(pColumnName);
        this.mConfigNode = pNode;
    }

    public String getTypeDesc() {
        if (iSize == 0)
            return sTypeName;

        if (this.iPrecision > 0)
            return sTypeName + "(" + iSize + "," + this.iPrecision + ")";
        return sTypeName + "(" + iSize + ")";
    }

    public ColumnDefinition(Node pNode, String pColumnName, int pDataType, long pBits) {
        this(pNode, pColumnName, pDataType);
        this.mColumnProperties = pBits;
    }

    public Node getXMLConfig() {
        return this.mConfigNode;
    }

    public boolean hasProperty(long pProperty) {
        return (this.mColumnProperties & pProperty) != 0;
    }

    public void setProperty(long pProperty) {
        this.mColumnProperties |= pProperty;
    }

    public void unSetProperty(long pProperty) {
        this.mColumnProperties ^= pProperty;
    }

    public void setProperty(long pProperty, boolean pValue) {
        if (pValue) {
            this.setProperty(pProperty);
        }
        else {
            this.unSetProperty(pProperty);
        }
    }

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

    public void setColumnName(String strColumnName) {
        this.strColumnName = strColumnName;
    }
    
    
    public static final int LOWER_CASE = 0;
    public static final int MIXED_CASE = 2;
    public static final int UPPER_CASE = 1;
   
    
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


    public String getColumnName(String idQuote, int pDBCase) {
        if (idQuote != null)
            return idQuote + setDBCase(pDBCase,strColumnName) + idQuote;

        return setDBCase(pDBCase,strColumnName);
    }
}
