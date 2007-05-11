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

import com.kni.etl.ketl.exceptions.KETLThreadException;

// TODO: Auto-generated Javadoc
/**
 * The Class DatabaseColumnDefinition.
 * 
 * @author nwakefield Creation Date: Mar 7, 2003
 */
public class DatabaseColumnDefinition extends ColumnDefinition {

    /** The PRIMAR y_ KEY. */
    public static int PRIMARY_KEY = 1 << 0;
    
    /** The SR c_ UNIQU e_ KEY. */
    public static int SRC_UNIQUE_KEY = 1 << 1;
    
    /** The UPDAT e_ TRIGGE r_ COLUMN. */
    public static int UPDATE_TRIGGER_COLUMN = 1 << 2;
    
    /** The UPDAT e_ COLUMN. */
    public static int UPDATE_COLUMN = 1 << 3;
    
    /** The INSER t_ COLUMN. */
    public static int INSERT_COLUMN = 1 << 4;
    
    /** The HAS h_ COLUMN. */
    public static int HASH_COLUMN = 1 << 5;

    /** The m alternate insert value. */
    private String mAlternateInsertValue = null;
    
    /** The m alternate update value. */
    private String mAlternateUpdateValue = null;
    
    /** The m wrap value. */
    private String mWrapValue = null;
    
    /** The m src class. */
    private Class mSrcClass;
    
    /** The exists. */
    public boolean exists = true;

    /**
     * The Constructor.
     * 
     * @param pColumnName the column name
     * @param pDataType the data type
     * @param pBits the bits
     * @param pNode the node
     */
    public DatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType, long pBits) {
        super(pNode, pColumnName, pDataType, pBits);

    }

    /**
     * The Constructor.
     * 
     * @param pColumnName the column name
     * @param pDataType the data type
     * @param pNode the node
     */
    public DatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType) {
        super(pNode, pColumnName, pDataType);

    }

    /**
     * Gets the alternate insert value.
     * 
     * @return Returns the alternateInsertValue.
     */
    public String getAlternateInsertValue() {
        return this.mAlternateInsertValue;
    }

    /**
     * Sets the alternate insert value.
     * 
     * @param pAlternateInsertValue The alternateInsertValue to set.
     */
    public void setAlternateInsertValue(String pAlternateInsertValue) {
        this.mAlternateInsertValue = pAlternateInsertValue;
    }

    /**
     * Gets the alternate update value.
     * 
     * @return Returns the alternateUpdateValue.
     */
    public String getAlternateUpdateValue() {
        return this.mAlternateUpdateValue;
    }

    /**
     * Sets the alternate update value.
     * 
     * @param pAlternateUpdateValue The alternateUpdateValue to set.
     */
    public void setAlternateUpdateValue(String pAlternateUpdateValue) {
        this.mAlternateUpdateValue = pAlternateUpdateValue;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.ColumnDefinition#longToBinary(long)
     */
    @Override
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
        StringBuilder tmp = new StringBuilder(super.toString() + "\n");

        tmp.append("PK = " + this.hasProperty(DatabaseColumnDefinition.PRIMARY_KEY) + "\n");
        tmp.append("SK = " + this.hasProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY) + "\n");
        tmp.append("IN = " + this.hasProperty(DatabaseColumnDefinition.INSERT_COLUMN) + "\n");
        tmp.append("UP = " + this.hasProperty(DatabaseColumnDefinition.UPDATE_COLUMN) + "\n");
        tmp.append("HC = " + this.hasProperty(DatabaseColumnDefinition.HASH_COLUMN) + "\n");
        tmp.append("UT = " + this.hasProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN));

        return tmp.toString();
    }

    /**
     * Sets the source class.
     * 
     * @param mSrcClass the new source class
     */
    public void setSourceClass(Class mSrcClass) {
        this.mSrcClass = mSrcClass;
    }

    /**
     * Gets the source class.
     * 
     * @return the source class
     */
    public Class getSourceClass() {
        return this.mSrcClass;
    }

    /**
     * Gets the parameter definition.
     * 
     * @return the parameter definition
     */
    public String getParameterDefinition(){
        if(this.mWrapValue == null) return "?";
        return this.mWrapValue;
    }
    
    /**
     * Sets the value wrapper.
     * 
     * @param arg0 the new value wrapper
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public void setValueWrapper(String arg0) throws KETLThreadException {
        this.mWrapValue = arg0;   
        
        if(this.mWrapValue == null) return;
        
        if(this.mWrapValue.replaceAll("[^?]","").length() != 1)
            throw new KETLThreadException("Value wrapper must contain 1 parameter reference, e.g. trim(?) not " + this.mWrapValue,Thread.currentThread());
    }
}
