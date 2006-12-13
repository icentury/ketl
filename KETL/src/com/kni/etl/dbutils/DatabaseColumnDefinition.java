package com.kni.etl.dbutils;

import org.w3c.dom.Node;

/**
 * @author nwakefield Creation Date: Mar 7, 2003
 */
public class DatabaseColumnDefinition extends ColumnDefinition {

    public static int PRIMARY_KEY = 1 << 0;
    public static int SRC_UNIQUE_KEY = 1 << 1;
    public static int UPDATE_TRIGGER_COLUMN = 1 << 2;
    public static int UPDATE_COLUMN = 1 << 3;
    public static int INSERT_COLUMN = 1 << 4;

    private String mAlternateInsertValue = null;
    private String mAlternateUpdateValue = null;
    private Class mSrcClass;
    public boolean exists = true;

    /**
     * @param pColumnName
     * @param pDataType
     * @param pBits
     */
    public DatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType, long pBits) {
        super(pNode, pColumnName, pDataType, pBits);

    }

    /**
     * @param pColumnName
     * @param pDataType
     */
    public DatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType) {
        super(pNode, pColumnName, pDataType);

    }

    /**
     * @return Returns the alternateInsertValue.
     */
    public String getAlternateInsertValue() {
        return mAlternateInsertValue;
    }

    /**
     * @param pAlternateInsertValue The alternateInsertValue to set.
     */
    public void setAlternateInsertValue(String pAlternateInsertValue) {
        mAlternateInsertValue = pAlternateInsertValue;
    }

    /**
     * @return Returns the alternateUpdateValue.
     */
    public String getAlternateUpdateValue() {
        return mAlternateUpdateValue;
    }

    /**
     * @param pAlternateUpdateValue The alternateUpdateValue to set.
     */
    public void setAlternateUpdateValue(String pAlternateUpdateValue) {
        mAlternateUpdateValue = pAlternateUpdateValue;
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
        StringBuilder tmp = new StringBuilder(super.toString() + "\n");

        tmp.append("PK = " + this.hasProperty(PRIMARY_KEY) + "\n");
        tmp.append("SK = " + this.hasProperty(SRC_UNIQUE_KEY) + "\n");
        tmp.append("IN = " + this.hasProperty(INSERT_COLUMN) + "\n");
        tmp.append("UP = " + this.hasProperty(UPDATE_COLUMN) + "\n");
        tmp.append("UT = " + this.hasProperty(UPDATE_TRIGGER_COLUMN));

        return tmp.toString();
    }

    public void setSourceClass(Class mSrcClass) {
        this.mSrcClass = mSrcClass;
    }

    public Class getSourceClass() {
        return mSrcClass;
    }
}
