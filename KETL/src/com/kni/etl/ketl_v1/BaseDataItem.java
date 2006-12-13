/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl_v1;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.kni.etl.stringtools.FastSimpleDateFormat;

// TODO: Shouldn't we be subclassing DataItem with a DatabaseDataItem class?

/**
 * Insert the type's description here. Creation date: (3/29/2002 4:11:30 PM)
 * 
 * @author: Administrator
 */
public abstract class BaseDataItem implements Cloneable, Serializable {

    public static final int BOOLEAN = 9;
    public static final int BYTEARRAY = 8;
    public static final int CHAR = 5;
    public static final int CHARARRAY = 6;
    public static final int DATE = 3;
    public static final int DOUBLE = 7;
    public static final int FLOAT = 4;
    public static final int INTEGER = 2;
    public static final int STRING = 1;
    public static final int LONG = 10;
    public static final int UNINITIALIZED = 0;

    /* 1 = String, 2 = int, 3 = date, 4 = double, 5 = char, 6 = char array of maxlength */
    int DataType;
    public int ObjectType = -1;
    SimpleDateFormat formatter;
    FastSimpleDateFormat fastFormatter;
    boolean isNull;
    boolean isConstant; // in theory, we don't lock the item - to be set and unset manually by the user of this
                        // item...it does not copy
    boolean ignore; // for use in keeping cardinality in synch between ports and steps...if true, this DataItem is a
                    // placeholder only.
    ParsePosition parsePosition;
    java.sql.Timestamp tmpSQLTimestamp = null;
    boolean valBoolean;
    byte[] valByteArray;
    byte valByte;
    char[] valCharArray;
    Date valDate;
    double valDouble;
    Long valLong;
    java.lang.String valError;
    float valFloat;
    int valInteger;
    String valString;

    public BaseDataItem(int dType) {
        this();
        this.DataType = dType;
    }

    public BaseDataItem() {
        super();
    }

    public Object clone() throws java.lang.CloneNotSupportedException {
        return (super.clone());
    }

    public abstract int compare(BaseDataItem pDataItem);

    /*
     * equals compares the object using hashcodes but will return false if the object datatypes are different.
     */
    public final boolean equals(Object arg0) {
        if ((DataType != ((DataItem) arg0).DataType) || (hashCode() != arg0.hashCode())) {
            return false;
        }

        return true;
    }
    
    public final void setNull(int arg0) {
        this.isNull = true;
        this.DataType = arg0;
        this.valError = null;
    }

    /**
     * @return int
     */
    public final int getDataType() {
        return DataType;
    }

    public abstract boolean set(byte pData, int pDataType);

    public abstract java.sql.Timestamp getSQLTImestamp();

    /**
     * Insert the method's description here. Creation date: (4/3/2002 3:25:39 PM)
     * 
     * @return java.lang.String
     */
    public abstract String getValAsString();

    public int hashCode() {
        return super.hashCode();
    }

    // Sets the value of the DataItem from another data item while keeping it's original type (if it can).
    // Returns 0 on success, -1 if it can't make the cast.
    public abstract int cast(BaseDataItem param);

    public abstract void set(BaseDataItem param);

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.sql.Date
     */
    public abstract void set(Date param);

    public abstract void setBoolean(boolean param);

    public abstract void setLong(long param);

    public abstract void setLong(Long param);

    public abstract void setByteArray(byte[] param);

    public abstract void setChar(byte param);

    public abstract void setChar(char param);

    public abstract void setCharArray(char[] param);

    public abstract void setDate(java.util.Date pDate);

    public abstract void setDate(long pDate);

    public abstract java.util.Date setDate(String param, SimpleDateFormat pFormatter, ParsePosition parsePosition);

    public abstract java.util.Date setDate(String param, FastSimpleDateFormat pFormatter, ParsePosition parsePosition);

    public abstract void setDate(String param, String formatString);

    public abstract void setDouble(double param);

    public abstract void setError(String pErrorString);

    public abstract void setFloat(float param);

    public abstract void setFloat(String param);

    public abstract void setInt(int param);

    public abstract void setInt(String param);

    public abstract void setLong(String param);

    public abstract void setDouble(String param);

    public abstract void setString(String param);

    public abstract String toString();

    public abstract void readExternal(java.io.ObjectInput s) throws ClassNotFoundException, IOException;

    public abstract void writeExternal(java.io.ObjectOutput s) throws IOException;

    public abstract boolean isValBoolean();

    /**
     * @return
     */
    public abstract byte getByte();

    /**
     * @return
     */
    public abstract byte[] getByteArray();

    /**
     * @return
     */
    public abstract char[] getCharArray();

    /**
     * @return
     */
    public abstract Date getDate();

    /**
     * @return
     */
    public abstract double getDouble();

    /**
     * @return
     */
    public abstract String getError();

    /**
     * @return
     */
    public abstract float getFloat();

    /**
     * @return
     */
    public abstract int getInteger();

    /**
     * @return
     */
    public abstract Long getLong();

    /**
     * @return
     */
    public abstract String getString();

    /**
     * @param b
     */
    public abstract void setByte(byte b);

    /**
     * @param i
     */
    public abstract void setInteger(int i);

    /**
     * @return
     */
    public abstract boolean isNull();

    /**
     * @return
     */
    public abstract boolean ignore();

    /**
     * @return
     */
    public abstract boolean isConstant();

    /**
     * @return
     */
    public abstract int getObjectType();

    /**
     * @param b
     */
    public abstract void setIgnore(boolean b);

    /**
     * @param b
     */
    public abstract void setConstant(boolean b);

    /**
     * @param b
     */
    public abstract void setNull(boolean b);

    /**
     * @param i
     */
    public abstract void setObjectType(int i);

    public final Object getObject() {

        if (this.isNull)
            return null;

        switch (this.DataType) {
        case BaseDataItem.STRING: // string
            return this.valString;
        case BaseDataItem.INTEGER: // int
            return this.valInteger;
        case BaseDataItem.LONG: // long
            return this.valLong;
        case BaseDataItem.DATE:
            return this.valDate;
        case BaseDataItem.FLOAT:
            return this.valFloat;
        case BaseDataItem.DOUBLE:
            return this.valDouble;
        case BaseDataItem.CHAR:
            return this.valByte;
        case BaseDataItem.CHARARRAY:
            return this.valCharArray;
        case BaseDataItem.BYTEARRAY:
            return this.valByteArray;
        case BaseDataItem.BOOLEAN:
            return Boolean.class;
        case BaseDataItem.UNINITIALIZED:
        default:
            throw new ClassCastException("Cannnot return type");
        }
    }
}
