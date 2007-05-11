/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 12, 2006
 * 
 */
package com.kni.etl;

import java.util.Date;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class DataItemHelper {

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

        return -1;
    }

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
