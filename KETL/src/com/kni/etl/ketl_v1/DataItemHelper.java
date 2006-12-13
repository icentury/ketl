/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Mar 3, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl_v1;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLException;
/**
 * @author nwakefield Creation Date: Mar 3, 2003 Implement a helper class which allows easy conversion of sql resultsets
 *         to dataitems.
 */
public class DataItemHelper {

    public static byte[] charToByteArray(char c) {
        byte[] twoBytes = { (byte) (c & 0xff), (byte) ((c >> 8) & 0xff) };

        return twoBytes;
    }

    public static final byte[] getDataItemsAsBytes(DataItem pDataItem) throws Exception {
        return (getDataItemsAsBytes(pDataItem, null));
    }

    public static final byte[] getDataItemsAsBytes(DataItem pDataItem, Format format) throws Exception {
        return getDataItemsAsBytes(pDataItem, format, null);
    }

    public static final byte[] getDataItemsAsBytes(DataItem pDataItem, Format format, String pCharset) throws Exception {
        String buffer;

        if (pDataItem.isNull == true) {
            return (null);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            buffer = pDataItem.valString;

            break;

        case BaseDataItem.INTEGER: // int
            buffer = Integer.toString(pDataItem.valInteger);

            break;

        case BaseDataItem.LONG: // long
            buffer = pDataItem.valLong.toString();

            break;

        case BaseDataItem.DATE:

            if (format != null) {
                buffer = ((SimpleDateFormat) format).format(pDataItem.valDate);
            }
            else {
                buffer = pDataItem.valDate.toString();
            }

            break;

        case BaseDataItem.FLOAT:

            if (format != null) {
                buffer = ((DecimalFormat) format).format(pDataItem.valFloat);
            }
            else {
                buffer = Float.toString(pDataItem.valFloat);
            }

            break;

        case BaseDataItem.DOUBLE:

            if (format != null) {
                buffer = ((DecimalFormat) format).format(pDataItem.valDouble);
            }
            else {
                buffer = Double.toString(pDataItem.valDouble);
            }

            break;

        case BaseDataItem.CHAR:
            return new byte[] { pDataItem.valByte };

        case BaseDataItem.CHARARRAY:
            buffer = String.valueOf(pDataItem.valCharArray);

            break;

        case BaseDataItem.BYTEARRAY:
            return pDataItem.valByteArray;

        case BaseDataItem.BOOLEAN:
            buffer = Boolean.toString(pDataItem.valBoolean);

            break;

        case BaseDataItem.UNINITIALIZED:

            // Empty string should suffice
            throw new ClassCastException("DataItem not initialized");

        default:
            throw new ClassCastException("Cannnot convert to string");
        }

        return pCharset == null ? buffer.getBytes() : buffer.getBytes(pCharset);
    }

    // convert DataItem datatype to a SQL DataType.
    public static final int getSQLDataTypeFromType(int pDataType) {
        switch (pDataType) {
        case BaseDataItem.STRING:
            return java.sql.Types.VARCHAR;

        case BaseDataItem.INTEGER:
            return java.sql.Types.INTEGER;

        case BaseDataItem.DATE:
            return java.sql.Types.TIMESTAMP;

        case BaseDataItem.FLOAT:
            return java.sql.Types.FLOAT;

        case BaseDataItem.LONG:
            return java.sql.Types.BIGINT;

        case BaseDataItem.DOUBLE:
            return java.sql.Types.DOUBLE;

        case BaseDataItem.CHAR:
            return java.sql.Types.CHAR;

        case BaseDataItem.BOOLEAN:
            return java.sql.Types.BIT;

        case BaseDataItem.BYTEARRAY:
            return java.sql.Types.VARBINARY;

        case BaseDataItem.CHARARRAY:
            return java.sql.Types.VARCHAR;
        }

        return java.sql.Types.OTHER;
    }

    public static final int getDataTypeIDbyName(String pDataTypeName) {
        if (pDataTypeName == null) {
            ResourcePool.LogMessage(Thread.currentThread().getName(),ResourcePool.WARNING_MESSAGE, "Missing datatype defaulting to String.");

            return BaseDataItem.STRING;
        }

        if (pDataTypeName.equals("FLOAT")) {
            return BaseDataItem.FLOAT;
        }

        if (pDataTypeName.equals("STRING")) {
            return BaseDataItem.STRING;
        }

        if (pDataTypeName.equals("LONG")) {
            return BaseDataItem.LONG;
        }

        if (pDataTypeName.equals("INTEGER")) {
            return BaseDataItem.INTEGER;
        }

        if (pDataTypeName.equals("DATE")) {
            return BaseDataItem.DATE;
        }

        if (pDataTypeName.equals("DOUBLE")) {
            return BaseDataItem.DOUBLE;
        }

        if (pDataTypeName.equals("CHAR")) {
            return BaseDataItem.CHAR;
        }

        if (pDataTypeName.equals("CHARARRAY")) {
            return BaseDataItem.CHARARRAY;
        }

        if (pDataTypeName.equals("BOOLEAN")) {
            return BaseDataItem.BOOLEAN;
        }

        if (pDataTypeName.equals("BYTEARRAY")) {
            return BaseDataItem.BYTEARRAY;
        }

        ResourcePool.LogMessage(Thread.currentThread().getName(),ResourcePool.WARNING_MESSAGE,  "Unknown datatype:" + pDataTypeName
                + " defaulting to String");

        return BaseDataItem.STRING;
    }

    public static final String getDataTypeNamebyID(int pDataType) {
        switch (pDataType) {
        case BaseDataItem.FLOAT:
            return "FLOAT";

        case BaseDataItem.STRING:
            return "STRING";

        case BaseDataItem.LONG:
            return "LONG";

        case BaseDataItem.INTEGER:
            return "INTEGER";

        case BaseDataItem.DATE:
            return "DATE";

        case BaseDataItem.DOUBLE:
            return "DOUBLE";

        case BaseDataItem.CHAR:
            return "CHAR";

        case BaseDataItem.CHARARRAY:
            return "CHARARRAY";

        case BaseDataItem.BOOLEAN:
            return "BOOLEAN";

        case BaseDataItem.BYTEARRAY:
            return "BYTEARRAY";

        default:
            return "Unknown!";
        }
    }

    public static final int getDataItemAsInteger(DataItem pDataItem) throws Exception {
        int in = -1;

        if (pDataItem.isNull == true) {
            return (in);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            in = Integer.parseInt(pDataItem.valString);

            break;

        case BaseDataItem.INTEGER: // int
            in = pDataItem.valInteger;

            break;

        case BaseDataItem.LONG: // long
            in = pDataItem.valLong.intValue();

            break;

        case BaseDataItem.DATE:

            // date
            in = (int) pDataItem.valDate.getTime();

            break;

        case BaseDataItem.FLOAT:

            // float .
            in = (int) pDataItem.valFloat;

            break;

        case BaseDataItem.DOUBLE:

            // float .
            in = (int) pDataItem.valDouble;

            break;

        case BaseDataItem.CHAR:

            // char
            in = pDataItem.valByte;

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            in = Integer.parseInt(String.valueOf(pDataItem.valCharArray));

            break;

        default:
            throw new ClassCastException("Cannnot convert to string");
        }

        return in;
    }

    public static final long getDataItemAsLong(DataItem pDataItem) throws Exception {
        long in = -1;

        if (pDataItem.isNull == true) {
            return (in);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            in = Long.parseLong(pDataItem.valString);

            break;

        case BaseDataItem.INTEGER: // int
            in = pDataItem.valInteger;

            break;

        case BaseDataItem.LONG: // long
            in = pDataItem.valLong.longValue();

            break;

        case BaseDataItem.DATE:

            // date
            in = pDataItem.valDate.getTime();

            break;

        case BaseDataItem.FLOAT:

            // float .
            in = (long) pDataItem.valFloat;

            break;

        case BaseDataItem.DOUBLE:

            // float .
            in = (long) pDataItem.valDouble;

            break;

        case BaseDataItem.CHAR:

            // char
            in = pDataItem.valByte;

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            in = Long.parseLong(String.valueOf(pDataItem.valCharArray));

            break;

        default:
            throw new ClassCastException("Cannnot convert to long");
        }

        return in;
    }

    public static final double getDataItemAsDouble(DataItem pDataItem) throws Exception {
        double db = -1;

        if (pDataItem.isNull == true) {
            return (db);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            db = Double.valueOf(pDataItem.valString).doubleValue();

            break;

        case BaseDataItem.INTEGER: // int
            db = pDataItem.valInteger;

            break;

        case BaseDataItem.LONG: // long
            db = pDataItem.valLong.doubleValue();

            break;

        case BaseDataItem.DATE:

            // date
            db = pDataItem.valDate.getTime();

            break;

        case BaseDataItem.FLOAT:

            // float .
            db = pDataItem.valFloat;

            break;

        case BaseDataItem.DOUBLE:

            // float .
            db = pDataItem.valDouble;

            break;

        case BaseDataItem.CHAR:

            // char
            db = pDataItem.valByte;

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            db = Double.valueOf(String.valueOf(pDataItem.valCharArray)).doubleValue();

            break;

        default:
            throw new ClassCastException("Cannnot convert to double");
        }

        return db;
    }

    public static final float getDataItemAsFloat(DataItem pDataItem) throws Exception {
        float fl = -1;

        if (pDataItem.isNull == true) {
            return (fl);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            fl = Float.parseFloat(pDataItem.valString);

            break;

        case BaseDataItem.INTEGER: // int
            fl = pDataItem.valInteger;

            break;

        case BaseDataItem.LONG: // int
            fl = pDataItem.valLong.floatValue();

            break;

        case BaseDataItem.DATE:

            // date
            fl = pDataItem.valDate.getTime();

            break;

        case BaseDataItem.FLOAT:

            // float .
            fl = pDataItem.valFloat;

            break;

        case BaseDataItem.DOUBLE:

            // float .
            fl = (float) pDataItem.valDouble;

            break;

        case BaseDataItem.CHAR:

            // char
            fl = pDataItem.valByte;

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            fl = Float.parseFloat(String.valueOf(pDataItem.valCharArray));

            break;

        default:
            throw new ClassCastException("Cannnot convert to float");
        }

        return fl;
    }

    public static final java.util.Date getDataItemAsDate(DataItem pDataItem) throws Exception {
        java.util.Date dt = null;

        if (pDataItem.isNull == true) {
            return (dt);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            dt = DateFormat.getTimeInstance().parse(pDataItem.valString);

            break;

        case BaseDataItem.INTEGER: // int
            dt = new java.util.Date(pDataItem.valInteger);

            break;

        case BaseDataItem.LONG: // int
            dt = new java.util.Date(pDataItem.valLong.longValue());

            break;

        case BaseDataItem.DATE:

            // date
            dt = pDataItem.valDate;

            break;

        case BaseDataItem.FLOAT:

            // float .
            dt = new Date((long) pDataItem.valFloat);

            break;

        case BaseDataItem.DOUBLE:

            // float .
            dt = new Date((long) pDataItem.valDouble);

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            dt = DateFormat.getTimeInstance().parse(String.valueOf(pDataItem.valCharArray));

            break;

        default:
            throw new ClassCastException("Cannnot convert to string");
        }

        return dt;
    }

    public static final char getDataItemAsChar(DataItem pDataItem) throws Exception {
        char ch = 0;

        if (pDataItem.isNull == true) {
            return (ch);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            if (pDataItem.valString.length() <= 1) {
                ch = pDataItem.valString.charAt(0);
            }
            else {
                throw new ClassCastException("String to long to be a char");
            }

            break;

        case BaseDataItem.INTEGER: // int

            if (pDataItem.valInteger < 256) {
                ch = (char) pDataItem.valInteger;
            }
            else {
                throw new ClassCastException("Integer greater than 256 char conversion not possible");
            }

            break;

        case BaseDataItem.LONG: // int

            if (pDataItem.valLong.longValue() < 256) {
                ch = (char) pDataItem.valLong.intValue();
            }
            else {
                throw new ClassCastException("Integer greater than 256 char conversion not possible");
            }

            break;

        case BaseDataItem.FLOAT:

            if (pDataItem.valFloat < 256) {
                ch = (char) pDataItem.valFloat;
            }
            else {
                throw new ClassCastException("Float greater than 256 char conversion not possible");
            }

            break;

        case BaseDataItem.DOUBLE:

            if (pDataItem.valDouble < 256) {
                ch = (char) pDataItem.valDouble;
            }
            else {
                throw new ClassCastException("Double greater than 256 char conversion not possible");
            }

            break;

        case BaseDataItem.CHAR:

            // char
            ch = (char) pDataItem.valByte;

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            if (pDataItem.valCharArray.length == 1) {
                ch = pDataItem.valCharArray[0];
            }
            else {
                throw new ClassCastException("ChrArray will not shrink to char");
            }

            break;

        default:
            throw new ClassCastException("Cannnot convert to char");
        }

        return ch;
    }

    public static final boolean getDataItemAsBoolean(DataItem pDataItem) throws Exception {
        boolean bo = false;

        if (pDataItem.isNull == true) {
            return (bo);
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean strin
            bo = Boolean.getBoolean(pDataItem.valString);

            break;

        case BaseDataItem.INTEGER: // int

            if (pDataItem.valInteger == 0) {
                bo = false;
            }
            else {
                bo = true;
            }

            break;

        case BaseDataItem.LONG: // long

            if (pDataItem.valLong.longValue() == 0) {
                bo = false;
            }
            else {
                bo = true;
            }

            break;

        case BaseDataItem.FLOAT:

            if (pDataItem.valFloat == 0) {
                bo = false;
            }
            else {
                bo = true;
            }

            break;

        case BaseDataItem.DOUBLE:

            if (pDataItem.valDouble == 0) {
                bo = false;
            }
            else {
                bo = true;
            }

            break;

        case BaseDataItem.CHAR:

            // char
            if (pDataItem.valByte == 0) {
                bo = false;
            }
            else {
                bo = true;
            }

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            bo = Boolean.getBoolean(String.valueOf(pDataItem.valCharArray));

            break;

        case BaseDataItem.BOOLEAN:
            bo = pDataItem.valBoolean;

            break;

        default:
            throw new ClassCastException("Cannnot convert to char");
        }

        return bo;
    }

    public static final String getDataItemAsString(DataItem pDataItem) throws Exception {
        String str = null;

        if (pDataItem.isNull == true) {
            return ("");
        }

        // Convert field to appropiate datatype
        switch (pDataItem.DataType) {
        case BaseDataItem.STRING: // string

            // clean string
            str = pDataItem.valString;

            break;

        case BaseDataItem.INTEGER: // int
            str = Integer.toString(pDataItem.valInteger);

            break;

        case BaseDataItem.LONG: // long
            str = pDataItem.valLong.toString();

            break;

        case BaseDataItem.DATE:

            // date
            str = pDataItem.valDate.toString();

            break;

        case BaseDataItem.FLOAT:

            // float .
            str = Float.toString(pDataItem.valFloat);

            break;

        case BaseDataItem.DOUBLE:

            // float .
            str = Double.toString(pDataItem.valDouble);

            break;

        case BaseDataItem.CHAR:

            // char
            str = String.valueOf(pDataItem.valByte);

            break;

        case BaseDataItem.CHARARRAY:

            // char array of maxlength
            str = String.valueOf(pDataItem.valCharArray);

            break;

        case BaseDataItem.BYTEARRAY:

            // Empty string should suffice
            str = new String(pDataItem.valByteArray);

            break;

        case BaseDataItem.UNINITIALIZED:

            // Empty string should suffice
            str = "";

            break;

        default:
            throw new ClassCastException("Cannnot convert to string");
        }

        return str;
    }

    /*
     * Convert SQL Type to DataItem type mappings taken from JDBC 3.0 specification section B-182
     */
    public static final int getDataTypeFromSQLType(int pSQLDataType, int pPrecision, int pScale) throws Exception {
        switch (pSQLDataType) {
        // Byte Array's
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
        case java.sql.Types.BINARY:
            return BaseDataItem.BYTEARRAY;

        // Boolean's
        case java.sql.Types.BIT:
            return BaseDataItem.BOOLEAN;

        // Date's
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
        case java.sql.Types.DATE:
            return BaseDataItem.DATE;

        // Double's
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
        case java.sql.Types.DOUBLE:
            return BaseDataItem.DOUBLE;

        // Longs
        case java.sql.Types.BIGINT:
            return BaseDataItem.LONG;

        // Float's
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
            return BaseDataItem.FLOAT;

        // Integer's
        case java.sql.Types.INTEGER:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
            return BaseDataItem.INTEGER;

        // String's
        case java.sql.Types.CHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.VARCHAR:
            return BaseDataItem.STRING;
        }

        throw new Exception("Unknown type " + new Integer(pSQLDataType).toString());
    }

    // convert sql datatype to required datatype
    // ENAHNCE: Implement full JDC 3.0 conversion
    public static final DataItem setFromSQLData(ResultSet pRS, int pColIndex, int pSQLDataType, int pRequiredDataType,
            DataItem pDataItem) throws Exception {
        if (pDataItem == null) {
            pDataItem = new DataItem();
        }

        pDataItem.DataType = pRequiredDataType;

        switch (pRequiredDataType) {
        case BaseDataItem.CHAR:

            switch (pSQLDataType) {
            case java.sql.Types.CHAR:
                pDataItem.setChar(pRS.getByte(0));

                break;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to char");
            }

            break;

        case BaseDataItem.CHARARRAY:

            switch (pSQLDataType) {
            case java.sql.Types.BIGINT:
            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:
            case java.sql.Types.CHAR:
            case java.sql.Types.DATE:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TINYINT:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.VARCHAR:

                String s = pRS.getString(pColIndex);

                if (s != null) {
                    pDataItem.setCharArray(s.toCharArray());
                }

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

                break;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to CharArray");
            }

            break;

        case BaseDataItem.STRING:

            switch (pSQLDataType) {
            case java.sql.Types.BIGINT:
            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:
            case java.sql.Types.CHAR:
            case java.sql.Types.DATE:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TINYINT:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.VARCHAR:
                pDataItem.setString(pRS.getString(pColIndex));

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

                break;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to String");
            }

            break;

        case BaseDataItem.BOOLEAN:

            switch (pSQLDataType) {
            case java.sql.Types.BIT:
                pDataItem.setBoolean(pRS.getBoolean(pColIndex));

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

                break;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to ByteArray");
            }

        case BaseDataItem.BYTEARRAY:

            switch (pSQLDataType) {
            case java.sql.Types.BIGINT:
            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:
            case java.sql.Types.CHAR:
            case java.sql.Types.DATE:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TINYINT:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.VARCHAR:
                pDataItem.setByteArray(pRS.getBytes(pColIndex));

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

                break;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to ByteArray");
            }

            break;

        case BaseDataItem.DATE:

            switch (pSQLDataType) {
            case java.sql.Types.DECIMAL:
            case java.sql.Types.BIGINT:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:

                java.math.BigDecimal bd = pRS.getBigDecimal(pColIndex);

                if (bd != null) {
                    pDataItem.setDate(bd.longValue());
                }

                break;

            // case java.sql.Types.DATE:
            // pDataItem.setDate(pRS.getDate(pColIndex));
            // break;
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                pDataItem.setDate(pRS.getInt(pColIndex));

                break;

            case java.sql.Types.TIME:

                Time t = pRS.getTime(pColIndex);

                if (t != null) {
                    pDataItem.setDate(t.getTime());
                }

                break;

            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.DATE:

                Timestamp ts = pRS.getTimestamp(pColIndex);

                if (ts != null) {
                    pDataItem.setDate(ts.getTime());
                }

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

                break;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to Date");
            }

            break;

        case BaseDataItem.DOUBLE:

            switch (pSQLDataType) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.TINYINT:
                pDataItem.setDouble(pRS.getDouble(pColIndex));

                break;

            case java.sql.Types.BIGINT:

                java.math.BigDecimal bd = pRS.getBigDecimal(pColIndex);

                if (bd != null) {
                    pDataItem.setDouble(bd.doubleValue());
                }

                break;

            case java.sql.Types.FLOAT:
                pDataItem.setDouble(pRS.getFloat(pColIndex));

            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:

                if (pRS.getBoolean(pColIndex)) {
                    pDataItem.setDouble(1);
                }
                else {
                    pDataItem.setDouble(0);
                }

                break;

            case java.sql.Types.CHAR:
                pDataItem.setDouble(pRS.getByte(pColIndex));

                break;

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR:
                pDataItem.setDouble(Double.parseDouble(pRS.getString(pColIndex)));

                break;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:

                Timestamp ts = pRS.getTimestamp(pColIndex);

                if (ts != null) {
                    pDataItem.setDouble(ts.getTime());
                }

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to double");
            }

            break;

        case BaseDataItem.FLOAT:

            switch (pSQLDataType) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.TINYINT:
            case java.sql.Types.FLOAT:
                pDataItem.setFloat(pRS.getFloat(pColIndex));

                break;

            case java.sql.Types.BIGINT:

                java.math.BigDecimal bd = pRS.getBigDecimal(pColIndex);

                if (bd != null) {
                    pDataItem.setFloat(bd.floatValue());
                }

                break;

            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:

                if (pRS.getBoolean(pColIndex)) {
                    pDataItem.setFloat(1);
                }
                else {
                    pDataItem.setFloat(0);
                }

                break;

            case java.sql.Types.CHAR:
                pDataItem.setFloat(pRS.getByte(pColIndex));

                break;

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR:
                pDataItem.setFloat(Float.parseFloat(pRS.getString(pColIndex)));

                break;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:

                Timestamp ts = pRS.getTimestamp(pColIndex);

                if (ts != null) {
                    pDataItem.setFloat(ts.getTime());
                }

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to float");
            }

            break;

        case BaseDataItem.INTEGER:

            switch (pSQLDataType) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.TINYINT:
            case java.sql.Types.FLOAT:
                pDataItem.setInt(pRS.getInt(pColIndex));

                break;

            case java.sql.Types.BIGINT:

                java.math.BigDecimal bd = pRS.getBigDecimal(pColIndex);

                if (bd != null) {
                    pDataItem.setInt(bd.intValue());
                }

                break;

            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:

                if (pRS.getBoolean(pColIndex)) {
                    pDataItem.setInt(1);
                }
                else {
                    pDataItem.setInt(0);
                }

                break;

            case java.sql.Types.CHAR:
                pDataItem.setInt(pRS.getByte(pColIndex));

                break;

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR:
                pDataItem.setInt(pRS.getString(pColIndex));

                break;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:

                Timestamp ts = pRS.getTimestamp(pColIndex);

                if (ts != null) {
                    pDataItem.setInt((int) ts.getTime());
                }

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to integer");
            }

            break;

        case BaseDataItem.LONG:

            switch (pSQLDataType) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.TINYINT:
            case java.sql.Types.FLOAT:
                pDataItem.setLong(pRS.getLong(pColIndex));

                break;

            case java.sql.Types.BIGINT:

                java.math.BigDecimal bd = pRS.getBigDecimal(pColIndex);

                if (bd != null) {
                    pDataItem.setLong(bd.longValue());
                }

                break;

            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:

                if (pRS.getBoolean(pColIndex)) {
                    pDataItem.setLong(1);
                }
                else {
                    pDataItem.setLong(0);
                }

                break;

            case java.sql.Types.CHAR:
                pDataItem.setLong(pRS.getByte(pColIndex));

                break;

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR:
                pDataItem.setLong(pRS.getString(pColIndex));

                break;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:

                Timestamp ts = pRS.getTimestamp(pColIndex);

                if (ts != null) {
                    pDataItem.setLong(ts.getTime());
                }

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

            default:
                pDataItem.isNull = true;
                throw new Exception("Cannot convert datatype to long");
            }

            break;
        }

        if (pRS.wasNull()) {
            pDataItem.isNull = true;
        }

        return pDataItem;
    }

    // convert sql datatype to required datatype
    // ENAHNCE: Implement full JDC 3.0 conversion
    public static final void setParameterFromDataItem(PreparedStatement pPreparedStatement, int pParameterPos,
            int pSQLDataType, DataItem pDataItem) throws SQLException {
        String str = null;
        int maxCharLength = pPreparedStatement.getConnection().getMetaData().getMaxCharLiteralLength();

        try {
            switch (pSQLDataType) {
            case java.sql.Types.BIGINT:
                pPreparedStatement.setLong(pParameterPos, DataItemHelper.getDataItemAsLong(pDataItem));

                break;

            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                pPreparedStatement.setFloat(pParameterPos, DataItemHelper.getDataItemAsFloat(pDataItem));

                break;

            case java.sql.Types.VARBINARY:
            case java.sql.Types.BINARY:
            case java.sql.Types.LONGVARBINARY:
                pPreparedStatement.setBytes(pParameterPos, pDataItem.valByteArray);

                break;

            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
                str = DataItemHelper.getDataItemAsString(pDataItem);

                if (str.length() <= maxCharLength) {
                    pPreparedStatement.setString(pParameterPos, str);
                }
                else {
                    pPreparedStatement.setCharacterStream(pParameterPos, new java.io.StringReader(str), str.length());
                }

                break;

            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.DATE:

                java.sql.Timestamp tm = new Timestamp(DataItemHelper.getDataItemAsDate(pDataItem).getTime());
                pPreparedStatement.setTimestamp(pParameterPos, tm);

                break;

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.DECIMAL:
                pPreparedStatement.setDouble(pParameterPos, DataItemHelper.getDataItemAsDouble(pDataItem));

                break;

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                pPreparedStatement.setInt(pParameterPos, DataItemHelper.getDataItemAsInteger(pDataItem));

                break;

            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                pPreparedStatement.setBoolean(pParameterPos, DataItemHelper.getDataItemAsBoolean(pDataItem));

                break;

            case java.sql.Types.NULL:
                pDataItem.isNull = true;

                break;

            default:

                try {
                    str = DataItemHelper.getDataItemAsString(pDataItem);

                    if (str.length() <= maxCharLength) {
                        pPreparedStatement.setString(pParameterPos, str);
                    }
                    else {
                        pPreparedStatement.setCharacterStream(pParameterPos, new java.io.StringReader(str), str
                                .length());
                    }
                } catch (SQLException e) {
                    e.setNextException(new SQLException("Cannot convert type to required type"));
                    throw e;
                }

                break;
            }
        } catch (SQLException e) {
            SQLException e2 = new SQLException("Could not populate statement parameter - " + e.getMessage());
            e2.setNextException(e);
            throw e2;
        } catch (Exception e) {
            e.printStackTrace();

            SQLException e2 = new SQLException("Could not populate statement parameter - " + e.getMessage());
            e2.setNextException((SQLException) e);

            throw e2;
        }
    }

    /*
     * populate statement with values from DataItem, insert null values for missing data item
     */
    public static final int populateStatementValues(PreparedStatement pPreparedStatement, int pParameterPos,
            DataItem[] pValues) throws SQLException {
        return (DataItemHelper.populateStatementValues(pPreparedStatement, pParameterPos, pValues, true, true));
    }

    /*
     * populate statement with values from DataItem, if pTreatNullDataItemsAsNulls is true then insert null value for
     * missing data item else go to next that isn't null and use that. If pTreatDataItemsOfNull = true then data items
     * with isnull set will not be skipped
     */
    public static final int populateStatementValues(PreparedStatement pPreparedStatement, int pParameterPos,
            DataItem[] pValues, boolean pTreatNullDataItemsAsNulls, boolean pTreatDataItemsOfNull) throws SQLException {
        if (pPreparedStatement == null) {
            return -1;
        }

        for (int i = 0; i < pValues.length; i++) {
            int iColType = java.sql.Types.NULL;

            if (pValues[i] != null) {
                iColType = DataItemHelper.getSQLDataTypeFromType(pValues[i].DataType);
            }

            if (((pValues[i] == null) && (pTreatNullDataItemsAsNulls == true))
                    || ((pValues[i] != null) && (pValues[i].isNull == true) && (pTreatDataItemsOfNull == true))) {
                pPreparedStatement.setNull(pParameterPos, iColType);
                pParameterPos++;
            }
            else if ((pValues[i] != null)
                    && ((pValues[i].isNull == false) || ((pTreatDataItemsOfNull == true) && (pValues[i].isNull == true)))) {
                DataItemHelper.setParameterFromDataItem(pPreparedStatement, pParameterPos, iColType, pValues[i]);

                pParameterPos++;
            }
        }

        return pParameterPos;
    }

    public static final String getKeyForDataItems(DataItem[] pdKey) {
        StringBuffer sbufKey = null;

        // Build unique key required for lookup.
        String str = null;

        for (int i = pdKey.length - 1; i >= 0; i--) {
            if (pdKey[i].isNull) {
                str = "\r";
            }
            else {
                str = pdKey[i].getValAsString();
            }

            if (sbufKey == null) {
                sbufKey = new StringBuffer(str);
            }
            else {
                sbufKey.append('\r');
                sbufKey.append(str);
            }
        }

        str = sbufKey.toString();

        return str;
    }

    public static String getDataItemArrayNullBasedKey(DataItem[][] pdKey, String pStartKey, int ikeySize) {
        StringBuffer sbufKey = new StringBuffer(ikeySize * pdKey[0].length);

        if (pStartKey != null) {
            sbufKey.append(pStartKey);
        }

        for (int i = ikeySize - 1; i >= 0; i--) {
            for (int ix = pdKey[i].length - 1; ix >= 0; ix--) {
                if (pdKey[i][ix].isNull) {
                    sbufKey.append('0');
                }
                else {
                    sbufKey.append('1');
                }
            }
        }

        return sbufKey.toString();
    }

    public final static Object getDataItemAsObject(Class pRequiredType, DataItem pDataItem) throws Exception {

        if (pRequiredType == String.class)
            return getDataItemAsString(pDataItem);
        if (pRequiredType == Integer.class || pRequiredType == int.class)
            return getDataItemAsInteger(pDataItem);
        if (pRequiredType == Long.class || pRequiredType == long.class)
            return getDataItemAsLong(pDataItem);
        if (pRequiredType == java.util.Date.class)
            return getDataItemAsDate(pDataItem);
        if (pRequiredType == Float.class  || pRequiredType == float.class)
            return getDataItemAsFloat(pDataItem);
        if (pRequiredType == Double.class || pRequiredType == double.class)
            return getDataItemAsDouble(pDataItem);
        if (pRequiredType == Boolean.class || pRequiredType == boolean.class)
            return getDataItemAsBoolean(pDataItem);
        if (pRequiredType == byte[].class)
            return getDataItemsAsBytes(pDataItem);
        
        throw new ClassCastException("Unknown datatype " + pRequiredType.getName());

    }

    public final static DataItem getDataItemFromObject(Class pRequiredType, Object obj) throws Exception {
        DataItem di = new DataItem();

        if (pRequiredType == String.class)
            di.setString((String) obj);
        else if (pRequiredType == Integer.class || pRequiredType == int.class)
            di.setInteger((Integer) obj);
        else if (pRequiredType == Long.class || pRequiredType == long.class)
            di.setLong((Long) obj);
        else if (pRequiredType == java.util.Date.class)
            di.setDate((java.util.Date) obj);
        else if (pRequiredType == Float.class || pRequiredType == float.class)
            di.setFloat((Float) obj);
        else if (pRequiredType == Double.class || pRequiredType == double.class)
            di.setDouble((Double) obj);
        else if (pRequiredType == Boolean.class || pRequiredType == boolean.class)
            di.setBoolean((Boolean) obj);
        else if (pRequiredType == byte[].class)
            di.setByteArray((byte[]) obj);
        else if (pRequiredType == char[].class)
            di.setCharArray((char[]) obj);
        else
            throw new ClassCastException("Unknown datatype " + pRequiredType.getName());
        return di;
    }
    
    public final static Class getClassForDataType(int type) throws Exception {
        
        switch (type) {
        case BaseDataItem.FLOAT:
            return float.class;

        case BaseDataItem.STRING:
            return String.class;

        case BaseDataItem.LONG:
            return long.class;

        case BaseDataItem.INTEGER:
            return int.class;

        case BaseDataItem.DATE:
            return Date.class;

        case BaseDataItem.DOUBLE:
            return double.class;

        case BaseDataItem.CHAR:
            return char.class;

        case BaseDataItem.CHARARRAY:
            return char[].class;

        case BaseDataItem.BOOLEAN:
            return boolean.class;

        case BaseDataItem.BYTEARRAY:
            return byte[].class;

        default:
            throw new KETLException("Unknown datatype " + type);
        }
       
    }
}
