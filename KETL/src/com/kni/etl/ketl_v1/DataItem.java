/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl_v1;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.stringtools.FastSimpleDateFormat;

// TODO: Shouldn't we be subclassing DataItem with a DatabaseDataItem class?

/**
 * Insert the type's description here. Creation date: (3/29/2002 4:11:30 PM)
 * 
 * @author: Administrator
 */
public class DataItem extends BaseDataItem {

    /**
     *
     */
    private static final long serialVersionUID = 3257850969751697206L;

    /**
     * DataItem constructor comment.
     */
    public DataItem() {
        super();
    }

    public DataItem(int dType) {
        this();
        this.DataType = dType;
    }

    public DataItem(DataItem di) {
        this();

        this.DataType = di.DataType;

        fastFormatter = di.fastFormatter;
        ignore = di.ignore;
        isConstant = di.isConstant;
        isNull = di.isNull;
        ObjectType = di.ObjectType;
        parsePosition = di.parsePosition;
        tmpSQLTimestamp = di.tmpSQLTimestamp;

        if (isNull == false) {
            switch (di.DataType) {
            case BaseDataItem.BOOLEAN:
                valBoolean = di.valBoolean;

                break;

            case BaseDataItem.BYTEARRAY:
                valByteArray = new byte[di.valByteArray.length];
                System.arraycopy(di.valByteArray, 0, valByteArray, 0, di.valByteArray.length);

                break;

            case BaseDataItem.CHAR:
                valByte = di.valByte;

                break;

            case BaseDataItem.CHARARRAY:
                valCharArray = new char[di.valCharArray.length];
                System.arraycopy(di.valCharArray, 0, valCharArray, 0, di.valCharArray.length);

                break;

            case BaseDataItem.DATE:

                if (di.valDate != null) {
                    valDate = new Date(di.valDate.getTime());
                }
                else {
                    valDate = null;
                }

                break;

            case BaseDataItem.DOUBLE:
                valDouble = di.valDouble;

                break;

            case BaseDataItem.FLOAT:
                valFloat = di.valFloat;

                break;

            case BaseDataItem.INTEGER:
                valInteger = di.valInteger;

                break;

            case BaseDataItem.LONG:
                valLong = di.valLong;

                break;

            case BaseDataItem.STRING:
                valString = di.valString;

                break;
            }
        }

        if (di.valError != null) {
            // valError = new String(valError);
            valError = di.valError;
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 11:47:53 PM)
     * 
     * @return java.lang.Object
     * @exception java.lang.CloneNotSupportedException The exception description.
     */
    public final Object clone() throws java.lang.CloneNotSupportedException {
        DataItem newItem = null;

        newItem = (DataItem) super.clone();

        if (valString != null) {
            newItem.valString = new String(valString);

            // newItem.valString = valString;
        }

        if (valDate != null) {
            newItem.valDate = (Date) valDate.clone();
        }

        if (valError != null) {
            newItem.valError = new String(valError);

            // newItem.valError = valError;
        }

        return newItem;
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:17:00 PM)
     * 
     * @return int
     * @param param datasources.DataItem
     */
    public final int compare(BaseDataItem pDataItem) {
        // always return null as greater or null will block.
        if (isNull == true) {
            return 1;
        }

        if (pDataItem.isNull == true) {
            return 1;
        }

        if (DataType == pDataItem.DataType) {
            switch (DataType) {
            case BaseDataItem.STRING:
                return valString.compareTo(pDataItem.valString);

            case BaseDataItem.INTEGER:

                if (valInteger > pDataItem.valInteger) {
                    return (1);
                }
                else if (valInteger < pDataItem.valInteger) {
                    return (-1);
                }
                else {
                    return (0);
                }

            case BaseDataItem.LONG:
                return valLong.compareTo(pDataItem.valLong);

            case BaseDataItem.DATE:
                return valDate.compareTo(pDataItem.valDate);

            case BaseDataItem.FLOAT:

                if (valFloat > pDataItem.valFloat) {
                    return (1);
                }
                else if (valFloat < pDataItem.valFloat) {
                    return (-1);
                }
                else {
                    return (0);
                }

            case BaseDataItem.DOUBLE:

                if (valDouble > pDataItem.valDouble) {
                    return (1);
                }
                else if (valDouble < pDataItem.valDouble) {
                    return (-1);
                }
                else {
                    return (0);
                }

            case BaseDataItem.CHAR:
                break;

            case BaseDataItem.CHARARRAY:
                break;
            }
        }
        else {
            // should throw exception
            return 0;
        }

        return 0;
    }

    public final java.sql.Timestamp getSQLTImestamp() {
        if (this.tmpSQLTimestamp == null) {
            this.tmpSQLTimestamp = new java.sql.Timestamp(this.valDate.getTime());
        }
        else {
            this.tmpSQLTimestamp.setTime(this.valDate.getTime());
        }

        return (this.tmpSQLTimestamp);
    }

    /**
     * Insert the method's description here. Creation date: (4/3/2002 3:25:39 PM)
     * 
     * @return java.lang.String
     */
    public final String getValAsString() {
        try {
            return DataItemHelper.getDataItemAsString(this);
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }

    /*
     * Use the correct hashcode for each stored datatype
     */
    public final int hashCode() {
        // Convert field to appropiate datatype
        switch (this.DataType) {
        case BaseDataItem.STRING: // string
            return (this.valString.hashCode());

        case BaseDataItem.LONG: // int

            // just return the pure int
            return this.valLong.hashCode();

        case BaseDataItem.INTEGER: // int

            // just return the pure int
            return (this.valInteger);

        case BaseDataItem.DATE:
            return (this.valDate.hashCode());

        case BaseDataItem.FLOAT:
            return (Float.floatToIntBits(this.valFloat));

        case BaseDataItem.DOUBLE:

            long bits = Double.doubleToLongBits(this.valDouble);

            return (int) (bits ^ (bits >> 32));

        case BaseDataItem.CHAR:
            return (this.valByte);

        case BaseDataItem.CHARARRAY: {
            int h = 0;
            int len = this.valCharArray.length;

            for (int i = 0; i < len; i++)
                h = (31 * h) + this.valCharArray[i];

            return h;
        }

        case BaseDataItem.BOOLEAN:
            return (this.valBoolean ? 1231 : 1237);

        case BaseDataItem.BYTEARRAY: {
            int h = 0;
            int len = this.valByteArray.length;

            for (int i = 0; i < len; i++)
                h = (31 * h) + this.valByteArray[i];

            return h;
        }

        default:
            ResourcePool.LogMessage(this,ResourcePool.WARNING_MESSAGE, "Unhandled datatype hashcode for datatype = "
                    + new Integer(this.DataType).toString());

            return (super.hashCode());
        }
    }

    // Sets the value of the DataItem from another data item while keeping it's original type (if it can).
    // Returns 0 on success, -1 if it can't make the cast.
    public final int cast(BaseDataItem param) {
        if (param == null) {
            this.isNull = true;

            return 0;
        }

        // Copy the misc info
        this.isNull = param.isNull;
        this.isConstant = param.isConstant;
        this.ObjectType = param.ObjectType;
        this.formatter = param.formatter;
        this.parsePosition = param.parsePosition;
        this.tmpSQLTimestamp = param.tmpSQLTimestamp;

        try {
            switch (this.DataType) {
            case BaseDataItem.STRING:
                valString = DataItemHelper.getDataItemAsString((DataItem) param);

                break;

            case BaseDataItem.INTEGER:
                valInteger = DataItemHelper.getDataItemAsInteger((DataItem) param);

                break;

            case BaseDataItem.DATE:
                valDate = DataItemHelper.getDataItemAsDate((DataItem) param);

                break;

            case BaseDataItem.LONG:
                valLong = new Long(DataItemHelper.getDataItemAsLong((DataItem) param));

                break;

            case BaseDataItem.FLOAT:
                valFloat = DataItemHelper.getDataItemAsFloat((DataItem) param);

                break;

            case BaseDataItem.DOUBLE:
                valDouble = DataItemHelper.getDataItemAsDouble((DataItem) param);

                break;

            case BaseDataItem.CHAR:
                valByte = (byte) DataItemHelper.getDataItemAsChar((DataItem) param);

                break;

            case BaseDataItem.BOOLEAN:
                valBoolean = DataItemHelper.getDataItemAsBoolean((DataItem) param);

                break;

            default:
                return -1; // unsupported cast
            }
        } catch (Exception e) {
            return -1; // unable to cast
        }

        return 0;
    }

    public final void set(BaseDataItem param) {
        if (param == null) {
            this.isNull = true;

            return;
        }

        this.isNull = param.isNull;
        this.ObjectType = param.ObjectType;
        this.formatter = param.formatter;
        this.parsePosition = param.parsePosition;
        this.tmpSQLTimestamp = param.tmpSQLTimestamp;
        this.DataType = param.DataType;
        this.valError = param.valError;

        switch (this.DataType) {
        case BaseDataItem.STRING: // string
            valString = param.valString;

            break;

        case BaseDataItem.INTEGER: // int
            valInteger = param.valInteger;

            break;

        case BaseDataItem.LONG: // long
            valLong = param.valLong;

            break;

        case BaseDataItem.DATE:

            if (param.valDate != null) {
                valDate.setTime(param.valDate.getTime());
            }
            else {
                valDate = null;
            }

            break;

        case BaseDataItem.FLOAT:
            valFloat = param.valFloat;

            break;

        case BaseDataItem.DOUBLE:
            valDouble = param.valDouble;

            break;

        case BaseDataItem.CHAR:
            valByte = param.valByte;

            break;

        case BaseDataItem.CHARARRAY:

            // TODO: copy char array!
            valCharArray = param.valCharArray;

            // char array of maxlength
            break;

        case BaseDataItem.BOOLEAN:
            valBoolean = param.valBoolean;

            break;

        case BaseDataItem.BYTEARRAY:

            // TODO: copy byte array!
            valByteArray = param.valByteArray;

            // char array of maxlength
            break;

        default:
            break;

        /* 1 = String, 2 = int, 3 = date, 4 = double, 5 = char, 6 = char array of maxlength */
        }
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.sql.Date
     */
    public final void set(Date param) {
        DataType = BaseDataItem.DATE;

        if (param == null) {
            isNull = true;
        }
        else {
            isNull = false;
        }

        valError = null;
        valDate = param;
    }

    public final void setBoolean(boolean param) {
        this.DataType = BaseDataItem.BOOLEAN;
        this.valBoolean = param;
        valError = null;
    }

    public final void setLong(long param) {
        this.DataType = BaseDataItem.LONG;
        this.valLong = new Long(param);
        valError = null;
    }

    public final void setLong(Long param) {
        this.DataType = BaseDataItem.LONG;
        this.valLong = param;
        valError = null;
    }

    public final void setByteArray(byte[] param) {
        this.DataType = BaseDataItem.BYTEARRAY;
        this.valByteArray = param;
        valError = null;
    }

    public final void setChar(byte param) {
        this.DataType = BaseDataItem.CHAR;
        this.valByte = param;
        valError = null;
    }

    public final void setChar(char param) {
        this.DataType = BaseDataItem.CHAR;
        this.valByte = (byte) param;
        valError = null;
    }

    public final void setCharArray(char[] param) {
        this.DataType = BaseDataItem.CHARARRAY;
        this.valCharArray = param;
        valError = null;
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final void setDate(java.util.Date pDate) {
        DataType = BaseDataItem.DATE;
        valDate = pDate;
        valError = null;

        if (valDate == null) {
            isNull = true;

            // be good to handle failover date formats, such that the tool tries alternate formats in a specified order
            // first
            if (parsePosition != null) {
                setError("Error in converting to datetime at position " + parsePosition.getErrorIndex()
                        + ", check datetime format.");
            }
            else {
                setError("Error in converting to datetime at position, error available in previous step.");
            }
        }
        else {
            isNull = false;
            valError = null;
        }
    }

    public final void setDate(long pDate) {
        DataType = BaseDataItem.DATE;

        if (this.valDate == null) {
            this.valDate = new Date(pDate);
        }
        else {
            valDate.setTime(pDate);
        }

        isNull = false;
        valError = null;
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final java.util.Date setDate(String param, SimpleDateFormat pFormatter, ParsePosition parsePosition) {
        DataType = BaseDataItem.DATE;
        valError = null;

        if (param == null) {
            isNull = true;

            return null;
        }

        formatter = pFormatter;

        if (parsePosition == null) {
            parsePosition = new ParsePosition(0);
        }
        else {
            parsePosition.setIndex(0);
        }

        valDate = formatter.parse(param, parsePosition);

        if (valDate == null) {
            isNull = true;

            // be good to handle failover date formats, such that the tool tries alternate formats in a specified order
            // first
            setError("Error in converting to datetime at position " + parsePosition.getErrorIndex()
                    + ", check datetime format.");
        }
        else {
            isNull = false;
            valError = null;
        }

        return (valDate);
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final java.util.Date setDate(String param, FastSimpleDateFormat pFormatter, ParsePosition parsePosition) {
        DataType = BaseDataItem.DATE;
        valError = null;

        if (param == null) {
            isNull = true;

            return null;
        }

        fastFormatter = pFormatter;

        if (parsePosition == null) {
            parsePosition = new ParsePosition(0);
        }
        else {
            parsePosition.setIndex(0);
        }

        valDate = fastFormatter.parse(param, parsePosition);

        if (valDate == null) {
            isNull = true;

            // be good to handle failover date formats, such that the tool tries alternate formats in a specified order
            // first
            setError("Error in converting to datetime at position " + parsePosition.getErrorIndex()
                    + ", check datetime format.");
        }
        else {
            isNull = false;
            valError = null;
        }

        return (valDate);
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final void setDate(String param, String formatString) {
        DataType = BaseDataItem.DATE;
        valError = null;

        if (param == null) {
            isNull = true;

            return;
        }

        if (formatter == null) {
            formatter = new SimpleDateFormat(formatString);
            parsePosition = new ParsePosition(0);
        }
        else {
            parsePosition.setIndex(0);
        }

        valDate = formatter.parse(param, parsePosition);

        if (valDate == null) {
            isNull = true;

            // be good to handle failover date formats, such that the tool tries alternate formats in a specified order
            // first
            setError("Error in converting to datetime at position " + parsePosition.getErrorIndex()
                    + ", check datetime format.");
        }
        else {
            isNull = false;
            valError = null;
        }
    }

    public final void setDouble(double param) {
        DataType = BaseDataItem.DOUBLE;
        valDouble = param;
        valError = null;
    }

    /**
     * Insert the method's description here. Creation date: (4/3/2002 2:59:57 PM)
     * 
     * @param pErrorString java.lang.String
     */
    public final void setError(String pErrorString) {
        valError = pErrorString;
    }


    public final void setFloat(float param) {
        DataType = BaseDataItem.FLOAT;
        valFloat = param;
        valError = null;
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final void setFloat(String param) {
        DataType = BaseDataItem.FLOAT;
        valError = null;

        if (param == null) {
            isNull = true;

            return;
        }

        try {
            valFloat = Float.parseFloat(param);
            isNull = false;
        } catch (NumberFormatException ee) {
            isNull = true;
            valError = ee.getMessage();
        }
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final void setInt(int param) {
        DataType = BaseDataItem.INTEGER;
        valInteger = param;
        valError = null;
    }

    public final void setInt(String param) {
        DataType = BaseDataItem.INTEGER;
        valError = null;

        if (param == null) {
            isNull = true;

            return;
        }

        try {
            valInteger = Integer.parseInt(param);
            isNull = false;
        } catch (NumberFormatException ee) {
            isNull = true;
            valError = ee.getMessage();
        }
    }

    public final void setLong(String param) {
        DataType = BaseDataItem.LONG;
        valError = null;

        if (param == null) {
            isNull = true;

            return;
        }

        try {
            valLong = new Long(Long.parseLong(param));
            isNull = false;
        } catch (NumberFormatException ee) {
            isNull = true;
            valError = ee.getMessage();
        }
    }

    public final void setDouble(String param) {
        DataType = BaseDataItem.DOUBLE;
        valError = null;

        if (param == null) {
            isNull = true;

            return;
        }

        try {
            valDouble = Double.parseDouble(param);
            isNull = false;
        } catch (NumberFormatException ee) {
            isNull = true;
            valError = ee.getMessage();
        }
    }

    /**
     * Insert the method's description here. Creation date: (3/29/2002 4:12:09 PM)
     * 
     * @param param java.lang.Float
     */
    public final void setString(String param) {
        DataType = BaseDataItem.STRING;
        valError = null;

        if (param == null) {
            isNull = true;
        }
        else {
            isNull = false;
        }

        valError = null;
        valString = param;
    }

    public final String toString() {
        if (isNull) {
            return "(null)";
        }
        else if (valError == null) {
            return this.getValAsString();
        }
        else {
            return valError.toString();
        }
    }

    public final void readExternal(java.io.ObjectInput s) throws ClassNotFoundException, IOException {
        DataType = s.readInt();
        ObjectType = s.readInt();
        isNull = s.readBoolean();
        isConstant = s.readBoolean();
        ignore = s.readBoolean();

        switch (this.DataType) {
        case BaseDataItem.STRING: // string
            this.valString = s.readUTF();

            break;

        case BaseDataItem.INTEGER: // int
            this.valInteger = s.readInt();

            break;

        case BaseDataItem.LONG: // long
            this.valLong = (Long) s.readObject();

            break;

        case BaseDataItem.DATE:
            this.valDate = (Date) s.readObject();

            break;

        case BaseDataItem.FLOAT:
            this.valFloat = s.readFloat();

            break;

        case BaseDataItem.DOUBLE:
            this.valDouble = s.readDouble();

            break;

        case BaseDataItem.CHAR:
            this.valByte = s.readByte();

            break;

        case BaseDataItem.CHARARRAY:
            this.valCharArray = (char[]) s.readObject();

            break;

        case BaseDataItem.BYTEARRAY:
            this.valByteArray = (byte[]) s.readObject();

            break;

        case BaseDataItem.BOOLEAN:
            this.valBoolean = s.readBoolean();

            break;

        case BaseDataItem.UNINITIALIZED:
        default:
            throw new ClassCastException("Cannnot convert to string");
        }
    }

    public final void writeExternal(java.io.ObjectOutput s) throws IOException {
        s.writeInt(DataType);
        s.writeInt(ObjectType);
        s.writeBoolean(isNull);
        s.writeBoolean(isConstant);
        s.writeBoolean(ignore);

        switch (this.DataType) {
        case BaseDataItem.STRING: // string
            s.writeUTF(this.valString);

            break;

        case BaseDataItem.INTEGER: // int
            s.writeInt(this.valInteger);

            break;

        case BaseDataItem.LONG: // long
            s.writeObject(this.valLong);

            break;

        case BaseDataItem.DATE:
            s.writeObject(this.valDate);

            break;

        case BaseDataItem.FLOAT:
            s.writeFloat(this.valFloat);

            break;

        case BaseDataItem.DOUBLE:
            s.writeDouble(this.valDouble);

            break;

        case BaseDataItem.CHAR:
            s.writeByte(this.valByte);

            break;

        case BaseDataItem.CHARARRAY:
            s.writeObject(this.valCharArray);

            break;

        case BaseDataItem.BYTEARRAY:
            s.writeObject(this.valByteArray);

            break;

        case BaseDataItem.BOOLEAN:
            s.writeBoolean(this.valBoolean);

            break;

        case BaseDataItem.UNINITIALIZED:
        default:
            throw new ClassCastException("Cannnot convert to string");
        }
    }

    /**
     * @return
     */
    public final boolean isValBoolean() {
        return valBoolean;
    }

    /**
     * @return
     */
    public final byte getByte() {
        return valByte;
    }

    /**
     * @return
     */
    public final byte[] getByteArray() {
        return valByteArray;
    }

    /**
     * @return
     */
    public final char[] getCharArray() {
        return valCharArray;
    }

    /**
     * @return
     */
    public final Date getDate() {
        return valDate;
    }

    /**
     * @return
     */
    public final double getDouble() {
        return valDouble;
    }

    /**
     * @return
     */
    public final java.lang.String getError() {
        return valError;
    }

    /**
     * @return
     */
    public final float getFloat() {
        return valFloat;
    }

    /**
     * @return
     */
    public final int getInteger() {
        return valInteger;
    }

    /**
     * @return
     */
    public final Long getLong() {
        return valLong;
    }

    /**
     * @return
     */
    public final String getString() {
        return valString;
    }

    /**
     * @param b
     */
    public final void setByte(byte b) {
        this.setByteArray(new byte[] { b });
    }

    /**
     * @param i
     */
    public final void setInteger(int i) {
        this.setInt(i);
    }

    /**
     * @return
     */
    public final boolean isNull() {
        return isNull;
    }

    /**
     * @return
     */
    public final boolean ignore() {
        return ignore;
    }

    /**
     * @return
     */
    public final boolean isConstant() {
        return isConstant;
    }

    /**
     * @return
     */
    public final int getObjectType() {
        return ObjectType;
    }

    /**
     * @param b
     */
    public final void setIgnore(boolean b) {
        ignore = b;
    }

    /**
     * @param b
     */
    public final void setConstant(boolean b) {
        isConstant = b;
    }

    /**
     * @param b
     */
    public final void setNull(boolean b) {
        isNull = b;
    }

    /**
     * @param i
     */
    public final void setObjectType(int i) {
        ObjectType = i;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.BaseDataItem#set(byte, int)
     */
    public final boolean set(byte pData, int pDataType) {
        return false;
    }
}
