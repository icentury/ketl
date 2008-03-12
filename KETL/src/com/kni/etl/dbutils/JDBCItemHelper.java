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

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.w3c.dom.Element;

// TODO: Auto-generated Javadoc
/**
 * The Class JDBCItemHelper.
 */
public class JDBCItemHelper {

    // convert sql datatype to required datatype
    // ENHANCE: Implement full JDC 3.0 conversion
    /**
     * Sets the parameter from class.
     * 
     * @param pPreparedStatement the prepared statement
     * @param parameterIndex the parameter index
     * @param pClass the class
     * @param pDataItem the data item
     * @param maxCharLength the max char length
     * @param pXMLConfig the XML config
     * @throws SQLException the SQL exception
     */
    public void setParameterFromClass(PreparedStatement pPreparedStatement, int parameterIndex, Class pClass,
            Object pDataItem, int maxCharLength, Element pXMLConfig) throws SQLException {

        if (pClass == Short.class || pClass == short.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.SMALLINT);
            else
                pPreparedStatement.setShort(parameterIndex, (Short) pDataItem);
        }
        else if (pClass == Integer.class || pClass == int.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.INTEGER);
            else
                pPreparedStatement.setInt(parameterIndex, (Integer) pDataItem);
        }
        else if (pClass == Double.class || pClass == double.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.DOUBLE);
            else
                pPreparedStatement.setDouble(parameterIndex, (Double) pDataItem);
        }
        else if (pClass == Float.class || pClass == float.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.FLOAT);
            else
                pPreparedStatement.setFloat(parameterIndex, (Float) pDataItem);

        }
        else if (pClass == Long.class || pClass == long.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.BIGINT);
            else
                pPreparedStatement.setLong(parameterIndex, (Long) pDataItem);

        }
        else if (pClass == byte[].class || pClass == Byte[].class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.VARBINARY);
            else
                pPreparedStatement.setBytes(parameterIndex, (byte[]) pDataItem);

        }
        else if (pClass == String.class) {
            {
                if (pDataItem == null)
                    pPreparedStatement.setNull(parameterIndex, java.sql.Types.VARCHAR);
                else {
                    String str = (String) pDataItem;

                    if (maxCharLength <= 0 || str.length() <= maxCharLength) {
                        pPreparedStatement.setString(parameterIndex, str);
                    }
                    else {
                        pPreparedStatement.setCharacterStream(parameterIndex, new java.io.StringReader(str), str
                                .length());
                    }
                }
            }

        }
        else if (pClass == java.util.Date.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
            else
                pPreparedStatement.setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date) pDataItem)
                        .getTime()));

        }
        else if (pClass == java.sql.Date.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.DATE);
            else
                pPreparedStatement.setDate(parameterIndex, (java.sql.Date) pDataItem);
        }
        else if (pClass == java.sql.Time.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.TIME);
            else
                pPreparedStatement.setTime(parameterIndex, (java.sql.Time) pDataItem);
        }
        else if (pClass == java.sql.Timestamp.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
            else
                pPreparedStatement.setTimestamp(parameterIndex, (java.sql.Timestamp) pDataItem);
        }
        else if (pClass == Boolean.class || pClass == boolean.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.BOOLEAN);
            else
                pPreparedStatement.setBoolean(parameterIndex, (Boolean) pDataItem);
        }
        else if (pClass == Character.class || pClass == char.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.BOOLEAN);
            else
                pPreparedStatement.setString(parameterIndex, pDataItem.toString());
        }
        else if (pClass == BigDecimal.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.NUMERIC);
            else
                pPreparedStatement.setBigDecimal(parameterIndex, (BigDecimal) pDataItem);
        }
        else if (pClass == java.sql.Clob.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.CLOB);
            else {
                pPreparedStatement.setClob(parameterIndex, (java.sql.Clob) pDataItem);
            }
        }
        else if (pClass == java.sql.Blob.class) {
            if (pDataItem == null)
                pPreparedStatement.setNull(parameterIndex, java.sql.Types.BLOB);
            else {
                pPreparedStatement.setBlob(parameterIndex, (java.sql.Blob) pDataItem);
            }
        }
        else if (pDataItem == null) {
            pPreparedStatement.setNull(parameterIndex, java.sql.Types.JAVA_OBJECT);
        }
        else
            pPreparedStatement.setObject(parameterIndex, pDataItem);

    }

    private StringBuffer sbuf;
    private char[] cbuf;

    /**
     * Gets the object from result set.
     * 
     * @param pRS the RS
     * @param columnIndex the column index
     * @param pClass the class
     * @param maxCharLength the max char length
     * @return the object from result set
     * @throws SQLException the SQL exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public Object getObjectFromResultSet(ResultSet pRS, int columnIndex, Class pClass, int maxCharLength)
            throws SQLException, IOException {

        Object result;

        if (pClass == Short.class || pClass == short.class) {
            result = pRS.getShort(columnIndex);
        }
        else if (pClass == Integer.class || pClass == int.class) {
            result = pRS.getInt(columnIndex);
        }
        else if (pClass == Double.class || pClass == double.class) {
            result = pRS.getDouble(columnIndex);
        }
        else if (pClass == Float.class || pClass == float.class) {
            result = pRS.getFloat(columnIndex);
        }
        else if (pClass == Long.class || pClass == long.class) {
            result = pRS.getLong(columnIndex);
        }
        else if (pClass == byte[].class) {
            result = pRS.getBytes(columnIndex);
        }
        else if (pClass == String.class) {

            result = pRS.getString(columnIndex);

            if (maxCharLength > 0 && result != null && ((String) result).length() >= maxCharLength) {

                // init buffer
                if (cbuf == null) {
                    sbuf = new StringBuffer();
                    cbuf = new char[maxCharLength];
                }

                Reader charStream = pRS.getCharacterStream(columnIndex);
                sbuf.setLength(0);
                int c;
                while ((c = charStream.read(cbuf, 0, maxCharLength)) != -1) {
                    sbuf.append(cbuf, 0, c);
                }
                result = sbuf.toString();
                charStream.close();
            }
        }
        else if (pClass == java.math.BigDecimal.class) {
            result = pRS.getBigDecimal(columnIndex);
        }
        else if (pClass == java.util.Date.class) {
            result = pRS.getTimestamp(columnIndex);
        }
        else if (pClass == java.sql.Date.class) {
            result = pRS.getDate(columnIndex);
        }
        else if (pClass == java.sql.Time.class) {
            result = pRS.getTime(columnIndex);
        }
        else if (pClass == java.sql.Timestamp.class) {
            result = pRS.getTimestamp(columnIndex);
        }
        else if (pClass == Boolean.class || pClass == boolean.class) {
            result = pRS.getBoolean(columnIndex);
        }
        else if (pClass == Clob.class) {
            result = pRS.getClob(columnIndex);
        }
        else if (pClass == Byte[].class) {
            result = pRS.getBytes(columnIndex);
        }
        else if (pClass == Blob.class) {
            result = pRS.getBlob(columnIndex);
        }
        else if (pClass == java.sql.Array.class) {
            result = pRS.getArray(columnIndex);
        }
        else {
            result = pRS.getObject(columnIndex);

            if (result != null && result.getClass() != pClass)
                throw new ClassCastException("Requested class [" + pClass.getCanonicalName()
                        + "] does not match class returned by result set [" + result.getClass().getCanonicalName()
                        + "]");
        }

        if (pRS.wasNull())
            result = null;

        return result;
    }

    /**
     * Gets the java type.
     * 
     * @param pSQLType the SQL type
     * @param pLength the length
     * @param pPrecision the precision
     * @param pScale the scale
     * @return the java type
     */
    public String getJavaType(int pSQLType, int pLength, int pPrecision, int pScale) {

        switch (pSQLType) {
        case java.sql.Types.BIGINT:
            return Long.class.getCanonicalName();
        case java.sql.Types.BINARY:
        case java.sql.Types.BLOB:
            return Byte[].class.getCanonicalName();
        case java.sql.Types.BIT:
            return Integer.class.getCanonicalName();
        case java.sql.Types.CHAR:
            return String.class.getCanonicalName();
        case java.sql.Types.DATE:
            return java.sql.Timestamp.class.getCanonicalName();
        case java.sql.Types.DOUBLE:
            return Double.class.getCanonicalName();
        case java.sql.Types.FLOAT:
            return Float.class.getCanonicalName();
        case java.sql.Types.INTEGER:
            return Integer.class.getCanonicalName();
        case java.sql.Types.LONGVARBINARY:
            return java.sql.Blob.class.getCanonicalName();
        case java.sql.Types.LONGVARCHAR:
            return java.sql.Clob.class.getCanonicalName();
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
            if (pScale == 0 && pPrecision > 0) {
                if (pPrecision < 5)
                    return Short.class.getCanonicalName();
                else if (pPrecision < 10)
                    return Integer.class.getCanonicalName();
                else if (pPrecision < 20)
                    return Long.class.getCanonicalName();
                else
                    return Double.class.getCanonicalName();
            }
            if (pScale > 0) {
                return Double.class.getCanonicalName();
            }
            return java.math.BigDecimal.class.getCanonicalName();
        case java.sql.Types.REAL:
            return Float.class.getCanonicalName();
        case java.sql.Types.SMALLINT:
            return Short.class.getCanonicalName();
        case java.sql.Types.TIME:
            return java.sql.Time.class.getCanonicalName();
        case java.sql.Types.TIMESTAMP:
            return java.sql.Timestamp.class.getCanonicalName();
        case java.sql.Types.TINYINT:
            return Byte.class.getCanonicalName();
        case java.sql.Types.VARBINARY:
            return Byte[].class.getCanonicalName();
        case java.sql.Types.CLOB:
        case java.sql.Types.VARCHAR:
            return String.class.getCanonicalName();
        case java.sql.Types.BOOLEAN:
            return Boolean.class.getCanonicalName();
        case java.sql.Types.ARRAY:
            return java.sql.Array.class.getCanonicalName();
        default:
            return Object.class.getCanonicalName();
        }
    }

}
