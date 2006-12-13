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

public class JDBCItemHelper {

    // convert sql datatype to required datatype
    // ENHANCE: Implement full JDC 3.0 conversion
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
        else if (pClass == byte[].class) {
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
                pPreparedStatement.setBigDecimal(parameterIndex, (BigDecimal)pDataItem);
        }
        else if (pDataItem == null) {
            pPreparedStatement.setNull(parameterIndex, java.sql.Types.JAVA_OBJECT);
        }
        else
            pPreparedStatement.setObject(parameterIndex, pDataItem);

    }

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

            if (maxCharLength > 0 && result != null && ((String) result).length() == maxCharLength) {
                Reader charStream = pRS.getCharacterStream(columnIndex);

                StringBuilder sb = new StringBuilder();
                int c;
                while ((c = charStream.read()) != -1) {
                    sb.append((char) c);
                }
                result = sb.toString();
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
        else if (pClass == Blob.class) {
            result = pRS.getBlob(columnIndex);
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

}
