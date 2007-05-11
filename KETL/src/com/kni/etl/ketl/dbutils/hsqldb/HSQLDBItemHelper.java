package com.kni.etl.ketl.dbutils.hsqldb;

import java.sql.SQLException;

import com.kni.etl.dbutils.JDBCItemHelper;

public class HSQLDBItemHelper extends JDBCItemHelper {

    static public String getType(Class cl, boolean isKey) throws SQLException {
        if (cl == Integer.class)
            return "INTEGER";
        if (cl == Double.class)
            return "DOUBLE";
        if (cl == String.class)
            return "VARCHAR";
        if (cl == java.sql.Date.class)
            return "DATE";
        if (cl == java.util.Date.class || cl == java.sql.Timestamp.class)
            return "TIMESTAMP";
        if (cl == java.sql.Time.class)
            return "TIME";
        if (cl == Float.class)
            return "FLOAT";
        if (cl == java.math.BigDecimal.class)
            return "DECIMAL";
        if (cl == Boolean.class)
            return "BOOLEAN";
        if (cl == Byte.class)
            return "TINYINT";
        if (cl == Short.class)
            return "SMALLINT";
        if (cl == Long.class)
            return "BIGINT";
        if (cl == Byte[].class)
            return "BINARY";

        if (isKey)
            throw new SQLException("Type is part of key and cannot be an OBJECT, check type class "
                    + cl.getCanonicalName());
        return "OBJECT";
    }
}