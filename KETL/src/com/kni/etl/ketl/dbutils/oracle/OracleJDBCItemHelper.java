package com.kni.etl.ketl.dbutils.oracle;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import oracle.jdbc.OracleResultSet;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;

import org.w3c.dom.Element;

import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.util.XMLHelper;

public class OracleJDBCItemHelper extends JDBCItemHelper {

    HashMap mArrayDescriptors = new HashMap();

    @Override
    public void setParameterFromClass(PreparedStatement pPreparedStatement, int parameterIndex, Class pClass,
            Object pDataItem, int maxCharLength, Element pXMLConfig) throws SQLException {

        if (pClass == Byte[].class)
            super.setParameterFromClass(pPreparedStatement, parameterIndex, pClass, pDataItem, maxCharLength,
                    pXMLConfig);
        else if (pClass == oracle.sql.OPAQUE.class)
            pPreparedStatement.setObject(parameterIndex, pDataItem);
        else if (pClass.isArray()) {
            HashMap res = (HashMap) this.mArrayDescriptors.get(pPreparedStatement);
            if (res == null) {
                res = new HashMap();
                this.mArrayDescriptors.put(pPreparedStatement, res);
            }

            ArrayDescriptor descriptor = (ArrayDescriptor) res.get(parameterIndex);

            if (true || descriptor == null) {
                String typename = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), "ARRAYTYPENAME", null);
                if (typename == null)
                    throw new SQLException("ARRAYTYPENAME must be specified for port containing an array");

                descriptor = ArrayDescriptor.createDescriptor(typename, pPreparedStatement.getConnection());
                res.put(parameterIndex, descriptor);
            }

            pPreparedStatement.setObject(parameterIndex, new ARRAY(descriptor, pPreparedStatement.getConnection(),
                    pDataItem));

        }
        else
            super.setParameterFromClass(pPreparedStatement, parameterIndex, pClass, pDataItem, maxCharLength,
                    pXMLConfig);
    }

    public String getJavaType(int pSQLType, int pLength, int pPrecision, int pScale) {

        switch (pSQLType) {
        case oracle.jdbc.OracleTypes.RAW:
            return Byte[].class.getCanonicalName();
        case oracle.jdbc.OracleTypes.OPAQUE:
            return oracle.sql.OPAQUE.class.getCanonicalName();
        case oracle.jdbc.OracleTypes.BLOB:
            return Byte[].class.getCanonicalName();
        case oracle.jdbc.OracleTypes.CLOB:
            return String.class.getCanonicalName();
        default:
            return super.getJavaType(pSQLType, pLength, pPrecision, pScale);
        }
    }

}
