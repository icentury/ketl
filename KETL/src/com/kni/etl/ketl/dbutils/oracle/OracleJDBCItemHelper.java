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
package com.kni.etl.ketl.dbutils.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import org.w3c.dom.Element;

import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class OracleJDBCItemHelper.
 */
public class OracleJDBCItemHelper extends JDBCItemHelper {

    /** The array descriptors. */
    HashMap mArrayDescriptors = new HashMap();

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.JDBCItemHelper#setParameterFromClass(java.sql.PreparedStatement, int, java.lang.Class, java.lang.Object, int, org.w3c.dom.Element)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.dbutils.JDBCItemHelper#getJavaType(int, int, int, int)
     */
    @Override
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
