package com.kni.etl.ketl.dbutils.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.w3c.dom.Element;

import com.kni.etl.dbutils.JDBCItemHelper;

public class SQLLoaderItemHelper extends JDBCItemHelper {

    @Override
    public void setParameterFromClass(PreparedStatement pPreparedStatement, int parameterIndex, Class pClass,
            Object pDataItem, int maxCharLength, Element pXMLConfig) throws SQLException {

    }

}
