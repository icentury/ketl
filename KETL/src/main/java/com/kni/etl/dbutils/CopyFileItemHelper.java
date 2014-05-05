package com.kni.etl.dbutils;

public class CopyFileItemHelper extends JDBCItemHelper {

	@Override
	public String getJavaType(int pSQLType, int pLength, int pPrecision,
			int pScale) {

		switch (pSQLType) {
		case java.sql.Types.DATE:
			return java.sql.Date.class.getCanonicalName();
		default:
			return super.getJavaType(pSQLType, pLength, pPrecision, pScale);
		}
	}

}
