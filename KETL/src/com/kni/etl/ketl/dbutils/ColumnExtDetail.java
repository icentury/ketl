/**
 * 
 */
package com.kni.etl.ketl.dbutils;

import java.io.Serializable;

public class ColumnExtDetail implements Serializable {
	public int scale, precision, size,position;

	public Class<?> targetClass;
}