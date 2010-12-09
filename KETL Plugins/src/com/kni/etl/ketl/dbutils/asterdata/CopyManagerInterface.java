package com.kni.etl.ketl.dbutils.asterdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.sql.SQLWarning;

public interface CopyManagerInterface {

	public abstract void setCopyBufferSize(int arg0);

	public abstract byte[] encodeString(String arg0) throws IOException;

	public abstract String getEncoding();

	/**
	 * Copy data from the InputStream into the given table using the default
	 * copy parameters.
	 */
	public abstract void copyIn(String table, InputStream is) throws SQLException;

	/**
	 * Copy data from the InputStream using the given COPY query. This allows
	 * specification of additional copy parameters such as the delimiter or NULL
	 * marker.
	 */
	public abstract void copyInQuery(String query, InputStream is) throws SQLException;

	/**
	 * Copy data from the given table to the OutputStream using the default copy
	 * parameters.
	 */
	public abstract void copyOut(String table, OutputStream os) throws SQLException;

	/**
	 * Copy data to the OutputStream using the given COPY query. This allows
	 * specification of additional copy parameters such as the delimiter or NULL
	 * marker.
	 */
	public abstract void copyOutQuery(String query, OutputStream os) throws SQLException;

	public abstract SQLWarning getWarnings();

	public abstract void clearWarnings();

	public abstract void commit() throws SQLException;

}