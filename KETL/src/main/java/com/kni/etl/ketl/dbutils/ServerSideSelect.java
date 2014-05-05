package com.kni.etl.ketl.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ServerSideSelect {
	private Connection con;

	private int cursorFetchSize = 1000;
	
	private int fetchSize = 100;

	private Statement stmt;

	private PreparedStatement preparedStmt;

	private PreparedStatement preparedFetchStmt;

	public ServerSideSelect(Connection connection) throws SQLException {
		super();
		this.con = connection;
		if (this.con.getAutoCommit()) {
			throw new SQLException("Server side select requires auto-commit to be set to false");
		}
	}

	public void close() throws SQLException {
		stmt.execute("close c1");
		stmt.close();
		preparedFetchStmt.close();
		preparedStmt.close();
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		stmt = con.createStatement();
		
		preparedStmt = con.prepareStatement("declare c1 NO SCROLL cursor for " + sql);
		preparedFetchStmt = con.prepareStatement("fetch forward " + cursorFetchSize + " from c1");
		preparedFetchStmt.setFetchSize(fetchSize);
		preparedFetchStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
		return preparedStmt;
	}

	public ResultSet executeQuery() throws SQLException {
		 this.preparedStmt.execute();
		 return this.getNextResultSet();
	}
	
	public ResultSet getNextResultSet() throws SQLException {
		return preparedFetchStmt.executeQuery();
	}

	public boolean isComplete(int rowsInResultset) {
		return !(rowsInResultset == this.cursorFetchSize);
	}

	public void setCursorFetchSize(int cursorFetchSize) {
		this.cursorFetchSize = cursorFetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

}
