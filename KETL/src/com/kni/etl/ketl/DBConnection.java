package com.kni.etl.ketl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public interface DBConnection {

    public static final String DRIVER_ATTRIB = "DRIVER";
    public static final String URL_ATTRIB = "URL";
    public static final String PASSWORD_ATTRIB = "PASSWORD";
    public static final String USER_ATTRIB = "USER";
    public static final String PRESQL_ATTRIB = "PRESQL";
    public static final String PRESQL = "PRESQL";
    public static final String POSTSQL = "POSTSQL";
    public static final String TABLE_ATTRIB = "TABLE";
    public static final String SCHEMA_ATTRIB = "SCHEMA";
    public static final String PK_ATTRIB = "PK";
    public static final String SK_ATTRIB = "SK";
    public static final String INSERT_ATTRIB = "INSERT";
    public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";
    public static final String UPDATE_ATTRIB = "UPDATE";

    public Connection getConnection() throws SQLException, ClassNotFoundException;
}
