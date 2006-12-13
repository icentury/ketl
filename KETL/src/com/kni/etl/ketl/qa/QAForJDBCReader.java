/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Nov 18, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.qa;

import java.sql.Connection;

import com.kni.etl.dbutils.SQLQuery;
import com.kni.etl.ketl.exceptions.KETLThreadException;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public interface QAForJDBCReader {

    public static String QA_SIZE_CLASSNAME = "com.kni.etl.ketl.qa.QAJDBCQuerySize";

    public abstract Connection getConnnection(int pos);

    public abstract void releaseConnnection(Connection cn);

    public abstract SQLQuery[] getSQLStatements() throws KETLThreadException;
}
