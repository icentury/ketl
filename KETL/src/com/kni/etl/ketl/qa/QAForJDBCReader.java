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

// TODO: Auto-generated Javadoc
/**
 * The Interface QAForJDBCReader.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public interface QAForJDBCReader {

    /** The Q a_ SIZ e_ CLASSNAME. */
    public static String QA_SIZE_CLASSNAME = "com.kni.etl.ketl.qa.QAJDBCQuerySize";

    /**
     * Gets the connnection.
     * 
     * @param pos the pos
     * 
     * @return the connnection
     */
    public abstract Connection getConnnection(int pos);

    /**
     * Release connnection.
     * 
     * @param cn the cn
     */
    public abstract void releaseConnnection(Connection cn);

    /**
     * Gets the SQL statements.
     * 
     * @return the SQL statements
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public abstract SQLQuery[] getSQLStatements() throws KETLThreadException;
}
