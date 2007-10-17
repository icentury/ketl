/*
 *  Copyright (C) Sep 10, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 */
package com.kni.etl.H2Triggers;



import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.h2.api.Trigger;

// Simple modified by trigger use in the metadata
public class TJobModdate implements Trigger {


    /* (non-Javadoc)
     * @see org.h2.api.Trigger#fire(java.sql.Connection, java.lang.Object[], java.lang.Object[])
     */
    public void fire(Connection arg0, Object[] oldRow, Object[] newRow)
            throws SQLException {
        newRow[10] = new Timestamp(new Date().getTime());
    }

    /* (non-Javadoc)
     * @see org.h2.api.Trigger#init(java.sql.Connection, java.lang.String, java.lang.String, java.lang.String)
     */
    public void init(Connection arg0, String arg1, String arg2, String arg3)
            throws SQLException {       
    }
}