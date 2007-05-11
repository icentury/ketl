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
package com.kni.etl.ketl.dbutils.postgresql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import junit.framework.Assert;
import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class PGCopyWriterTest.
 */
public class PGCopyWriterTest extends TestCase {

    /**
     * Test PG copy writer.
     */
    public void testPGCopyWriter() {
        // very simple test case
        // open connection to db
        // create test table
        // load 2000000 rows in batch

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
            Assert.fail("Couldn't find driver class:");

        }

        Connection c = null;
        try {
            try {
                // The second and third arguments are the username and password,
                // respectively. They should be whatever is necessary to connect
                // to the database.
                c = DriverManager.getConnection("jdbc:postgresql://localhost/postgres", "postgres", "postgres");
            } catch (SQLException se) {
                se.printStackTrace();
                Assert.fail("Couldn't connect: print out a stack trace and exit.");

            }

            Statement stmt = null;
            try {
                stmt = c.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail("Creating statement");
            }

            String tableName = "COPYTEST" + System.nanoTime();
            try {
                stmt.executeUpdate("create table " + tableName
                        + "(colStr text,colDbl double precision, colInt integer,colTimestamp timestamp)");
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail("Creating target test table " + tableName);
            }

            PGCopyWriter copyWriter = null;
            try {
                copyWriter = new PGCopyWriter(c);
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail("Instantiating copy writer");
            }

            copyWriter.createLoadCommand(tableName, new String[] { "colStr", "colDbl", "colInt", "colTimestamp" });

            int vals = 2000000;
            Date st = new java.util.Date();
            for (int i = 1; i <= vals; i++) {
                try {
                    copyWriter.setString(1, "val" + i);

                    copyWriter.setDouble(2, 23.454545 + i, 3);

                    if (i % 100 == 0) {
                        copyWriter.setNull(3, java.sql.Types.INTEGER);
                    }
                    else {
                        copyWriter.setInt(3, i);
                    }
                    copyWriter.setTimestamp(4, new java.sql.Timestamp(i));
                } catch (IOException e) {
                    e.printStackTrace();
                    Assert.fail("Writing field to record for submission to db using copy writer");
                }

                try {
                    copyWriter.addBatch();
                } catch (Exception e) {
                    Assert.fail("Submitting record into batch");
                }

                if (i % 50000 == 0 || i == vals) {
                    try {
                        copyWriter.executeBatch();
                    } catch (Exception e) {
                        Assert.fail("Submitting batch");
                    }
                    System.out.println("Submitting batch, records inserted: " + i);

                }
            }

            try {
                c.commit();
            } catch (Exception e) {
                Assert.fail("Commit data");
            }

            float time = (new Date().getTime() - st.getTime()) / (float) 1000;

            System.out.println("Write Done: " + time + ", " + vals / time + "rec/s");

            try {
                copyWriter.close();
            } catch (Exception e) {
                Assert.fail("Close copy writer");
            }

            try {
                stmt.executeUpdate("drop table " + tableName);
            } catch (Exception e) {
                Assert.fail("Drop target table " + tableName);
            }
        } finally {

            try {
                c.close();
            } catch (SQLException e) {
                Assert.fail("Closing connection");
                e.printStackTrace();
            }
        }
    }

}
