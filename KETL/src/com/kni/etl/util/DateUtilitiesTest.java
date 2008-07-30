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
package com.kni.etl.util;

import junit.framework.Assert;
import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class DateUtilitiesTest.
 */
public class DateUtilitiesTest extends TestCase {

    /**
     * Test is same day.
     */
    public void testIsSameDay() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test is same instant.
     */
    public void testIsSameInstant() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test parse date.
     */
    public void testParseDate() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add years.
     */
    public void testAddYears() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add months.
     */
    public void testAddMonths() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add weeks.
     */
    public void testAddWeeks() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add days.
     */
    public void testAddDays() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add hours.
     */
    public void testAddHours() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add minutes.
     */
    public void testAddMinutes() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add seconds.
     */
    public void testAddSeconds() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add milliseconds.
     */
    public void testAddMilliseconds() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test add.
     */
    public void testAdd() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test round.
     */
    public void testRound() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test truncate date int.
     */
    public void testTruncateDateInt() {
        Assert.fail("Not yet implemented");
    }

    /**
     * Test truncate date.
     * 
     * @throws InterruptedException the interrupted exception
     */
    public void testTruncateDate() throws InterruptedException {

        java.util.Date date1 = new java.util.Date();
        Thread.sleep(200);
        java.util.Date date2 = new java.util.Date();
        java.util.Date date3 = DateUtilities.truncate(date1);
        java.util.Date date4 = DateUtilities.truncate(date2);

        if (date3.equals(date4))
            System.out.println("Matched " + date3 + " and " + date4);
        else
            Assert.fail("Dates did not match");
    }

}
