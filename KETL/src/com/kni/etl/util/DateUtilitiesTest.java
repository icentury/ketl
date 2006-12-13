package com.kni.etl.util;

import junit.framework.TestCase;


public class DateUtilitiesTest extends TestCase {

    public void testIsSameDay() {
        fail("Not yet implemented");
    }

    public void testIsSameInstant() {
        fail("Not yet implemented");
    }

    public void testParseDate() {
        fail("Not yet implemented");
    }

    public void testAddYears() {
        fail("Not yet implemented");
    }

    public void testAddMonths() {
        fail("Not yet implemented");
    }

    public void testAddWeeks() {
        fail("Not yet implemented");
    }

    public void testAddDays() {
        fail("Not yet implemented");
    }

    public void testAddHours() {
        fail("Not yet implemented");
    }

    public void testAddMinutes() {
        fail("Not yet implemented");
    }

    public void testAddSeconds() {
        fail("Not yet implemented");
    }

    public void testAddMilliseconds() {
        fail("Not yet implemented");
    }

    public void testAdd() {
        fail("Not yet implemented");
    }

    public void testRound() {
        fail("Not yet implemented");
    }

    public void testTruncateDateInt() {
        fail("Not yet implemented");
    }

    public void testTruncateDate() throws InterruptedException {
        
        java.util.Date date1 = new java.util.Date();
        Thread.sleep(200);
        java.util.Date date2 = new java.util.Date();
        java.util.Date date3 = DateUtilities.truncate(date1);
        java.util.Date date4 = DateUtilities.truncate(date2);
        
        if(date3.equals(date4))
            System.out.println("Matched " + date3 + " and " + date4);
        else
            fail("Dates did not match");
    }

}
