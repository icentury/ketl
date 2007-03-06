/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.sql.SQLException;
import java.util.Date;

/**
 * Insert the type's description here. Creation date: (8/22/2006 1:50:01 PM)
 * 
 * @author: dnguyen
 */
public class ETLJobSchedule {

    private int Month;
    private int MonthOfYear;
    private int Day;
    private int DayOfWeek;
    private int DayOfMonth;
    private int Hour;
    private int HourOfDay;
    private int Minute;
    private int MinuteOfHour;
    private String Description;
    private Date OnceOnlyDate;
    private Date EnableDate;
    private Date DisableDate;
    private boolean isValidated = false;

    public int getMonth() {
        return Month;
    }

    public int getMonthOfYear() {
        return MonthOfYear;
    }

    public int getDay() {
        return Day;
    }

    public int getDayOfWeek() {
        return DayOfWeek;
    }

    public int getDayOfMonth() {
        return DayOfMonth;
    }

    public int getHour() {
        return Hour;
    }

    public int getHourOfDay() {
        return HourOfDay;
    }

    public int getMinute() {
        return Minute;
    }

    public int getMinuteOfHour() {
        return MinuteOfHour;
    }

    public String getDescription() {
        return Description;
    }

    public Date getOnceOnlyDate() {
        return OnceOnlyDate;
    }

    public Date getEnableDate() {
        return EnableDate;
    }

    public Date getDisableDate() {
        return DisableDate;
    }

    public boolean isScheduleValidated() {
        return isValidated;
    }

    /**
     * ETLJobStatus constructor comment.
     */
    public ETLJobSchedule(int pMonth, int pMonthOfYear, int pDay, int pDayOfWeek, int pDayOfMonth, int pHour,
            int pHourOfDay, int pMinute, int pMinuteOfHour, String pDescription, Date pOnceOnlyDate, Date pEnableDate,
            Date pDisableDate) {
        Month = pMonth;
        MonthOfYear = pMonthOfYear;
        Day = pDay;
        DayOfWeek = pDayOfWeek;
        DayOfMonth = pDayOfMonth;
        Hour = pHour;
        HourOfDay = pHourOfDay;
        Minute = pMinute;
        MinuteOfHour = pMinuteOfHour;
        Description = pDescription;
        OnceOnlyDate = pOnceOnlyDate;
        EnableDate = pEnableDate;
        DisableDate = pDisableDate;
    }

    /**
     * @return Returns errors if it's invalid.
     * @author dnguyen 2006-08-22
     */
    public String validateSchedule() {
        try {
            setDefaults();
            isValidated = true;
            return ""; // i.e. no errors
        } catch (Exception ex) {
            return ex.getMessage();
        }
    }

    private void setDefaults() throws SQLException, java.lang.Exception {

        // if pOnceOnlyDate is set, then no other increments should be set.
        if (OnceOnlyDate != null) {
            Month = -1;
            Day = -1;
            Hour = -1;
            Minute = -1;
            MonthOfYear = -1;
            DayOfMonth = -1;
            DayOfWeek = -1;
            HourOfDay = -1;
            MinuteOfHour = -1;
            return;
        }

        // if pMonthOfYear is set (Jan, Feb, Mar, etc.) then the increment is pMonth=12 (every year)
        if (MonthOfYear >= 0) {
            if (MonthOfYear > 11)
                throw new Exception("Invalid job schedule value: Month-Of-Year values are 0-11 for Jan-Dec.");
            if ((MonthOfYear >= 0) && (Month > 0))
                throw new Exception(
                        "Invalid job schedule combination: Only the Month-Increment or Month-Of-Year should be set.");
            else
                Month = 12;
        }
        // if pDayOfMonth is set (1st, 5th day of the month, etc.) and the month increment is not set, then default to 1
        // (every month)
        if (DayOfMonth > 0) {
            if (DayOfMonth > 31)
                throw new Exception("Invalid job schedule value: Day-Of-Month values are 1-31.");
            else if ((Day > 0) || (DayOfWeek > 0))
                throw new Exception(
                        "Invalid job schedule combination: Only the Day-Increment or Day-Of-Month or Day-Of-Week should be set.");
            else if (Month <= 0)
                Month = 1; // if (pMonth >= 0) this is fine. Eg.every 3 months on the x day of the month
        }

        // if pDayOfWeek is set (Sun, Mon, etc.), and no month recurrance was set, then this is every week (vs. the
        // first weekday of the month)
        if (DayOfWeek >= 0) {
            if (DayOfWeek > 6)
                throw new Exception("Invalid job schedule value: Day-Of-Week values are 1-7 for Sun-Sat.");
            else if ((Day > 0) || (DayOfMonth > 0))
                throw new Exception(
                        "Invalid job schedule combination: Only the Day-Increment or Day-Of-Month or Day-Of-Week should be set.");
            else if ((Month <= 0)) // if (pMonth >= 0), every 2 months on the first Wednesday.
                Day = 7; // every Friday means set Day increment = 7
        }

        // if pHourOfDay is set and no day recurrance was set, then default to every day
        if (HourOfDay >= 0) {
            if (HourOfDay > 23)
                throw new Exception("Invalid job schedule value: Hour-Of-Day values are 0-23.");
            else if (Hour > 0)
                throw new Exception(
                        "Invalid job schedule combination: Only the Hour-Increment or Hour-Of-Day should be set.");
            else if ((Day <= 0) && (DayOfMonth <= 0) && (DayOfWeek <= 0))
                Day = 1;
        }

        // if pMinuteOfHour is set and no hour recurrance was set, then default to every hour
        if (MinuteOfHour >= 0) {
            if (MinuteOfHour > 59)
                throw new Exception("Invalid job schedule value: Minute-of-Hour values are 0-59.");
            else if (Minute > 0)
                throw new Exception(
                        "Invalid job schedule combination: Only the Minute-Increment or Minute-of-Hour should be set.");
            else if ((Hour <= 0) && (HourOfDay <= 0))
                Hour = 1;
        }
    }
}
