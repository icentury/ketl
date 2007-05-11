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
        return this.Month;
    }

    public int getMonthOfYear() {
        return this.MonthOfYear;
    }

    public int getDay() {
        return this.Day;
    }

    public int getDayOfWeek() {
        return this.DayOfWeek;
    }

    public int getDayOfMonth() {
        return this.DayOfMonth;
    }

    public int getHour() {
        return this.Hour;
    }

    public int getHourOfDay() {
        return this.HourOfDay;
    }

    public int getMinute() {
        return this.Minute;
    }

    public int getMinuteOfHour() {
        return this.MinuteOfHour;
    }

    public String getDescription() {
        return this.Description;
    }

    public Date getOnceOnlyDate() {
        return this.OnceOnlyDate;
    }

    public Date getEnableDate() {
        return this.EnableDate;
    }

    public Date getDisableDate() {
        return this.DisableDate;
    }

    public boolean isScheduleValidated() {
        return this.isValidated;
    }

    /**
     * ETLJobStatus constructor comment.
     */
    public ETLJobSchedule(int pMonth, int pMonthOfYear, int pDay, int pDayOfWeek, int pDayOfMonth, int pHour,
            int pHourOfDay, int pMinute, int pMinuteOfHour, String pDescription, Date pOnceOnlyDate, Date pEnableDate,
            Date pDisableDate) {
        this.Month = pMonth;
        this.MonthOfYear = pMonthOfYear;
        this.Day = pDay;
        this.DayOfWeek = pDayOfWeek;
        this.DayOfMonth = pDayOfMonth;
        this.Hour = pHour;
        this.HourOfDay = pHourOfDay;
        this.Minute = pMinute;
        this.MinuteOfHour = pMinuteOfHour;
        this.Description = pDescription;
        this.OnceOnlyDate = pOnceOnlyDate;
        this.EnableDate = pEnableDate;
        this.DisableDate = pDisableDate;
    }

    /**
     * @return Returns errors if it's invalid.
     * @author dnguyen 2006-08-22
     */
    public String validateSchedule() {
        try {
            this.setDefaults();
            this.isValidated = true;
            return ""; // i.e. no errors
        } catch (Exception ex) {
            return ex.getMessage();
        }
    }

    private void setDefaults() throws SQLException, java.lang.Exception {

        // if pOnceOnlyDate is set, then no other increments should be set.
        if (this.OnceOnlyDate != null) {
            this.Month = -1;
            this.Day = -1;
            this.Hour = -1;
            this.Minute = -1;
            this.MonthOfYear = -1;
            this.DayOfMonth = -1;
            this.DayOfWeek = -1;
            this.HourOfDay = -1;
            this.MinuteOfHour = -1;
            return;
        }

        // if pMonthOfYear is set (Jan, Feb, Mar, etc.) then the increment is pMonth=12 (every year)
        if (this.MonthOfYear >= 0) {
            if (this.MonthOfYear > 11)
                throw new Exception("Invalid job schedule value: Month-Of-Year values are 0-11 for Jan-Dec.");
            if ((this.MonthOfYear >= 0) && (this.Month > 0))
                throw new Exception(
                        "Invalid job schedule combination: Only the Month-Increment or Month-Of-Year should be set.");
            else
                this.Month = 12;
        }
        // if pDayOfMonth is set (1st, 5th day of the month, etc.) and the month increment is not set, then default to 1
        // (every month)
        if (this.DayOfMonth > 0) {
            if (this.DayOfMonth > 31)
                throw new Exception("Invalid job schedule value: Day-Of-Month values are 1-31.");
            else if ((this.Day > 0) || (this.DayOfWeek > 0))
                throw new Exception(
                        "Invalid job schedule combination: Only the Day-Increment or Day-Of-Month or Day-Of-Week should be set.");
            else if (this.Month <= 0)
                this.Month = 1; // if (pMonth >= 0) this is fine. Eg.every 3 months on the x day of the month
        }

        // if pDayOfWeek is set (Sun, Mon, etc.), and no month recurrance was set, then this is every week (vs. the
        // first weekday of the month)
        if (this.DayOfWeek >= 0) {
            if (this.DayOfWeek > 6)
                throw new Exception("Invalid job schedule value: Day-Of-Week values are 1-7 for Sun-Sat.");
            else if ((this.Day > 0) || (this.DayOfMonth > 0))
                throw new Exception(
                        "Invalid job schedule combination: Only the Day-Increment or Day-Of-Month or Day-Of-Week should be set.");
            else if ((this.Month <= 0)) // if (pMonth >= 0), every 2 months on the first Wednesday.
                this.Day = 7; // every Friday means set Day increment = 7
        }

        // if pHourOfDay is set and no day recurrance was set, then default to every day
        if (this.HourOfDay >= 0) {
            if (this.HourOfDay > 23)
                throw new Exception("Invalid job schedule value: Hour-Of-Day values are 0-23.");
            else if (this.Hour > 0)
                throw new Exception(
                        "Invalid job schedule combination: Only the Hour-Increment or Hour-Of-Day should be set.");
            else if ((this.Day <= 0) && (this.DayOfMonth <= 0) && (this.DayOfWeek <= 0))
                this.Day = 1;
        }

        // if pMinuteOfHour is set and no hour recurrance was set, then default to every hour
        if (this.MinuteOfHour >= 0) {
            if (this.MinuteOfHour > 59)
                throw new Exception("Invalid job schedule value: Minute-of-Hour values are 0-59.");
            else if (this.Minute > 0)
                throw new Exception(
                        "Invalid job schedule combination: Only the Minute-Increment or Minute-of-Hour should be set.");
            else if ((this.Hour <= 0) && (this.HourOfDay <= 0))
                this.Hour = 1;
        }
    }
}
