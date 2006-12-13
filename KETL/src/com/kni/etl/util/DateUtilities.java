/*
 * Copyright  2002-2005 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.kni.etl.util;

import java.text.ChoiceFormat;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.TimeZone;

/**
 * Helper methods to deal with date/time formatting with a specific defined format (<a
 * href="http://www.w3.org/TR/NOTE-datetime">ISO8601</a>) or a plurialization correct elapsed time in minutes and
 * seconds.
 * 
 * @since Ant 1.5
 */
public final class DateUtilities {

    /**
     * The UTC time zone (often referred to as GMT).
     */
    public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("GMT");
    /**
     * Number of milliseconds in a standard second.
     * 
     * @since 2.1
     */
    public static final long MILLIS_PER_SECOND = 1000;
    /**
     * Number of milliseconds in a standard minute.
     * 
     * @since 2.1
     */
    public static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
    /**
     * Number of milliseconds in a standard hour.
     * 
     * @since 2.1
     */
    public static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
    /**
     * Number of milliseconds in a standard day.
     * 
     * @since 2.1
     */
    public static final long MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

    /**
     * This is half a month, so this represents whether a date is in the top or bottom half of the month.
     */
    public final static int SEMI_MONTH = 1001;

    private static final int[][] fields = { { Calendar.MILLISECOND }, { Calendar.SECOND }, { Calendar.MINUTE },
            { Calendar.HOUR_OF_DAY, Calendar.HOUR }, { Calendar.DATE, Calendar.DAY_OF_MONTH, Calendar.AM_PM
            /* Calendar.DAY_OF_YEAR, Calendar.DAY_OF_WEEK, Calendar.DAY_OF_WEEK_IN_MONTH */
            }, { Calendar.MONTH, SEMI_MONTH }, { Calendar.YEAR }, { Calendar.ERA } };

    /**
     * A week range, starting on Sunday.
     */
    public final static int RANGE_WEEK_SUNDAY = 1;

    /**
     * A week range, starting on Monday.
     */
    public final static int RANGE_WEEK_MONDAY = 2;

    /**
     * A week range, starting on the day focused.
     */
    public final static int RANGE_WEEK_RELATIVE = 3;

    /**
     * A week range, centered around the day focused.
     */
    public final static int RANGE_WEEK_CENTER = 4;

    /**
     * A month range, the week starting on Sunday.
     */
    public final static int RANGE_MONTH_SUNDAY = 5;

    /**
     * A month range, the week starting on Monday.
     */
    public final static int RANGE_MONTH_MONDAY = 6;

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Checks if two date objects are on the same day ignoring time.
     * </p>
     * <p>
     * 28 Mar 2002 13:45 and 28 Mar 2002 06:01 would return true. 28 Mar 2002 13:45 and 12 Mar 2002 13:45 would return
     * false.
     * </p>
     * 
     * @param date1 the first date, not altered, not null
     * @param date2 the second date, not altered, not null
     * @return true if they represent the same day
     * @throws IllegalArgumentException if either date is <code>null</code>
     * @since 2.1
     */
    public static boolean isSameDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            return false;
        }
        return truncate(date1).equals(truncate(date2));

    }

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Checks if two date objects represent the same instant in time.
     * </p>
     * <p>
     * This method compares the long millisecond time of the two objects.
     * </p>
     * 
     * @param date1 the first date, not altered, not null
     * @param date2 the second date, not altered, not null
     * @return true if they represent the same millisecond instant
     * @throws IllegalArgumentException if either date is <code>null</code>
     * @since 2.1
     */
    public static boolean isSameInstant(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            return false;
        }
        return date1.getTime() == date2.getTime();
    }

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Parses a string representing a date by trying a variety of different parsers.
     * </p>
     * <p>
     * The parse will try each parse pattern in turn. A parse is only deemed sucessful if it parses the whole of the
     * input string. If no parse patterns match, a ParseException is thrown.
     * </p>
     * 
     * @param str the date to parse, not null
     * @param parsePatterns the date format patterns to use, see SimpleDateFormat, not null
     * @return the parsed date
     * @throws IllegalArgumentException if the date string or pattern array is null
     * @throws ParseException if none of the date patterns were suitable
     */
    public static Date parseDate(String str, String[] parsePatterns) throws ParseException {
        if (str == null || parsePatterns == null) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);
        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0]);
            }
            else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if (date != null && pos.getIndex() == str.length()) {
                return date;
            }
        }
        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of years to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addYears(Date date, int amount) {
        return add(date, Calendar.YEAR, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of months to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addMonths(Date date, int amount) {
        return add(date, Calendar.MONTH, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of weeks to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addWeeks(Date date, int amount) {
        return add(date, Calendar.WEEK_OF_YEAR, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of days to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addDays(Date date, int amount) {
        return add(date, Calendar.DAY_OF_MONTH, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of hours to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addHours(Date date, int amount) {
        return add(date, Calendar.HOUR_OF_DAY, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of minutes to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addMinutes(Date date, int amount) {
        return add(date, Calendar.MINUTE, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of seconds to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addSeconds(Date date, int amount) {
        return add(date, Calendar.SECOND, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds a number of milliseconds to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date addMilliseconds(Date date, int amount) {
        return add(date, Calendar.MILLISECOND, amount);
    }

    // -----------------------------------------------------------------------
    /**
     * Adds to a date returning a new object. The original date object is unchanged.
     * 
     * @param date the date, not null
     * @param calendarField the calendar field to add to
     * @param amount the amount to add, may be negative
     * @return the new date object with the amount added
     * @throws IllegalArgumentException if the date is null
     */
    public static Date add(Date date, int calendarField, int amount) {
        if (date == null) {
            return null;
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(calendarField, amount);
        return c.getTime();
    }

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Round this date, leaving the field specified as the most significant field.
     * </p>
     * <p>
     * For example, if you had the datetime of 28 Mar 2002 13:45:01.231, if this was passed with HOUR, it would return
     * 28 Mar 2002 14:00:00.000. If this was passed with MONTH, it would return 1 April 2002 0:00:00.000.
     * </p>
     * <p>
     * For a date in a timezone that handles the change to daylight saving time, rounding to Calendar.HOUR_OF_DAY will
     * behave as follows. Suppose daylight saving time begins at 02:00 on March 30. Rounding a date that crosses this
     * time would produce the following values:
     * <ul>
     * <li>March 30, 2003 01:10 rounds to March 30, 2003 01:00</li>
     * <li>March 30, 2003 01:40 rounds to March 30, 2003 03:00</li>
     * <li>March 30, 2003 02:10 rounds to March 30, 2003 03:00</li>
     * <li>March 30, 2003 02:40 rounds to March 30, 2003 04:00</li>
     * </ul>
     * </p>
     * 
     * @param date the date to work with
     * @param field the field from <code>Calendar</code> or <code>SEMI_MONTH</code>
     * @return the rounded date
     * @throws IllegalArgumentException if the date is <code>null</code>
     * @throws ArithmeticException if the year is over 280 million
     */
    public static Date round(Date date, int field) {
        if (date == null) {
            return null;
        }
        Calendar gval = Calendar.getInstance();
        gval.setTime(date);
        modify(gval, field, true);
        return gval.getTime();
    }

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Truncate this date, leaving the field specified as the most significant field.
     * </p>
     * <p>
     * For example, if you had the datetime of 28 Mar 2002 13:45:01.231, if you passed with HOUR, it would return 28 Mar
     * 2002 13:00:00.000. If this was passed with MONTH, it would return 1 Mar 2002 0:00:00.000.
     * </p>
     * 
     * @param date the date to work with
     * @param field the field from <code>Calendar</code> or <code>SEMI_MONTH</code>
     * @return the rounded date
     * @throws IllegalArgumentException if the date is <code>null</code>
     * @throws ArithmeticException if the year is over 280 million
     */
    public static Date truncate(Date date, int field) {
        if (date == null) {
            return null;
        }
        Calendar gval = Calendar.getInstance();
        gval.setTime(date);
        modify(gval, field, false);
        return gval.getTime();
    }
    
    public static Date truncate(Date date) {
        return truncate(date,Calendar.DATE);
    }

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Internal calculation method.
     * </p>
     * 
     * @param val the calendar
     * @param field the field constant
     * @param round true to round, false to truncate
     * @throws ArithmeticException if the year is over 280 million
     */
    private static void modify(Calendar val, int field, boolean round) {
        if (val.get(Calendar.YEAR) > 280000000) {
            throw new ArithmeticException("Calendar value too large for accurate calculations");
        }

        if (field == Calendar.MILLISECOND) {
            return;
        }

        // ----------------- Fix for LANG-59 ---------------------- START ---------------
        // see http://issues.apache.org/jira/browse/LANG-59
        //
        // Manually truncate milliseconds, seconds and minutes, rather than using
        // Calendar methods.

        Date date = val.getTime();
        long time = date.getTime();
        boolean done = false;

        // truncate milliseconds
        int millisecs = val.get(Calendar.MILLISECOND);
        if (!round || millisecs < 500) {
            time = time - millisecs;
            if (field == Calendar.SECOND) {
                done = true;
            }
        }

        // truncate seconds
        int seconds = val.get(Calendar.SECOND);
        if (!done && (!round || seconds < 30)) {
            time = time - (seconds * 1000L);
            if (field == Calendar.MINUTE) {
                done = true;
            }
        }

        // truncate minutes
        int minutes = val.get(Calendar.MINUTE);
        if (!done && (!round || minutes < 30)) {
            time = time - (minutes * 60000L);
        }

        // reset time
        if (date.getTime() != time) {
            date.setTime(time);
            val.setTime(date);
        }
        // ----------------- Fix for LANG-59 ----------------------- END ----------------

        boolean roundUp = false;
        for (int i = 0; i < fields.length; i++) {
            for (int j = 0; j < fields[i].length; j++) {
                if (fields[i][j] == field) {
                    // This is our field... we stop looping
                    if (round && roundUp) {
                        if (field == SEMI_MONTH) {
                            // This is a special case that's hard to generalize
                            // If the date is 1, we round up to 16, otherwise
                            // we subtract 15 days and add 1 month
                            if (val.get(Calendar.DATE) == 1) {
                                val.add(Calendar.DATE, 15);
                            }
                            else {
                                val.add(Calendar.DATE, -15);
                                val.add(Calendar.MONTH, 1);
                            }
                        }
                        else {
                            // We need at add one to this field since the
                            // last number causes us to round up
                            val.add(fields[i][0], 1);
                        }
                    }
                    return;
                }
            }
            // We have various fields that are not easy roundings
            int offset = 0;
            boolean offsetSet = false;
            // These are special types of fields that require different rounding rules
            switch (field) {
            case SEMI_MONTH:
                if (fields[i][0] == Calendar.DATE) {
                    // If we're going to drop the DATE field's value,
                    // we want to do this our own way.
                    // We need to subtrace 1 since the date has a minimum of 1
                    offset = val.get(Calendar.DATE) - 1;
                    // If we're above 15 days adjustment, that means we're in the
                    // bottom half of the month and should stay accordingly.
                    if (offset >= 15) {
                        offset -= 15;
                    }
                    // Record whether we're in the top or bottom half of that range
                    roundUp = offset > 7;
                    offsetSet = true;
                }
                break;
            case Calendar.AM_PM:
                if (fields[i][0] == Calendar.HOUR_OF_DAY) {
                    // If we're going to drop the HOUR field's value,
                    // we want to do this our own way.
                    offset = val.get(Calendar.HOUR_OF_DAY);
                    if (offset >= 12) {
                        offset -= 12;
                    }
                    roundUp = offset > 6;
                    offsetSet = true;
                }
                break;
            }
            if (!offsetSet) {
                int min = val.getActualMinimum(fields[i][0]);
                int max = val.getActualMaximum(fields[i][0]);
                // Calculate the offset from the minimum allowed value
                offset = val.get(fields[i][0]) - min;
                // Set roundUp if this is more than half way between the minimum and maximum
                roundUp = offset > ((max - min) / 2);
            }
            // We need to remove this field
            if (offset != 0) {
                val.set(fields[i][0], val.get(fields[i][0]) - offset);
            }
        }
        throw new IllegalArgumentException("The field " + field + " is not supported");

    }

    // -------------------------------------------------------------------------
    // Deprecated int constants
    // TODO: Remove in 3.0

    /**
     * Number of milliseconds in a standard second.
     * 
     * @deprecated Use MILLIS_PER_SECOND. This will be removed in Commons Lang 3.0.
     */
    public static final int MILLIS_IN_SECOND = 1000;
    /**
     * Number of milliseconds in a standard minute.
     * 
     * @deprecated Use MILLIS_PER_MINUTE. This will be removed in Commons Lang 3.0.
     */
    public static final int MILLIS_IN_MINUTE = 60 * 1000;
    /**
     * Number of milliseconds in a standard hour.
     * 
     * @deprecated Use MILLIS_PER_HOUR. This will be removed in Commons Lang 3.0.
     */
    public static final int MILLIS_IN_HOUR = 60 * 60 * 1000;
    /**
     * Number of milliseconds in a standard day.
     * 
     * @deprecated Use MILLIS_PER_DAY. This will be removed in Commons Lang 3.0.
     */
    public static final int MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
}
