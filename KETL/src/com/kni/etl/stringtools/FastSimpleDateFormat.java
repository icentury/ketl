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
/*  Copyright 1999 Sun Microsystems, Inc. All rights reserved.  */
/*  Copyright 1999 Sun Microsystems, Inc. Tous droits réservés. */

/*
 * @(#)SimpleDateFormat.java    1.43 99/02/01
 *
 * (C) Copyright Taligent, Inc. 1996 - All Rights Reserved
 * (C) Copyright IBM Corp. 1996-1998 - All Rights Reserved
 *
 * Portions copyright (c) 1996-1998 Sun Microsystems, Inc. All Rights Reserved.
 *
 *   The original version of this source code and documentation is copyrighted
 * and owned by Taligent, Inc., a wholly-owned subsidiary of IBM. These
 * materials are provided under terms of a License Agreement between Taligent
 * and Sun. This technology is protected by multiple US and International
 * patents. This notice and attribution to Taligent may not be removed.
 *   Taligent is a registered trademark of Taligent, Inc.
 *
 * Permission to use, copy, modify, and distribute this software
 * and its documentation for NON-COMMERCIAL purposes and without
 * fee is hereby granted provided that this copyright notice
 * appears in all copies. Please refer to the file "copyright.html"
 * for further important copyright and licensing information.
 *
 * SUN MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SUN SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 *
 */
/*
 * Created on May 12, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.stringtools;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.TimeZone;

// TODO: Auto-generated Javadoc
/**
 * The Class FastSimpleDateFormat.
 */
public class FastSimpleDateFormat extends DateFormat {

    /** Cache to hold the DateTimePatterns of a Locale. */
    private static Hashtable cachedLocaleData = new Hashtable(3);

    // the internal serial version which says which version was written
    // - 0 (default) for version up to JDK 1.1.3
    // - 1 for version from JDK 1.1.4, which includes a new field
    /** The Constant currentSerialVersion. */
    static final int currentSerialVersion = 1;
    
    /** The Constant GMT. */
    private static final String GMT = "GMT";
    
    /** The Constant GMT_MINUS. */
    //private static final String GMT_MINUS = "GMT-";

    // For time zones that have no names, use strings GMT+minutes and
    // GMT-minutes. For instance, in France the time zone is GMT+60.
    /** The Constant GMT_PLUS. */
    //private static final String GMT_PLUS = "GMT+";
    
    /** The Constant millisPerHour. */
    private static final int millisPerHour = 60 * 60 * 1000;
    
    /** The Constant millisPerMinute. */
    private static final int millisPerMinute = 60 * 1000;

    /** The Constant NANOSECOND. */
    private static final int NANOSECOND = 20;
    
    /** The Constant EPOCH. */
    private static final int EPOCH = 21;
    // Map index into pattern character string to Calendar field number
    /** The Constant PATTERN_INDEX_TO_CALENDAR_FIELD. */
    private static final int[] PATTERN_INDEX_TO_CALENDAR_FIELD = { Calendar.ERA, Calendar.YEAR, Calendar.MONTH,
            Calendar.DATE, Calendar.HOUR_OF_DAY, Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND,
            Calendar.MILLISECOND, Calendar.DAY_OF_WEEK, Calendar.DAY_OF_YEAR, Calendar.DAY_OF_WEEK_IN_MONTH,
            Calendar.WEEK_OF_YEAR, Calendar.WEEK_OF_MONTH, Calendar.AM_PM, Calendar.HOUR, Calendar.HOUR,
            Calendar.ZONE_OFFSET, Calendar.ZONE_OFFSET, Calendar.ZONE_OFFSET, FastSimpleDateFormat.NANOSECOND, FastSimpleDateFormat.EPOCH };

    // Map index into pattern character string to DateFormat field number
    /** The Constant PATTERN_INDEX_TO_DATE_FORMAT_FIELD. */
    /*private static final int[] PATTERN_INDEX_TO_DATE_FORMAT_FIELD = { DateFormat.ERA_FIELD, DateFormat.YEAR_FIELD,
            DateFormat.MONTH_FIELD, DateFormat.DATE_FIELD, DateFormat.HOUR_OF_DAY1_FIELD,
            DateFormat.HOUR_OF_DAY0_FIELD, DateFormat.MINUTE_FIELD, DateFormat.SECOND_FIELD,
            DateFormat.MILLISECOND_FIELD, DateFormat.DAY_OF_WEEK_FIELD, DateFormat.DAY_OF_YEAR_FIELD,
            DateFormat.DAY_OF_WEEK_IN_MONTH_FIELD, DateFormat.WEEK_OF_YEAR_FIELD, DateFormat.WEEK_OF_MONTH_FIELD,
            DateFormat.AM_PM_FIELD, DateFormat.HOUR1_FIELD, DateFormat.HOUR0_FIELD, DateFormat.TIMEZONE_FIELD,
            DateFormat.TIMEZONE_FIELD, 18, 19, 20 };
     */
    // 19 is for Nanoseconds
    /** The Constant patternChars. */
    static final String patternChars = "GyMdkHmsSEDFwWahKzZTNe";

    // the official serial version ID which says cryptically
    // which version we're compatible with
    /** The Constant serialVersionUID. */
    static final long serialVersionUID = 4774881970558875024L;
    
    /** The value cache index. */
    private String[] valueCacheIndex = null;
    
    /** The value cache position result. */
    private int[] valueCachePositionResult = null;
    
    /** The value cache time result. */
    private int[] valueCacheTimeResult = null;
    
    /** The default century start. */
    private Date defaultCenturyStart;
    
    /** The default century start year. */
    private transient int defaultCenturyStartYear;
    
    /** The format data. */
    private DateFormatSymbols formatData;
    
    /** The pattern. */
    private String pattern;
    
    /** The serial version on stream. */
    private int serialVersionOnStream = FastSimpleDateFormat.currentSerialVersion;

    /**
     * Instantiates a new fast simple date format.
     */
    public FastSimpleDateFormat() {
        this(DateFormat.SHORT, DateFormat.SHORT + 4, Locale.getDefault());
    }

    /* Package-private, called by DateFormat factory methods */
    /**
     * Instantiates a new fast simple date format.
     * 
     * @param timeStyle the time style
     * @param dateStyle the date style
     * @param loc the loc
     */
    FastSimpleDateFormat(int timeStyle, int dateStyle, Locale loc) {
        /* try the cache first */
        String[] dateTimePatterns = (String[]) FastSimpleDateFormat.cachedLocaleData.get(loc);

        if (dateTimePatterns == null) { /* cache miss */

            ResourceBundle r = ResourceBundle.getBundle("java.text.resources.LocaleElements", loc);
            dateTimePatterns = r.getStringArray("DateTimePatterns");

            /* update cache */
            FastSimpleDateFormat.cachedLocaleData.put(loc, dateTimePatterns);
        }

        this.formatData = new DateFormatSymbols(loc);

        if ((timeStyle >= 0) && (dateStyle >= 0)) {
            Object[] dateTimeArgs = { dateTimePatterns[timeStyle], dateTimePatterns[dateStyle] };
            this.applyPattern(MessageFormat.format(dateTimePatterns[8], dateTimeArgs));
        }
        else if (timeStyle >= 0) {
            this.applyPattern(dateTimePatterns[timeStyle]);
        }
        else if (dateStyle >= 0) {
            this.applyPattern(dateTimePatterns[dateStyle]);
        }
        else {
            throw new IllegalArgumentException("No date or time style specified");
        }

        this.initialize(loc);
    }

    /**
     * Instantiates a new fast simple date format.
     * 
     * @param pattern the pattern
     */
    public FastSimpleDateFormat(String pattern) {
        this(pattern, Locale.getDefault());
    }

    /**
     * Instantiates a new fast simple date format.
     * 
     * @param pattern the pattern
     * @param formatData the format data
     */
    public FastSimpleDateFormat(String pattern, DateFormatSymbols formatData) {
        this.applyPattern(pattern);
        this.formatData = (DateFormatSymbols) formatData.clone();
        this.initialize(Locale.getDefault());
    }

    /**
     * Instantiates a new fast simple date format.
     * 
     * @param pattern the pattern
     * @param loc the loc
     */
    public FastSimpleDateFormat(String pattern, Locale loc) {
        this.applyPattern(pattern);
        this.formatData = new DateFormatSymbols(loc);
        this.initialize(loc);
    }

    /** The mi pattern length. */
    private int miPatternLength;

    /**
     * Apply the given localized pattern string to this date format.
     */
    /**
     * Apply the given unlocalized pattern string to this date format.
     * 
     * @param pattern the pattern
     */
    public void applyPattern(String pattern) {
        this.pattern = pattern;
        this.miPatternLength = this.pattern.length();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.text.DateFormat#format(java.util.Date, java.lang.StringBuffer, java.text.FieldPosition)
     */
    @Override
    public StringBuffer format(Date arg0, StringBuffer arg1, FieldPosition arg2) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Gets the 2 digit year start.
     * 
     * @return the 2 digit year start
     */
    public Date get2DigitYearStart() {
        return this.defaultCenturyStart;
    }

    /**
     * Gets the date format symbols.
     * 
     * @return the date format symbols
     */
    public DateFormatSymbols getDateFormatSymbols() {
        return (DateFormatSymbols) this.formatData.clone();
    }

    /* (non-Javadoc)
     * @see java.text.DateFormat#hashCode()
     */
    @Override
    public int hashCode() {
        return this.pattern.hashCode();

        // just enough fields for a reasonable distribution
    }

    /* Initialize calendar and numberFormat fields */
    /**
     * Initialize.
     * 
     * @param loc the loc
     */
    private void initialize(Locale loc) {
        // The format object must be constructed using the symbols for this zone.
        // However, the calendar should use the current default TimeZone.
        // If this is not contained in the locale zone strings, then the zone
        // will be formatted using generic GMT+/-H:MM nomenclature.
        this.calendar = Calendar.getInstance(TimeZone.getDefault(), loc);
        this.numberFormat = NumberFormat.getInstance(loc);
        this.numberFormat.setGroupingUsed(false);

        if (this.numberFormat instanceof DecimalFormat) {
            ((DecimalFormat) this.numberFormat).setDecimalSeparatorAlwaysShown(false);
        }

        this.numberFormat.setParseIntegerOnly(true); /* So that dd.MM.yy can be parsed */
        this.numberFormat.setMinimumFractionDigits(0); // To prevent "Jan 1.00, 1997.00"

        this.initializeDefaultCentury();
    }

    /*
     * Initialize the fields we use to disambiguate ambiguous years. Separate so we can call it from readObject().
     */
    /**
     * Initialize default century.
     */
    private void initializeDefaultCentury() {
        this.calendar.setTime(new Date());
        this.calendar.add(Calendar.YEAR, -80);
        this.parseAmbiguousDatesAsAfter(this.calendar.getTime());
    }

    /**
     * Match string.
     * 
     * @param patternCharIndex the pattern char index
     * @param text the text
     * @param start the start
     * @param field the field
     * @param data the data
     * 
     * @return the int
     */
    private int matchString(int patternCharIndex, String text, int start, int field, String[] data) {
        int i = 0;
        int count = data.length;

        if (field == Calendar.DAY_OF_WEEK) {
            i = 1;
        }

        // There may be multiple strings in the data[] array which begin with
        // the same prefix (e.g., Cerven and Cervenec (June and July) in Czech).
        // We keep track of the longest match, and return that. Note that this
        // unfortunately requires us to test all array elements.
        int bestMatchLength = 0;

        // There may be multiple strings in the data[] array which begin with
        // the same prefix (e.g., Cerven and Cervenec (June and July) in Czech).
        // We keep track of the longest match, and return that. Note that this
        // unfortunately requires us to test all array elements.
        int bestMatch = -1;

        for (; i < count; ++i) {
            int length = data[i].length();

            // Always compare if we have no match yet; otherwise only compare
            // against potentially better matches (longer strings).
            if ((length > bestMatchLength) && text.regionMatches(true, start, data[i], 0, length)) {
                bestMatch = i;
                bestMatchLength = length;
            }
        }

        if (bestMatch >= 0) {
            this.setCalendarCacheField(patternCharIndex, field, bestMatch);

            return start + bestMatchLength;
        }

        return -start;
    }

    /**
     * Overrides DateFormat.
     * 
     * @param text the text
     * @param pos the pos
     * 
     * @return the date
     * 
     * @see java.text.DateFormat
     */
    @Override
    public Date parse(String text, ParsePosition pos) {
        int start = pos.getIndex();
        int oldStart = start;
        boolean[] ambiguousYear = { false };

        this.calendar.clear(); // Clears all the time fields

        boolean inQuote = false; // inQuote set true when hits 1st single quote
        char prevCh = 0;
        int count = 0;
        int interQuoteCount = 1; // Number of chars between quotes

        for (int i = 0; i < this.miPatternLength; ++i) {
            char ch = this.pattern.charAt(i);

            if (inQuote) {
                if (ch == '\'') {
                    // ends with 2nd single quote
                    inQuote = false;

                    // two consecutive quotes outside a quote means we have
                    // a quote literal we need to match.
                    if (count == 0) {
                        if ((start >= text.length()) || (ch != text.charAt(start))) {
                            pos.setIndex(oldStart);
                            pos.setErrorIndex(start);

                            return null;
                        }

                        ++start;
                    }

                    count = 0;
                    interQuoteCount = 0;
                }
                else {
                    // pattern uses text following from 1st single quote.
                    if ((start >= text.length()) || (ch != text.charAt(start))) {
                        // Check for cases like: 'at' in pattern vs "xt"
                        // in time text, where 'a' doesn't match with 'x'.
                        // If fail to match, return null.
                        pos.setIndex(oldStart); // left unchanged
                        pos.setErrorIndex(start);

                        return null;
                    }

                    ++count;
                    ++start;
                }
            }
            else // !inQuote
            {
                if (ch == '\'') {
                    inQuote = true;

                    if (count > 0) // handle cases like: e'at'
                    {
                        int startOffset = start;
                        start = this.subParse(text, start, prevCh, count, true, ambiguousYear);

                        if (start < 0) {
                            pos.setErrorIndex(startOffset);
                            pos.setIndex(oldStart);

                            return null;
                        }

                        count = 0;
                    }

                    if (interQuoteCount == 0) {
                        // This indicates two consecutive quotes inside a quote,
                        // for example, 'o''clock'. We need to parse this as
                        // representing a single quote within the quote.
                        int startOffset = start;

                        if ((start >= text.length()) || (ch != text.charAt(start))) {
                            pos.setErrorIndex(startOffset);
                            pos.setIndex(oldStart);

                            return null;
                        }

                        ++start;
                        count = 1; // Make it look like we never left
                    }
                }
                else if (((ch >= 'a') && (ch <= 'z')) || ((ch >= 'A') && (ch <= 'Z'))) {
                    // ch is a date-time pattern
                    if ((ch != prevCh) && (count > 0)) // e.g., yyyyMMdd
                    {
                        int startOffset = start;

                        // This is the only case where we pass in 'true' for
                        // obeyCount. That's because the next field directly
                        // abuts this one, so we have to use the count to know when
                        // to stop parsing. [LIU]
                        start = this.subParse(text, start, prevCh, count, true, ambiguousYear);

                        if (start < 0) {
                            pos.setErrorIndex(startOffset);
                            pos.setIndex(oldStart);

                            return null;
                        }

                        prevCh = ch;
                        count = 1;
                    }
                    else {
                        if (ch != prevCh) {
                            prevCh = ch;
                        }

                        count++;
                    }
                }
                else if (count > 0) {
                    // handle cases like: MM-dd-yy, HH:mm:ss, or yyyy MM dd,
                    // where ch = '-', ':', or ' ', repectively.
                    int startOffset = start;
                    start = this.subParse(text, start, prevCh, count, true, ambiguousYear);

                    if (start < 0) {
                        pos.setErrorIndex(startOffset);
                        pos.setIndex(oldStart);

                        return null;
                    }

                    if ((start >= text.length()) || (ch != text.charAt(start))) {
                        // handle cases like: 'MMMM dd' in pattern vs. "janx20"
                        // in time text, where ' ' doesn't match with 'x'.
                        pos.setErrorIndex(start);
                        pos.setIndex(oldStart);

                        return null;
                    }

                    start++;
                    count = 0;
                    prevCh = 0;
                }
                else // any other unquoted characters
                {
                    if ((start >= text.length()) || (ch != text.charAt(start))) {
                        // handle cases like: 'MMMM dd' in pattern vs.
                        // "jan,,,20" in time text, where " " doesn't
                        // match with ",,,".
                        pos.setErrorIndex(start);
                        pos.setIndex(oldStart);

                        return null;
                    }

                    start++;
                }

                ++interQuoteCount;
            }
        }

        // Parse the last item in the pattern
        if (count > 0) {
            int startOffset = start;
            start = this.subParse(text, start, prevCh, count, false, ambiguousYear);

            if (start < 0) {
                pos.setIndex(oldStart);
                pos.setErrorIndex(startOffset);

                return null;
            }
        }

        // At this point the fields of Calendar have been set. Calendar
        // will fill in default values for missing fields when the time
        // is computed.
        pos.setIndex(start);

        Date parsedDate;

        try {
            if (ambiguousYear[0]) {
                Calendar savedCalendar = (Calendar) this.calendar.clone();
                parsedDate = this.calendar.getTime();

                if (parsedDate.before(this.defaultCenturyStart)) {
                    // We can't use add here because that does a complete() first.
                    savedCalendar.set(Calendar.YEAR, this.defaultCenturyStartYear + 100);
                    parsedDate = savedCalendar.getTime();
                }
            }
            else {
                parsedDate = this.calendar.getTime();
            }
        }

        // An IllegalArgumentException will be thrown by Calendar.getTime()
        // if any fields are out of range, e.g., MONTH == 17.
        catch (IllegalArgumentException e) {
            pos.setErrorIndex(start);
            pos.setIndex(oldStart);

            return null;
        }

        return parsedDate;
    }

    /*
     * Define one-century window into which to disambiguate dates using two-digit years.
     */
    /**
     * Parses the ambiguous dates as after.
     * 
     * @param startDate the start date
     */
    private void parseAmbiguousDatesAsAfter(Date startDate) {
        this.defaultCenturyStart = startDate;
        this.calendar.setTime(startDate);
        this.defaultCenturyStartYear = this.calendar.get(Calendar.YEAR);
    }

    /**
     * Override readObject.
     * 
     * @param stream the stream
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();

        if (this.serialVersionOnStream < 1) {
            // didn't have defaultCenturyStart field
            this.initializeDefaultCentury();
        }
        else {
            // fill in dependent transient field
            this.parseAmbiguousDatesAsAfter(this.defaultCenturyStart);
        }

        this.serialVersionOnStream = FastSimpleDateFormat.currentSerialVersion;
    }

    /**
     * Sets the 2 digit year start.
     * 
     * @param startDate the new 2 digit year start
     */
    public void set2DigitYearStart(Date startDate) {
        this.parseAmbiguousDatesAsAfter(startDate);
    }

    /**
     * Sets the date format symbols.
     * 
     * @param newFormatSymbols the new date format symbols
     */
    public void setDateFormatSymbols(DateFormatSymbols newFormatSymbols) {
        this.formatData = (DateFormatSymbols) newFormatSymbols.clone();
    }

    /**
     * Sets the calendar cache.
     * 
     * @param pPos the pos
     * @param pCacheStartPos the cache start pos
     * 
     * @return the int
     */
    private int setCalendarCache(int pPos, int pCacheStartPos) {
        this.valueCachePositionResult[pPos] = pCacheStartPos;

        return this.valueCachePositionResult[pPos];
    }

    /**
     * Sets the calendar from cache.
     * 
     * @param pField the field
     * @param pPos the pos
     * 
     * @return the int
     */
    private int setCalendarFromCache(int pField, int pPos) {
        this.calendar.set(pField, this.valueCacheTimeResult[pPos]);

        return this.valueCachePositionResult[pPos];
    }

    /**
     * Sets the calendar cache field.
     * 
     * @param pPos the pos
     * @param pField the field
     * @param value the value
     */
    private void setCalendarCacheField(int pPos, int pField, int value) {
        this.valueCacheTimeResult[pPos] = value;

        this.calendar.set(pField, this.valueCacheTimeResult[pPos]);
    }

    /**
     * Sub parse.
     * 
     * @param text the text
     * @param start the start
     * @param ch the ch
     * @param count the count
     * @param obeyCount the obey count
     * @param ambiguousYear the ambiguous year
     * 
     * @return the int
     */
    private int subParse(String text, int start, char ch, int count, boolean obeyCount, boolean[] ambiguousYear) {
        Number number;
        int value = 0;
        int i;
        ParsePosition pos = new ParsePosition(0);
        int patternCharIndex = -1;

        if ((patternCharIndex = FastSimpleDateFormat.patternChars.indexOf(ch)) == -1) {
            return -start;
        }

        if (this.valueCacheIndex == null) {
            this.valueCacheIndex = new String[FastSimpleDateFormat.patternChars.length()];
            this.valueCachePositionResult = new int[FastSimpleDateFormat.patternChars.length()];
            this.valueCacheTimeResult = new int[FastSimpleDateFormat.patternChars.length()];
        }

        int field = FastSimpleDateFormat.PATTERN_INDEX_TO_CALENDAR_FIELD[patternCharIndex];

        String strSub = null;

        if (obeyCount) {
            strSub = text.substring(start, start + count);
        }
        else {
            strSub = text.substring(start);
        }

        if ((this.valueCacheIndex[patternCharIndex] != null)
                && (this.valueCachePositionResult[patternCharIndex] == start + count)
                && (this.valueCacheIndex[patternCharIndex].equals(strSub))) {
            switch (patternCharIndex) {
            case 17: // 'z' - ZONE_OFFSET
            case 18: // 'Z' - HOUR_OFFSET
                this.setCalendarFromCache(Calendar.DST_OFFSET, patternCharIndex + 1);

                return this.setCalendarFromCache(Calendar.ZONE_OFFSET, patternCharIndex);

            default:
                return this.setCalendarFromCache(field, patternCharIndex);
            }
        }

        this.valueCacheIndex[patternCharIndex] = strSub;

        pos.setIndex(start);

        // If there are any spaces here, skip over them. If we hit the end
        // of the string, then fail.
        int textLen = text.length();

        for (;;) {
            if (pos.getIndex() >= textLen) {
                return this.setCalendarCache(patternCharIndex, -start);
            }

            char c = text.charAt(pos.getIndex());

            if ((c != ' ') && (c != '\t')) {
                break;
            }

            pos.setIndex(pos.getIndex() + 1);
        }

        if ((patternCharIndex == 4) /* HOUR_OF_DAY1_FIELD */|| (patternCharIndex == 15) /* HOUR1_FIELD */
                || ((patternCharIndex == 2) /* MONTH_FIELD */&& (count <= 2)) || (patternCharIndex == 1) /* YEAR */
             ) {
            // int parseStart = pos.getIndex(); // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
            value = Integer.parseInt(strSub);
        }

        switch (patternCharIndex) {
        case 0: // 'G' - ERA
            return this.setCalendarCache(patternCharIndex, this.matchString(patternCharIndex, text, start, Calendar.ERA,
                    this.formatData.getEras()));

        case 1: // 'y' - YEAR
            // If there are 3 or more YEAR pattern characters, this indicates
            // that the year value is to be treated literally, without any
            // two-digit year adjustments (e.g., from "01" to 2001). Otherwise
            // we made adjustments to place the 2-digit year in the proper
            // century, for parsed strings from "00" to "99". Any other string
            // is treated literally: "2250", "-1", "1", "002".

            if ((count <= 2) && ((pos.getIndex() - start) == 2) && Character.isDigit(text.charAt(start))
                    && Character.isDigit(text.charAt(start + 1))) {
                int ambiguousTwoDigitYear = this.defaultCenturyStartYear % 100;
                ambiguousYear[0] = value == ambiguousTwoDigitYear;
                value += (((this.defaultCenturyStartYear / 100) * 100) + ((value < ambiguousTwoDigitYear) ? 100 : 0));
            }

            this.setCalendarCacheField(patternCharIndex, Calendar.YEAR, value);

            return this.setCalendarCache(patternCharIndex, pos.getIndex() + count);

        case 2: // 'M' - MONTH

            if (count <= 2) // i.e., M or MM.
            {
                // Don't want to parse the month if it is a string
                // while pattern uses numeric style: M or MM.
                // [We computed 'value' above.]
                this.setCalendarCacheField(patternCharIndex, Calendar.MONTH, value - 1);

                return this.setCalendarCache(patternCharIndex, pos.getIndex() + count);
            }

            // count >= 3 // i.e., MMM or MMMM
            // Want to be able to parse both short and long forms.
            // Try count == 4 first:
            int newStart = 0;

            if ((newStart = this.matchString(patternCharIndex, text, start, Calendar.MONTH, this.formatData.getMonths())) > 0) {
                return this.setCalendarCache(patternCharIndex, newStart);
            }

            // count == 4 failed, now try count == 3
            return this.setCalendarCache(patternCharIndex, this.matchString(patternCharIndex, text, start, Calendar.MONTH,
                    this.formatData.getShortMonths()));

        case 4:

            // 'k' - HOUR_OF_DAY: 1-based. eg, 23:59 + 1 hour =>> 24:59
            // [We computed 'value' above.]
            if (value == (this.calendar.getMaximum(Calendar.HOUR_OF_DAY) + 1)) {
                value = 0;
            }

            this.setCalendarCacheField(patternCharIndex, Calendar.HOUR_OF_DAY, value);

            return this.setCalendarCache(patternCharIndex, pos.getIndex());

        case 9:

            // 'E' - DAY_OF_WEEK
            newStart = 0;

            if ((newStart = this.matchString(patternCharIndex, text, start, Calendar.DAY_OF_WEEK, this.formatData.getWeekdays())) > 0) {
                return this.setCalendarCache(patternCharIndex, newStart);
            }

            // DDDD failed, now try DDD
            return this.setCalendarCache(patternCharIndex, this.matchString(patternCharIndex, text, start, Calendar.DAY_OF_WEEK,
                    this.formatData.getShortWeekdays()));

        case 14:
            return this.setCalendarCache(patternCharIndex, this.matchString(patternCharIndex, text, start, Calendar.AM_PM,
                    this.formatData.getAmPmStrings()));

        case 15: // 'h' - HOUR:1-based. eg, 11PM + 1 hour =>> 12 AM

            // [We computed 'value' above.]
            if (value == (this.calendar.getLeastMaximum(Calendar.HOUR) + 1)) {
                value = 0;
            }

            this.setCalendarCacheField(patternCharIndex, Calendar.HOUR, value);

            return this.setCalendarCache(patternCharIndex, pos.getIndex());

        case 17: // 'z' - ZONE_OFFSET
        {
            int sign = 0;
            int offset;

            // For time zones that have no known names, look for strings
            // of the form:
            // GMT[+-]hours:minutes or
            // GMT[+-]hhmm or
            // GMT.
            if (((textLen - start) > FastSimpleDateFormat.GMT.length()) && text.regionMatches(true, start, FastSimpleDateFormat.GMT, 0, FastSimpleDateFormat.GMT.length())) {
                this.setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, 0);

                pos.setIndex(start + FastSimpleDateFormat.GMT.length());

                if (text.charAt(pos.getIndex()) == '+') {
                    sign = 1;
                }
                else if (text.charAt(pos.getIndex()) == '-') {
                    sign = -1;
                }
                else {
                    this.setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, 0);

                    return this.setCalendarCache(patternCharIndex, pos.getIndex());
                }

                // Look for hours:minutes or hhmm.
                pos.setIndex(pos.getIndex() + 1);

                // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
                int parseStart = pos.getIndex();
                Number tzNumber = this.numberFormat.parse(text, pos);

                if ((tzNumber == null) || (pos.getIndex() == parseStart)) {
                    return this.setCalendarCache(patternCharIndex, -start);
                }

                if (text.charAt(pos.getIndex()) == ':') {
                    // This is the hours:minutes case
                    offset = tzNumber.intValue() * 60;
                    pos.setIndex(pos.getIndex() + 1);

                    // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
                    parseStart = pos.getIndex();
                    tzNumber = this.numberFormat.parse(text, pos);

                    if ((tzNumber == null) || (pos.getIndex() == parseStart)) {
                        return this.setCalendarCache(patternCharIndex, -start);
                    }

                    offset += tzNumber.intValue();
                }
                else {
                    // This is the hhmm case.
                    offset = tzNumber.intValue();

                    if (offset < 24) {
                        offset *= 60;
                    }
                    else {
                        offset = (offset % 100) + (offset / 100 * 60);
                    }
                }

                // Fall through for final processing below of 'offset' and 'sign'.
            }
            else {
                // At this point, check for named time zones by looking through
                // the locale data from the DateFormatZoneData strings.
                // Want to be able to parse both short and long forms.
                for (i = 0; i < this.formatData.getZoneStrings().length; i++) {
                    // Checking long and short zones [1 & 2],
                    // and long and short daylight [3 & 4].
                    int j = 1;

                    for (; j <= 4; ++j) {
                        if (text.regionMatches(true, start, this.formatData.getZoneStrings()[i][j], 0, this.formatData
                                .getZoneStrings()[i][j].length())) {
                            break;
                        }
                    }

                    if (j <= 4) {
                        TimeZone tz = TimeZone.getTimeZone(this.formatData.getZoneStrings()[i][0]);
                        this.setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, tz.getRawOffset());

                        // Must call set() with something -- TODO -- Fix this to
                        // use the correct DST SAVINGS for the zone.
                        this.setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, (j >= 3) ? FastSimpleDateFormat.millisPerHour : 0);

                        return this.setCalendarCache(patternCharIndex, start + this.formatData.getZoneStrings()[i][j].length());
                    }
                }

                // As a last resort, look for numeric timezones of the form
                // [+-]hhmm as specified by RFC 822. This code is actually
                // a little more permissive than RFC 822. It will try to do
                // its best with numbers that aren't strictly 4 digits long.
                DecimalFormat fmt = new DecimalFormat("+####;-####");
                fmt.setParseIntegerOnly(true);

                // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
                int parseStart = pos.getIndex();
                Number tzNumber = fmt.parse(text, pos);

                if ((tzNumber == null) || (pos.getIndex() == parseStart)) {
                    return this.setCalendarCache(patternCharIndex, -start); // Wasn't actually a number.
                }

                offset = tzNumber.intValue();
                sign = 1;

                if (offset < 0) {
                    sign = -1;
                    offset = -offset;
                }

                if (offset < 24) {
                    offset = offset * 60;
                }
                else {
                    offset = (offset % 100) + (offset / 100 * 60);
                }

                // Fall through for final processing below of 'offset' and 'sign'.
            }

            // Do the final processing for both of the above cases. We only
            // arrive here if the form GMT+/-... or an RFC 822 form was seen.
            if (sign != 0) {
                offset *= (FastSimpleDateFormat.millisPerMinute * sign);

                if (this.calendar.getTimeZone().useDaylightTime()) {
                    this.setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, FastSimpleDateFormat.millisPerHour);

                    offset -= FastSimpleDateFormat.millisPerHour;
                }

                this.setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, offset);

                return this.setCalendarCache(patternCharIndex, pos.getIndex());
            }
        }

            // All efforts to parse a zone failed.
            return this.setCalendarCache(patternCharIndex, -start);

        case 18: // 'Z' - HOUR_OFFSET
        {
            int sign = 0;
            int offset;

            // Look for numeric timezones of the form
            // [+-]hhmm as specified by RFC 822. This code is actually
            // a little more permissive than RFC 822. It will try to do
            // its best with numbers that aren't strictly 4 digits long.
            DecimalFormat fmt = new DecimalFormat("+####;-####");
            fmt.setParseIntegerOnly(true);

            // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
            int parseStart = pos.getIndex();
            Number tzNumber = fmt.parse(text, pos);

            if ((tzNumber == null) || (pos.getIndex() == parseStart)) {
                return this.setCalendarCache(patternCharIndex, -start); // Wasn't actually a number.
            }

            offset = tzNumber.intValue();
            sign = 1;

            if (offset < 0) {
                sign = -1;
                offset = -offset;
            }

            if (offset < 24) {
                offset = offset * 60;
            }
            else {
                offset = (offset % 100) + (offset / 100 * 60);
            }

            // Fall through for final processing below of 'offset' and 'sign'.
            // Do the final processing for both of the above cases. We only
            // arrive here if the form GMT+/-... or an RFC 822 form was seen.
            if (sign != 0) {
                offset *= (FastSimpleDateFormat.millisPerMinute * sign);

                if (this.calendar.getTimeZone().useDaylightTime()) {
                    this.setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, FastSimpleDateFormat.millisPerHour);

                    offset -= FastSimpleDateFormat.millisPerHour;
                }

                this.setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, offset);

                return this.setCalendarCache(patternCharIndex, pos.getIndex());
            }
        }

            // All efforts to parse a zone failed.
            return this.setCalendarCache(patternCharIndex, -start);

        // 
        case 19:
            throw new RuntimeException("Nanoseconds not supported");
        case 21:
            this.calendar.setTimeInMillis(Long.parseLong(strSub));

            return strSub.length();
        default:

            // case 3: // 'd' - DATE
            // case 5: // 'H' - HOUR_OF_DAY:0-based. eg, 23:59 + 1 hour =>> 00:59
            // case 6: // 'm' - MINUTE
            // case 7: // 's' - SECOND
            // case 8: // 'S' - MILLISECOND
            // case 10: // 'D' - DAY_OF_YEAR
            // case 11: // 'F' - DAY_OF_WEEK_IN_MONTH
            // case 12: // 'w' - WEEK_OF_YEAR
            // case 13: // 'W' - WEEK_OF_MONTH
            // case 16: // 'K' - HOUR: 0-based. eg, 11PM + 1 hour =>> 0 AM
            // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
            int parseStart = pos.getIndex();

            // Handle "generic" fields
            if (obeyCount) {
                if ((start + count) > textLen) {
                    return this.setCalendarCache(patternCharIndex, -start);
                }

                number = this.numberFormat.parse(text.substring(0, start + count), pos);
            }
            else {
                number = this.numberFormat.parse(text, pos);
            }

            if ((number != null) && (pos.getIndex() != parseStart)) {
                this.setCalendarCacheField(patternCharIndex, field, number.intValue());

                return this.setCalendarCache(patternCharIndex, pos.getIndex());
            }

            return this.setCalendarCache(patternCharIndex, -start);
        }
    }

    /**
     * Return a localized pattern string describing this date format.
     * 
     * @return the string
     */
    public String toLocalizedPattern() {
        return this.translatePattern(this.pattern, FastSimpleDateFormat.patternChars, this.formatData.getLocalPatternChars());
    }

    /**
     * Return a pattern string describing this date format.
     * 
     * @return the string
     */
    public String toPattern() {
        return this.pattern;
    }

    /**
     * Translate a pattern, mapping each character in the from string to the corresponding character in the to string.
     * 
     * @param pattern the pattern
     * @param from the from
     * @param to the to
     * 
     * @return the string
     */
    private String translatePattern(String pattern, String from, String to) {
        StringBuffer result = new StringBuffer();
        boolean inQuote = false;

        for (int i = 0; i < pattern.length(); ++i) {
            char c = pattern.charAt(i);

            if (inQuote) {
                if (c == '\'') {
                    inQuote = false;
                }
            }
            else {
                if (c == '\'') {
                    inQuote = true;
                }
                else if (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z'))) {
                    int ci = from.indexOf(c);

                    if (ci == -1) {
                        throw new IllegalArgumentException("Illegal pattern " + " character '" + c + "'");
                    }

                    c = to.charAt(ci);
                }
            }

            result.append(c);
        }

        if (inQuote) {
            throw new IllegalArgumentException("Unfinished quote in pattern");
        }

        return result.toString();
    }

    // Pad the shorter numbers up to maxCount digits.
    /**
     * Zero padding number.
     * 
     * @param value the value
     * @param minDigits the min digits
     * @param maxDigits the max digits
     * 
     * @return the string
     */
    @SuppressWarnings("unused")
    private String zeroPaddingNumber(long value, int minDigits, int maxDigits) {
        this.numberFormat.setMinimumIntegerDigits(minDigits);
        this.numberFormat.setMaximumIntegerDigits(maxDigits);

        return this.numberFormat.format(value);
    }
}
