/*
 * Modified by Kinetic Networks, Inc. The code should be given
 * back to Sun as it performs substantially faster than the standard
 * date format class.
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

public class FastSimpleDateFormat extends DateFormat {

    /**
     * Cache to hold the DateTimePatterns of a Locale.
     */
    private static Hashtable cachedLocaleData = new Hashtable(3);

    // the internal serial version which says which version was written
    // - 0 (default) for version up to JDK 1.1.3
    // - 1 for version from JDK 1.1.4, which includes a new field
    static final int currentSerialVersion = 1;
    private static final String GMT = "GMT";
    private static final String GMT_MINUS = "GMT-";

    // For time zones that have no names, use strings GMT+minutes and
    // GMT-minutes. For instance, in France the time zone is GMT+60.
    private static final String GMT_PLUS = "GMT+";
    private static final int millisPerHour = 60 * 60 * 1000;
    private static final int millisPerMinute = 60 * 1000;

    private static final int NANOSECOND = 20;
    private static final int EPOCH = 21;
    // Map index into pattern character string to Calendar field number
    private static final int[] PATTERN_INDEX_TO_CALENDAR_FIELD = { Calendar.ERA, Calendar.YEAR, Calendar.MONTH,
            Calendar.DATE, Calendar.HOUR_OF_DAY, Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND,
            Calendar.MILLISECOND, Calendar.DAY_OF_WEEK, Calendar.DAY_OF_YEAR, Calendar.DAY_OF_WEEK_IN_MONTH,
            Calendar.WEEK_OF_YEAR, Calendar.WEEK_OF_MONTH, Calendar.AM_PM, Calendar.HOUR, Calendar.HOUR,
            Calendar.ZONE_OFFSET, Calendar.ZONE_OFFSET, Calendar.ZONE_OFFSET, NANOSECOND, EPOCH };

    // Map index into pattern character string to DateFormat field number
    private static final int[] PATTERN_INDEX_TO_DATE_FORMAT_FIELD = { DateFormat.ERA_FIELD, DateFormat.YEAR_FIELD,
            DateFormat.MONTH_FIELD, DateFormat.DATE_FIELD, DateFormat.HOUR_OF_DAY1_FIELD,
            DateFormat.HOUR_OF_DAY0_FIELD, DateFormat.MINUTE_FIELD, DateFormat.SECOND_FIELD,
            DateFormat.MILLISECOND_FIELD, DateFormat.DAY_OF_WEEK_FIELD, DateFormat.DAY_OF_YEAR_FIELD,
            DateFormat.DAY_OF_WEEK_IN_MONTH_FIELD, DateFormat.WEEK_OF_YEAR_FIELD, DateFormat.WEEK_OF_MONTH_FIELD,
            DateFormat.AM_PM_FIELD, DateFormat.HOUR1_FIELD, DateFormat.HOUR0_FIELD, DateFormat.TIMEZONE_FIELD,
            DateFormat.TIMEZONE_FIELD, 18, 19, 20 };

    // 19 is for Nanoseconds
    static final String patternChars = "GyMdkHmsSEDFwWahKzZTNe";

    // the official serial version ID which says cryptically
    // which version we're compatible with
    static final long serialVersionUID = 4774881970558875024L;
    private String[] valueCacheIndex = null;
    private int[] valueCachePositionResult = null;
    private int[] valueCacheTimeResult = null;
    private Date defaultCenturyStart;
    private transient int defaultCenturyStartYear;
    private DateFormatSymbols formatData;
    private String pattern;
    private int serialVersionOnStream = currentSerialVersion;

    public FastSimpleDateFormat() {
        this(SHORT, SHORT + 4, Locale.getDefault());
    }

    /* Package-private, called by DateFormat factory methods */
    FastSimpleDateFormat(int timeStyle, int dateStyle, Locale loc) {
        /* try the cache first */
        String[] dateTimePatterns = (String[]) cachedLocaleData.get(loc);

        if (dateTimePatterns == null) { /* cache miss */

            ResourceBundle r = ResourceBundle.getBundle("java.text.resources.LocaleElements", loc);
            dateTimePatterns = r.getStringArray("DateTimePatterns");

            /* update cache */
            cachedLocaleData.put(loc, dateTimePatterns);
        }

        formatData = new DateFormatSymbols(loc);

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

        initialize(loc);
    }

    public FastSimpleDateFormat(String pattern) {
        this(pattern, Locale.getDefault());
    }

    public FastSimpleDateFormat(String pattern, DateFormatSymbols formatData) {
        this.applyPattern(pattern);
        this.formatData = (DateFormatSymbols) formatData.clone();
        initialize(Locale.getDefault());
    }

    public FastSimpleDateFormat(String pattern, Locale loc) {
        this.applyPattern(pattern);
        this.formatData = new DateFormatSymbols(loc);
        initialize(loc);
    }

    private int miPatternLength;

    /**
     * Apply the given localized pattern string to this date format.
     */
    /**
     * Apply the given unlocalized pattern string to this date format.
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
    public StringBuffer format(Date arg0, StringBuffer arg1, FieldPosition arg2) {
        // TODO Auto-generated method stub
        return null;
    }

    public Date get2DigitYearStart() {
        return defaultCenturyStart;
    }

    public DateFormatSymbols getDateFormatSymbols() {
        return (DateFormatSymbols) formatData.clone();
    }

    public int hashCode() {
        return pattern.hashCode();

        // just enough fields for a reasonable distribution
    }

    /* Initialize calendar and numberFormat fields */
    private void initialize(Locale loc) {
        // The format object must be constructed using the symbols for this zone.
        // However, the calendar should use the current default TimeZone.
        // If this is not contained in the locale zone strings, then the zone
        // will be formatted using generic GMT+/-H:MM nomenclature.
        calendar = Calendar.getInstance(TimeZone.getDefault(), loc);
        numberFormat = NumberFormat.getInstance(loc);
        numberFormat.setGroupingUsed(false);

        if (numberFormat instanceof DecimalFormat) {
            ((DecimalFormat) numberFormat).setDecimalSeparatorAlwaysShown(false);
        }

        numberFormat.setParseIntegerOnly(true); /* So that dd.MM.yy can be parsed */
        numberFormat.setMinimumFractionDigits(0); // To prevent "Jan 1.00, 1997.00"

        initializeDefaultCentury();
    }

    /*
     * Initialize the fields we use to disambiguate ambiguous years. Separate so we can call it from readObject().
     */
    private void initializeDefaultCentury() {
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR, -80);
        parseAmbiguousDatesAsAfter(calendar.getTime());
    }

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
            setCalendarCacheField(patternCharIndex, field, bestMatch);

            return start + bestMatchLength;
        }

        return -start;
    }

    /**
     * Overrides DateFormat
     * 
     * @see java.text.DateFormat
     */
    public Date parse(String text, ParsePosition pos) {
        int start = pos.getIndex();
        int oldStart = start;
        boolean[] ambiguousYear = { false };

        calendar.clear(); // Clears all the time fields

        boolean inQuote = false; // inQuote set true when hits 1st single quote
        char prevCh = 0;
        int count = 0;
        int interQuoteCount = 1; // Number of chars between quotes

        for (int i = 0; i < this.miPatternLength; ++i) {
            char ch = pattern.charAt(i);

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
                        start = subParse(text, start, prevCh, count, true, ambiguousYear);

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
                        start = subParse(text, start, prevCh, count, true, ambiguousYear);

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
                    start = subParse(text, start, prevCh, count, true, ambiguousYear);

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
            start = subParse(text, start, prevCh, count, false, ambiguousYear);

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
                Calendar savedCalendar = (Calendar) calendar.clone();
                parsedDate = calendar.getTime();

                if (parsedDate.before(defaultCenturyStart)) {
                    // We can't use add here because that does a complete() first.
                    savedCalendar.set(Calendar.YEAR, defaultCenturyStartYear + 100);
                    parsedDate = savedCalendar.getTime();
                }
            }
            else {
                parsedDate = calendar.getTime();
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
    private void parseAmbiguousDatesAsAfter(Date startDate) {
        defaultCenturyStart = startDate;
        calendar.setTime(startDate);
        defaultCenturyStartYear = calendar.get(Calendar.YEAR);
    }

    /**
     * Override readObject.
     */
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();

        if (serialVersionOnStream < 1) {
            // didn't have defaultCenturyStart field
            initializeDefaultCentury();
        }
        else {
            // fill in dependent transient field
            parseAmbiguousDatesAsAfter(defaultCenturyStart);
        }

        serialVersionOnStream = currentSerialVersion;
    }

    public void set2DigitYearStart(Date startDate) {
        parseAmbiguousDatesAsAfter(startDate);
    }

    public void setDateFormatSymbols(DateFormatSymbols newFormatSymbols) {
        this.formatData = (DateFormatSymbols) newFormatSymbols.clone();
    }

    private int setCalendarCache(int pPos, int pCacheStartPos) {
        this.valueCachePositionResult[pPos] = pCacheStartPos;

        return this.valueCachePositionResult[pPos];
    }

    private int setCalendarFromCache(int pField, int pPos) {
        calendar.set(pField, this.valueCacheTimeResult[pPos]);

        return this.valueCachePositionResult[pPos];
    }

    private void setCalendarCacheField(int pPos, int pField, int value) {
        this.valueCacheTimeResult[pPos] = value;

        calendar.set(pField, this.valueCacheTimeResult[pPos]);
    }

    private int subParse(String text, int start, char ch, int count, boolean obeyCount, boolean[] ambiguousYear) {
        Number number;
        int value = 0;
        int i;
        ParsePosition pos = new ParsePosition(0);
        int patternCharIndex = -1;

        if ((patternCharIndex = patternChars.indexOf(ch)) == -1) {
            return -start;
        }

        if (this.valueCacheIndex == null) {
            this.valueCacheIndex = new String[patternChars.length()];
            this.valueCachePositionResult = new int[patternChars.length()];
            this.valueCacheTimeResult = new int[patternChars.length()];
        }

        int field = PATTERN_INDEX_TO_CALENDAR_FIELD[patternCharIndex];

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
                setCalendarFromCache(Calendar.DST_OFFSET, patternCharIndex + 1);

                return setCalendarFromCache(Calendar.ZONE_OFFSET, patternCharIndex);

            default:
                return setCalendarFromCache(field, patternCharIndex);
            }
        }

        this.valueCacheIndex[patternCharIndex] = strSub;

        pos.setIndex(start);

        // If there are any spaces here, skip over them. If we hit the end
        // of the string, then fail.
        int textLen = text.length();

        for (;;) {
            if (pos.getIndex() >= textLen) {
                return setCalendarCache(patternCharIndex, -start);
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
            return setCalendarCache(patternCharIndex, matchString(patternCharIndex, text, start, Calendar.ERA,
                    formatData.getEras()));

        case 1: // 'y' - YEAR
            // If there are 3 or more YEAR pattern characters, this indicates
            // that the year value is to be treated literally, without any
            // two-digit year adjustments (e.g., from "01" to 2001). Otherwise
            // we made adjustments to place the 2-digit year in the proper
            // century, for parsed strings from "00" to "99". Any other string
            // is treated literally: "2250", "-1", "1", "002".

            if ((count <= 2) && ((pos.getIndex() - start) == 2) && Character.isDigit(text.charAt(start))
                    && Character.isDigit(text.charAt(start + 1))) {
                int ambiguousTwoDigitYear = defaultCenturyStartYear % 100;
                ambiguousYear[0] = value == ambiguousTwoDigitYear;
                value += (((defaultCenturyStartYear / 100) * 100) + ((value < ambiguousTwoDigitYear) ? 100 : 0));
            }

            setCalendarCacheField(patternCharIndex, Calendar.YEAR, value);

            return setCalendarCache(patternCharIndex, pos.getIndex() + count);

        case 2: // 'M' - MONTH

            if (count <= 2) // i.e., M or MM.
            {
                // Don't want to parse the month if it is a string
                // while pattern uses numeric style: M or MM.
                // [We computed 'value' above.]
                setCalendarCacheField(patternCharIndex, Calendar.MONTH, value - 1);

                return setCalendarCache(patternCharIndex, pos.getIndex() + count);
            }

            // count >= 3 // i.e., MMM or MMMM
            // Want to be able to parse both short and long forms.
            // Try count == 4 first:
            int newStart = 0;

            if ((newStart = matchString(patternCharIndex, text, start, Calendar.MONTH, formatData.getMonths())) > 0) {
                return setCalendarCache(patternCharIndex, newStart);
            }

            // count == 4 failed, now try count == 3
            return setCalendarCache(patternCharIndex, matchString(patternCharIndex, text, start, Calendar.MONTH,
                    formatData.getShortMonths()));

        case 4:

            // 'k' - HOUR_OF_DAY: 1-based. eg, 23:59 + 1 hour =>> 24:59
            // [We computed 'value' above.]
            if (value == (calendar.getMaximum(Calendar.HOUR_OF_DAY) + 1)) {
                value = 0;
            }

            setCalendarCacheField(patternCharIndex, Calendar.HOUR_OF_DAY, value);

            return setCalendarCache(patternCharIndex, pos.getIndex());

        case 9:

            // 'E' - DAY_OF_WEEK
            newStart = 0;

            if ((newStart = matchString(patternCharIndex, text, start, Calendar.DAY_OF_WEEK, formatData.getWeekdays())) > 0) {
                return setCalendarCache(patternCharIndex, newStart);
            }

            // DDDD failed, now try DDD
            return setCalendarCache(patternCharIndex, matchString(patternCharIndex, text, start, Calendar.DAY_OF_WEEK,
                    formatData.getShortWeekdays()));

        case 14:
            return setCalendarCache(patternCharIndex, matchString(patternCharIndex, text, start, Calendar.AM_PM,
                    formatData.getAmPmStrings()));

        case 15: // 'h' - HOUR:1-based. eg, 11PM + 1 hour =>> 12 AM

            // [We computed 'value' above.]
            if (value == (calendar.getLeastMaximum(Calendar.HOUR) + 1)) {
                value = 0;
            }

            setCalendarCacheField(patternCharIndex, Calendar.HOUR, value);

            return setCalendarCache(patternCharIndex, pos.getIndex());

        case 17: // 'z' - ZONE_OFFSET
        {
            int sign = 0;
            int offset;

            // For time zones that have no known names, look for strings
            // of the form:
            // GMT[+-]hours:minutes or
            // GMT[+-]hhmm or
            // GMT.
            if (((textLen - start) > GMT.length()) && text.regionMatches(true, start, GMT, 0, GMT.length())) {
                setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, 0);

                pos.setIndex(start + GMT.length());

                if (text.charAt(pos.getIndex()) == '+') {
                    sign = 1;
                }
                else if (text.charAt(pos.getIndex()) == '-') {
                    sign = -1;
                }
                else {
                    setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, 0);

                    return setCalendarCache(patternCharIndex, pos.getIndex());
                }

                // Look for hours:minutes or hhmm.
                pos.setIndex(pos.getIndex() + 1);

                // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
                int parseStart = pos.getIndex();
                Number tzNumber = numberFormat.parse(text, pos);

                if ((tzNumber == null) || (pos.getIndex() == parseStart)) {
                    return setCalendarCache(patternCharIndex, -start);
                }

                if (text.charAt(pos.getIndex()) == ':') {
                    // This is the hours:minutes case
                    offset = tzNumber.intValue() * 60;
                    pos.setIndex(pos.getIndex() + 1);

                    // WORK AROUND BUG IN NUMBER FORMAT IN 1.2B3
                    parseStart = pos.getIndex();
                    tzNumber = numberFormat.parse(text, pos);

                    if ((tzNumber == null) || (pos.getIndex() == parseStart)) {
                        return setCalendarCache(patternCharIndex, -start);
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
                for (i = 0; i < formatData.getZoneStrings().length; i++) {
                    // Checking long and short zones [1 & 2],
                    // and long and short daylight [3 & 4].
                    int j = 1;

                    for (; j <= 4; ++j) {
                        if (text.regionMatches(true, start, formatData.getZoneStrings()[i][j], 0, formatData
                                .getZoneStrings()[i][j].length())) {
                            break;
                        }
                    }

                    if (j <= 4) {
                        TimeZone tz = TimeZone.getTimeZone(formatData.getZoneStrings()[i][0]);
                        setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, tz.getRawOffset());

                        // Must call set() with something -- TODO -- Fix this to
                        // use the correct DST SAVINGS for the zone.
                        setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, (j >= 3) ? millisPerHour : 0);

                        return setCalendarCache(patternCharIndex, start + formatData.getZoneStrings()[i][j].length());
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
                    return setCalendarCache(patternCharIndex, -start); // Wasn't actually a number.
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
                offset *= (millisPerMinute * sign);

                if (calendar.getTimeZone().useDaylightTime()) {
                    setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, millisPerHour);

                    offset -= millisPerHour;
                }

                setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, offset);

                return setCalendarCache(patternCharIndex, pos.getIndex());
            }
        }

            // All efforts to parse a zone failed.
            return setCalendarCache(patternCharIndex, -start);

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
                return setCalendarCache(patternCharIndex, -start); // Wasn't actually a number.
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
                offset *= (millisPerMinute * sign);

                if (calendar.getTimeZone().useDaylightTime()) {
                    setCalendarCacheField(patternCharIndex + 1, Calendar.DST_OFFSET, millisPerHour);

                    offset -= millisPerHour;
                }

                setCalendarCacheField(patternCharIndex, Calendar.ZONE_OFFSET, offset);

                return setCalendarCache(patternCharIndex, pos.getIndex());
            }
        }

            // All efforts to parse a zone failed.
            return setCalendarCache(patternCharIndex, -start);

        // 
        case 19:
            throw new RuntimeException("Nanoseconds not supported");
        case 21:
            calendar.setTimeInMillis(Long.parseLong(strSub));

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
                    return setCalendarCache(patternCharIndex, -start);
                }

                number = numberFormat.parse(text.substring(0, start + count), pos);
            }
            else {
                number = numberFormat.parse(text, pos);
            }

            if ((number != null) && (pos.getIndex() != parseStart)) {
                setCalendarCacheField(patternCharIndex, field, number.intValue());

                return setCalendarCache(patternCharIndex, pos.getIndex());
            }

            return setCalendarCache(patternCharIndex, -start);
        }
    }

    /**
     * Return a localized pattern string describing this date format.
     */
    public String toLocalizedPattern() {
        return translatePattern(pattern, patternChars, formatData.getLocalPatternChars());
    }

    /**
     * Return a pattern string describing this date format.
     */
    public String toPattern() {
        return pattern;
    }

    /**
     * Translate a pattern, mapping each character in the from string to the corresponding character in the to string.
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
    private String zeroPaddingNumber(long value, int minDigits, int maxDigits) {
        numberFormat.setMinimumIntegerDigits(minDigits);
        numberFormat.setMaximumIntegerDigits(maxDigits);

        return numberFormat.format(value);
    }
}
