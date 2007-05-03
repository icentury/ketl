/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jun 1, 2006
 * 
 */
package com.kni.etl.util;

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Calendar;
import java.util.Date;

public class DateAdd {

    static String[] ABBREVS = { "dy", "wk", "yr", "hr", "s", "m" };
    static int[] ABBREV_TYPES = { Calendar.DAY_OF_MONTH, Calendar.WEEK_OF_YEAR, Calendar.YEAR, Calendar.HOUR_OF_DAY,
            Calendar.SECOND, Calendar.MINUTE };
    int[] vals = new int[DateAdd.ABBREV_TYPES.length];

    Calendar mWorkCal = Calendar.getInstance();

    public DateAdd(String arg0) throws Exception {
        String[] res = arg0.split(",");

        for (String element : res) {
            NumberFormat nmf = NumberFormat.getIntegerInstance();
            ParsePosition pos = new ParsePosition(0);
            Number num = nmf.parse(element, pos);
            String type = element.substring(pos.getIndex());
            boolean match = false;
            for (int x = 0; x < DateAdd.ABBREVS.length; x++) {
                if (DateAdd.ABBREVS[x].equalsIgnoreCase(type)) {
                    this.vals[x] = num.intValue();
                    match = true;
                }

            }
            if (match == false) {
                throw new Exception("DateAdd string invalid, field " + element + " contains invalid pattern");
            }

        }
    }

    public Date increment(Date arg0) {
        this.mWorkCal.setTime(arg0);
        for (int x = 0; x < DateAdd.ABBREVS.length; x++) {
            this.mWorkCal.add(DateAdd.ABBREV_TYPES[x], this.vals[x]);
        }

        return this.mWorkCal.getTime();
    }

}