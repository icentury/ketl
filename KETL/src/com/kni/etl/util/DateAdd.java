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
    int[] vals = new int[ABBREV_TYPES.length];

    Calendar mWorkCal = Calendar.getInstance();

    public DateAdd(String arg0) throws Exception {
        String[] res = arg0.split(",");

        for (int i = 0; i < res.length; i++) {
            NumberFormat nmf = NumberFormat.getIntegerInstance();
            ParsePosition pos = new ParsePosition(0);
            Number num = nmf.parse(res[i], pos);
            String type = res[i].substring(pos.getIndex());
            boolean match = false;
            for (int x = 0; x < ABBREVS.length; x++) {
                if (ABBREVS[x].equalsIgnoreCase(type)) {
                    vals[x] = num.intValue();
                    match = true;
                }

            }
            if (match == false) {
                throw new Exception("DateAdd string invalid, field " + res[i] + " contains invalid pattern");
            }

        }
    }

    public Date increment(Date arg0) {
        mWorkCal.setTime(arg0);
        for (int x = 0; x < ABBREVS.length; x++) {
            mWorkCal.add(ABBREV_TYPES[x], vals[x]);
        }

        return mWorkCal.getTime();
    }

}