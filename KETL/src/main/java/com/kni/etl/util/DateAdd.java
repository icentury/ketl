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

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Calendar;
import java.util.Date;

// TODO: Auto-generated Javadoc
/**
 * The Class DateAdd.
 */
public class DateAdd {

    /** The ABBREVS. */
    static String[] ABBREVS = { "dy", "wk", "yr", "hr", "s", "m" };
    
    /** The ABBRE v_ TYPES. */
    static int[] ABBREV_TYPES = { Calendar.DAY_OF_MONTH, Calendar.WEEK_OF_YEAR, Calendar.YEAR, Calendar.HOUR_OF_DAY,
            Calendar.SECOND, Calendar.MINUTE };
    
    /** The vals. */
    int[] vals = new int[DateAdd.ABBREV_TYPES.length];

    /** The work cal. */
    Calendar mWorkCal = Calendar.getInstance();

    /**
     * Instantiates a new date add.
     * 
     * @param arg0 the arg0
     * 
     * @throws Exception the exception
     */
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

    /**
     * Increment.
     * 
     * @param arg0 the arg0
     * 
     * @return the date
     */
    public Date increment(Date arg0) {
        this.mWorkCal.setTime(arg0);
        for (int x = 0; x < DateAdd.ABBREVS.length; x++) {
            this.mWorkCal.add(DateAdd.ABBREV_TYPES[x], this.vals[x]);
        }

        return this.mWorkCal.getTime();
    }

}