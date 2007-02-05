/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 14, 2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.kni.etl.stringtools;

import java.math.BigDecimal;

import com.kni.etl.ketl.qa.QAEventGenerator;


/**
 * @author nicholas.wakefield
 *
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
public class NumberFormatter
{
    final static String[] units = { "YB", "ZB", "EB", "PB", "TB", "GB", "MB", "KB", "bytes" };
    final static java.text.DecimalFormat formatter = new java.text.DecimalFormat("##.##");

    // long can't actually hold that much (2^64 < 10^20)
    public static String format(long s)
    {
        for (int i = 0; i < units.length; i++)
        {
            if (s >= Math.pow(1024, units.length - i - 1))
            {
                return formatter.format((s / Math.pow(1024, units.length - i - 1))) + units[i];
            }
        }

        return "?b";
    }
    
    public static int convertToBytes(String size) {
        if (size == null) {
            return -1;
        }

        try {
            return new BigDecimal(size).intValue();
        } catch (NumberFormatException e) {
            // couldn't convert directly therefore check for abbreviations
            for (int i = 1; i < abrevs.length; i++) {
                String tmp = size.substring(size.length() - abrevs[i].length());

                if (tmp.equalsIgnoreCase(abrevs[i])) {
                    try {
                        tmp = size.substring(0, size.length() - abrevs[i].length());

                        return new BigDecimal(tmp).multiply(sizes[i]).intValue();
                    } catch (NumberFormatException e1) {
                        return -1;
                    }
                }
            }
        }

        return -1;
    }
    
    private final static BigDecimal[] sizes = { new BigDecimal("1"), new BigDecimal("1024"), new BigDecimal("1048576"),
        new BigDecimal("1073741824"), new BigDecimal("1099511627776") };

    public final static String[] abrevs = { null, "k", "mb", "gb", "tb" };
    
}
