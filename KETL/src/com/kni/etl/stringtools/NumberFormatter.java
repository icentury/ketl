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
/*
 * Created on Jul 14, 2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.kni.etl.stringtools;

import java.math.BigDecimal;


// TODO: Auto-generated Javadoc
/**
 * The Class NumberFormatter.
 * 
 * @author nicholas.wakefield
 * 
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
public class NumberFormatter
{
    
    /** The Constant units. */
    final static String[] units = { "YB", "ZB", "EB", "PB", "TB", "GB", "MB", "KB", "bytes" };
    
    /** The Constant formatter. */
    final static java.text.DecimalFormat formatter = new java.text.DecimalFormat("##.##");

    // long can't actually hold that much (2^64 < 10^20)
    /**
     * Format.
     * 
     * @param s the s
     * 
     * @return the string
     */
    public static String format(long s)
    {
        for (int i = 0; i < NumberFormatter.units.length; i++)
        {
            if (s >= Math.pow(1024, NumberFormatter.units.length - i - 1))
            {
                return NumberFormatter.formatter.format((s / Math.pow(1024, NumberFormatter.units.length - i - 1))) + NumberFormatter.units[i];
            }
        }

        return "?b";
    }
    
    /**
     * Convert to bytes.
     * 
     * @param size the size
     * 
     * @return the int
     */
    public static int convertToBytes(String size) {
        if (size == null) {
            return -1;
        }

        try {
            return new BigDecimal(size).intValue();
        } catch (NumberFormatException e) {
            // couldn't convert directly therefore check for abbreviations
            for (int i = 1; i < NumberFormatter.abrevs.length; i++) {
                String tmp = size.substring(size.length() - NumberFormatter.abrevs[i].length());

                if (tmp.equalsIgnoreCase(NumberFormatter.abrevs[i])) {
                    try {
                        tmp = size.substring(0, size.length() - NumberFormatter.abrevs[i].length());

                        return new BigDecimal(tmp).multiply(NumberFormatter.sizes[i]).intValue();
                    } catch (NumberFormatException e1) {
                        return -1;
                    }
                }
            }
        }

        return -1;
    }
    
    /** The Constant sizes. */
    private final static BigDecimal[] sizes = { new BigDecimal("1"), new BigDecimal("1024"), new BigDecimal("1048576"),
        new BigDecimal("1073741824"), new BigDecimal("1099511627776") };

    /** The Constant abrevs. */
    public final static String[] abrevs = { null, "k", "mb", "gb", "tb" };
    
}
