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
 * Created on Nov 5, 2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.util;

import java.util.ArrayList;
import java.util.StringTokenizer;

import com.kni.etl.dbutils.ResourcePool;

// TODO: Auto-generated Javadoc
/**
 * The Class ArgumentParserUtil.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class ArgumentParserUtil {

    /**
     * Split quote aware.
     * 
     * @param strToSplit the str to split
     * 
     * @return the string[]
     */
    public static String[] splitQuoteAware(String strToSplit) {
        ArrayList res = new ArrayList();
        boolean inQuotes = false, escape = false;
        StringBuilder part = new StringBuilder();
        int i = 0;
        while (i < strToSplit.length()) {
            char c = strToSplit.charAt(i++);

            if (escape) {
                part.append(c);
                escape = false;
                continue;
            }

            switch (c) {
            case '\\':
                if (i < strToSplit.length() && strToSplit.charAt(i) == '"')
                    escape = true;
                else
                    part.append(c);
                break;
            case '"':
                inQuotes = true;
                while (inQuotes && i < strToSplit.length()) {
                    c = strToSplit.charAt(i++);
                    if (escape == false) {
                        if (c == '\\') {
                            if (i < strToSplit.length() && strToSplit.charAt(i) == '"')
                                escape = true;
                            else
                                part.append(c);
                            continue;
                        }

                        if (c == '"') {
                            inQuotes = false;
                            continue;
                        }
                    }
                    else
                        escape = false;
                    part.append(c);
                }
                break;
            case ' ':
                if (part.length() > 0) {
                    res.add(part.toString());
                    part = new StringBuilder();
                }
                break;
            default:
                part.append(c);
            }
        }

        if (part.length() > 0) {
            res.add(part.toString());
        }
        String[] tmp = new String[res.size()];

        res.toArray(tmp);
        return tmp;
    }

    /**
     * Use to be cmdRun Insert the method's description here. Creation date: (4/21/2002 8:40:01 AM)
     * 
     * @param pArg java.lang.String
     * @param pVarName the var name
     * 
     * @return java.lang.String[]
     */
    public static String extractArguments(String pArg, String pVarName) {
        String result = null;
        int argPos = -1;

        argPos = pArg.indexOf(pVarName);

        if (argPos != -1) {
            String fields = pArg.substring(pVarName.length());

            if (fields.length() > 0) {
                result = fields;
            }
        }

        return (result);
    }

    /**
     * Insert the method's description here. Creation date: (4/21/2002 8:40:01 AM)
     * 
     * @param pArg java.lang.String
     * @param pVarName the var name
     * 
     * @return java.lang.String[]
     */
    public static String[] extractMultipleArguments(String pArg, String pVarName) {
        String[] result = null;
        int argPos = -1;

        argPos = pArg.indexOf(pVarName);

        if (argPos != -1) {
            try {
                String fields = pArg.substring(pVarName.length(), pArg.indexOf("]", pVarName.length()));

                if (fields.indexOf(',') != -1) {
                    // string contains multiple files
                    StringTokenizer st = new StringTokenizer(fields, ",");

                    int nFields = st.countTokens();

                    result = new String[nFields];

                    int pos = 0;

                    while (st.hasMoreTokens()) {
                        result[pos] = st.nextToken();
                        pos++;
                    }
                }
                else if (fields.length() > 0) {
                    result = new String[1];
                    result[0] = fields;
                }
            } catch (Exception e) {
                ResourcePool.LogMessage("ERROR: Badly formed \"" + pVarName + "\" parameter: " + pArg);
                System.exit(-1);
            }
        }

        return (result);
    }
}
