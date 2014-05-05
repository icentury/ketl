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

import java.io.File;
import java.io.FileInputStream;
import java.io.StringWriter;

// TODO: Auto-generated Javadoc
/**
 * The Class EncodeBase64.
 */
public class EncodeBase64 {

    /** The Base table. */
    static String BaseTable[] = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
            "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6",
            "7", "8", "9", "+", "/" };

    /**
     * Encode.
     * 
     * @param filename the filename
     * 
     * @return the string
     */
    public static String encode(String filename) {

        StringWriter out = new StringWriter();
        try {
            File f = new File(filename);
            FileInputStream fin = new FileInputStream(filename);

            // read the entire file into the byte array
            byte bytes[] = new byte[(int) (f.length())];
            int n = fin.read(bytes);

            if (n < 1)
                return ""; // no bytes to encode!?!

            byte buf[] = new byte[4]; // array of base64 characters

            int n3byt = n / 3; // how 3 bytes groups?
            int nrest = n % 3; // the remaining bytes from the grouping
            int k = n3byt * 3; // we are doing 3 bytes at a time
            int linelength = 0; // current linelength
            int i = 0; // index

            // do the 3-bytes groups ...
            while (i < k) {
                buf[0] = (byte) ((bytes[i] & 0xFC) >> 2);
                buf[1] = (byte) (((bytes[i] & 0x03) << 4) | ((bytes[i + 1] & 0xF0) >> 4));
                buf[2] = (byte) (((bytes[i + 1] & 0x0F) << 2) | ((bytes[i + 2] & 0xC0) >> 6));
                buf[3] = (byte) (bytes[i + 2] & 0x3F);
                out.write(EncodeBase64.BaseTable[buf[0]]);
                out.write(EncodeBase64.BaseTable[buf[1]]);
                out.write(EncodeBase64.BaseTable[buf[2]]);
                out.write(EncodeBase64.BaseTable[buf[3]]);
                /*
                 * The above code can be written in more "optimized" way. Harder to understand but more compact. Thanks
                 * to J. Tordera for the tip! buf[0]= (byte)(b[i] >> 2); buf[1]= (byte)(((b[i] & 0x03) << 4)|(b[i+1]>>
                 * 4)); buf[2]= (byte)(((b[i+1] & 0x0F)<< 2)|(b[i+2]>> 6)); buf[3]= (byte)(b[i+2] & 0x3F);
                 * send(out,BaseTable[buf[0]]+BaseTable[buf[1]]+ BaseTable[buf[2]]+BaseTable[buf[3]]);
                 */

                if ((linelength += 4) >= 76) {
                    out.write("\r\n");
                    linelength = 0;
                }
                i += 3;
            }

            // deals with with the padding ...
            if (nrest == 2) {
                // 2 bytes left
                buf[0] = (byte) ((bytes[k] & 0xFC) >> 2);
                buf[1] = (byte) (((bytes[k] & 0x03) << 4) | ((bytes[k + 1] & 0xF0) >> 4));
                buf[2] = (byte) ((bytes[k + 1] & 0x0F) << 2);
            }
            else if (nrest == 1) {
                // 1 byte left
                buf[0] = (byte) ((bytes[k] & 0xFC) >> 2);
                buf[1] = (byte) ((bytes[k] & 0x03) << 4);
            }
            if (nrest > 0) {
                // send the padding
                if ((linelength += 4) >= 76)
                    out.write("\r\n");
                out.write(EncodeBase64.BaseTable[buf[0]]);
                out.write(EncodeBase64.BaseTable[buf[1]]);
                // Thanks to R. Claerman for the bug fix here!
                if (nrest == 2) {
                    out.write(EncodeBase64.BaseTable[buf[2]]);
                }
                else {
                    out.write("=");
                }
                out.write("=");
            }
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return out.toString();
    }

}
