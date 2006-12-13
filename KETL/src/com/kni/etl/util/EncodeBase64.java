package com.kni.etl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringWriter;

public class EncodeBase64 {

    static String BaseTable[] = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
            "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6",
            "7", "8", "9", "+", "/" };

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
                out.write(BaseTable[buf[0]]);
                out.write(BaseTable[buf[1]]);
                out.write(BaseTable[buf[2]]);
                out.write(BaseTable[buf[3]]);
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
                out.write(BaseTable[buf[0]]);
                out.write(BaseTable[buf[1]]);
                // Thanks to R. Claerman for the bug fix here!
                if (nrest == 2) {
                    out.write(BaseTable[buf[2]]);
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
