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
package com.kni.util;

import java.io.UnsupportedEncodingException;

import com.kni.etl.ketl.exceptions.KETLError;

// TODO: Auto-generated Javadoc
/**
 * The Class Bytes.
 */
final public class Bytes {

    /**
     * Unpack2.
     * 
     * @param arr the arr
     * @param offs the offs
     * 
     * @return the short
     */
    public static short unpack2(byte[] arr, int offs) {
        return (short) ((arr[offs] << 8) | (arr[offs + 1] & 0xFF));
    }

    /**
     * Unpack4.
     * 
     * @param arr the arr
     * @param offs the offs
     * 
     * @return the int
     */
    public static int unpack4(byte[] arr, int offs) {
        return (arr[offs] << 24) | ((arr[offs + 1] & 0xFF) << 16) | ((arr[offs + 2] & 0xFF) << 8)
                | (arr[offs + 3] & 0xFF);
    }

    /**
     * Unpack8.
     * 
     * @param arr the arr
     * @param offs the offs
     * 
     * @return the long
     */
    public static long unpack8(byte[] arr, int offs) {
        return ((long) Bytes.unpack4(arr, offs) << 32) | (Bytes.unpack4(arr, offs + 4) & 0xFFFFFFFFL);
    }

    /**
     * Unpack f4.
     * 
     * @param arr the arr
     * @param offs the offs
     * 
     * @return the float
     */
    public static float unpackF4(byte[] arr, int offs) {
        return Float.intBitsToFloat(Bytes.unpack4(arr, offs));
    }

    /**
     * Unpack f8.
     * 
     * @param arr the arr
     * @param offs the offs
     * 
     * @return the double
     */
    public static double unpackF8(byte[] arr, int offs) {
        return Double.longBitsToDouble(Bytes.unpack8(arr, offs));
    }

    /**
     * Unpack str.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param encoding the encoding
     * 
     * @return the string
     */
    public static String unpackStr(byte[] arr, int offs, String encoding) {
        int len = Bytes.unpack4(arr, offs);
        if (len >= 0) {
            char[] chars = new char[len];
            offs += 4;
            for (int i = 0; i < len; i++) {
                chars[i] = (char) Bytes.unpack2(arr, offs);
                offs += 2;
            }
            return new String(chars);
        }
        else if (len < -1) {
            if (encoding != null) {
                try {
                    return new String(arr, offs, -len - 2, encoding);
                } catch (UnsupportedEncodingException x) {
                    throw new KETLError("UNSUPPORTED_ENCODING");
                }
            }
            else {
                return new String(arr, offs, -len - 2);
            }
        }
        return null;
    }

    /**
     * Pack2.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param val the val
     */
    public static void pack2(byte[] arr, int offs, short val) {
        arr[offs] = (byte) (val >> 8);
        arr[offs + 1] = (byte) val;
    }

    /**
     * Pack4.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param val the val
     */
    public static void pack4(byte[] arr, int offs, int val) {
        arr[offs] = (byte) (val >> 24);
        arr[offs + 1] = (byte) (val >> 16);
        arr[offs + 2] = (byte) (val >> 8);
        arr[offs + 3] = (byte) val;
    }

    /**
     * Pack8.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param val the val
     */
    public static void pack8(byte[] arr, int offs, long val) {
        Bytes.pack4(arr, offs, (int) (val >> 32));
        Bytes.pack4(arr, offs + 4, (int) val);
    }

    /**
     * Pack f4.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param val the val
     */
    public static void packF4(byte[] arr, int offs, float val) {
        Bytes.pack4(arr, offs, Float.floatToIntBits(val));
    }

    /**
     * Pack f8.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param val the val
     */
    public static void packF8(byte[] arr, int offs, double val) {
        Bytes.pack8(arr, offs, Double.doubleToLongBits(val));
    }

    /**
     * Pack str.
     * 
     * @param arr the arr
     * @param offs the offs
     * @param str the str
     * @param encoding the encoding
     * 
     * @return the int
     */
    public static int packStr(byte[] arr, int offs, String str, String encoding) {
        if (str == null) {
            Bytes.pack4(arr, offs, -1);
            offs += 4;
        }
        else if (encoding == null) {
            int n = str.length();
            Bytes.pack4(arr, offs, n);
            offs += 4;
            for (int i = 0; i < n; i++) {
                Bytes.pack2(arr, offs, (short) str.charAt(i));
                offs += 2;
            }
        }
        else {
            try {
                byte[] bytes = str.getBytes(encoding);
                Bytes.pack4(arr, offs, -2 - bytes.length);
                System.arraycopy(bytes, 0, arr, offs + 4, bytes.length);
                offs += 4 + bytes.length;
            } catch (UnsupportedEncodingException x) {
                throw new KETLError("UNSUPPORTED_ENCODING");
            }
        }
        return offs;
    }

    /**
     * Sizeof.
     * 
     * @param str the str
     * @param encoding the encoding
     * 
     * @return the int
     */
    public static int sizeof(String str, String encoding) {
        try {
            return str == null ? 4 : encoding == null ? 4 + str.length() * 2
                    : 4 + new String(str).getBytes(encoding).length;
        } catch (UnsupportedEncodingException x) {
            throw new KETLError("UNSUPPORTED_ENCODING");
        }
    }

    /**
     * Sizeof.
     * 
     * @param arr the arr
     * @param offs the offs
     * 
     * @return the int
     */
    public static int sizeof(byte[] arr, int offs) {
        int len = Bytes.unpack4(arr, offs);
        if (len >= 0) {
            return 4 + len * 2;
        }
        else if (len < -1) {
            return 4 - 2 - len;
        }
        else {
            return 4;
        }
    }

    /**
     * Pack2.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] pack2(short val) {
        return new byte[] { (byte) (val >> 8), (byte) val };
    }

    /**
     * Pack4.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] pack4(int val) {
        return new byte[] { (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val };
    }

    /**
     * Pack8.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] pack8(long val) {
        return new byte[] { (byte) (val >> 56), (byte) (val >> 48), (byte) (val >> 40), (byte) (val >> 32),
                (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val };
    }

    /**
     * Pack f4.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] packF4(float val) {
        return Bytes.pack4(Float.floatToIntBits(val));
    }

    /**
     * Pack f8.
     * 
     * @param val the val
     * 
     * @return the byte[]
     */
    public static byte[] packF8(double val) {
        return Bytes.pack8(Double.doubleToLongBits(val));
    }

    /**
     * Pack str.
     * 
     * @param str the str
     * @param encoding the encoding
     * 
     * @return the byte[]
     * 
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static byte[] packStr(String str, String encoding) throws UnsupportedEncodingException {
        if (str == null) {
            return Bytes.pack4(-1);
        }
        if (encoding == null) {
            return str.getBytes();

        }

        return str.getBytes(encoding);

    }

    /**
     * Append.
     * 
     * @param a the a
     * @param b the b
     * 
     * @return the byte[]
     */
    public static byte[] append(byte[] a, byte[] b) {
        byte[] z = new byte[a.length + b.length + 1];
        System.arraycopy(a, 0, z, 0, a.length);
        z[a.length] = 0;
        System.arraycopy(b, 0, z, a.length, b.length);
        return z;
    }

    /**
     * Join.
     * 
     * @param a the a
     * @param b the b
     * 
     * @return the byte[]
     */
    public static byte[] join(byte[] a, byte[] b) {
        byte[] z = new byte[a.length + b.length];
        System.arraycopy(a, 0, z, 0, a.length);
        System.arraycopy(b, 0, z, a.length, b.length);
        return z;
    }

}