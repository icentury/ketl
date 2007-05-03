package com.kni.util;

import java.io.UnsupportedEncodingException;

import com.kni.etl.ketl.exceptions.KETLError;

final public class Bytes {

    public static short unpack2(byte[] arr, int offs) {
        return (short) ((arr[offs] << 8) | (arr[offs + 1] & 0xFF));
    }

    public static int unpack4(byte[] arr, int offs) {
        return (arr[offs] << 24) | ((arr[offs + 1] & 0xFF) << 16) | ((arr[offs + 2] & 0xFF) << 8)
                | (arr[offs + 3] & 0xFF);
    }

    public static long unpack8(byte[] arr, int offs) {
        return ((long) Bytes.unpack4(arr, offs) << 32) | (Bytes.unpack4(arr, offs + 4) & 0xFFFFFFFFL);
    }

    public static float unpackF4(byte[] arr, int offs) {
        return Float.intBitsToFloat(Bytes.unpack4(arr, offs));
    }

    public static double unpackF8(byte[] arr, int offs) {
        return Double.longBitsToDouble(Bytes.unpack8(arr, offs));
    }

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

    public static void pack2(byte[] arr, int offs, short val) {
        arr[offs] = (byte) (val >> 8);
        arr[offs + 1] = (byte) val;
    }

    public static void pack4(byte[] arr, int offs, int val) {
        arr[offs] = (byte) (val >> 24);
        arr[offs + 1] = (byte) (val >> 16);
        arr[offs + 2] = (byte) (val >> 8);
        arr[offs + 3] = (byte) val;
    }

    public static void pack8(byte[] arr, int offs, long val) {
        Bytes.pack4(arr, offs, (int) (val >> 32));
        Bytes.pack4(arr, offs + 4, (int) val);
    }

    public static void packF4(byte[] arr, int offs, float val) {
        Bytes.pack4(arr, offs, Float.floatToIntBits(val));
    }

    public static void packF8(byte[] arr, int offs, double val) {
        Bytes.pack8(arr, offs, Double.doubleToLongBits(val));
    }

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

    public static int sizeof(String str, String encoding) {
        try {
            return str == null ? 4 : encoding == null ? 4 + str.length() * 2
                    : 4 + new String(str).getBytes(encoding).length;
        } catch (UnsupportedEncodingException x) {
            throw new KETLError("UNSUPPORTED_ENCODING");
        }
    }

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

    public static byte[] pack2(short val) {
        return new byte[] { (byte) (val >> 8), (byte) val };
    }

    public static byte[] pack4(int val) {
        return new byte[] { (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val };
    }

    public static byte[] pack8(long val) {
        return new byte[] { (byte) (val >> 56), (byte) (val >> 48), (byte) (val >> 40), (byte) (val >> 32),
                (byte) (val >> 24), (byte) (val >> 16), (byte) (val >> 8), (byte) val };
    }

    public static byte[] packF4(float val) {
        return Bytes.pack4(Float.floatToIntBits(val));
    }

    public static byte[] packF8(double val) {
        return Bytes.pack8(Double.doubleToLongBits(val));
    }

    public static byte[] packStr(String str, String encoding) throws UnsupportedEncodingException {
        if (str == null) {
            return Bytes.pack4(-1);
        }
        if (encoding == null) {
            return str.getBytes();

        }

        return str.getBytes(encoding);

    }

    public static byte[] append(byte[] a, byte[] b) {
        byte[] z = new byte[a.length + b.length + 1];
        System.arraycopy(a, 0, z, 0, a.length);
        z[a.length] = 0;
        System.arraycopy(b, 0, z, a.length, b.length);
        return z;
    }

    public static byte[] join(byte[] a, byte[] b) {
        byte[] z = new byte[a.length + b.length];
        System.arraycopy(a, 0, z, 0, a.length);
        System.arraycopy(b, 0, z, a.length, b.length);
        return z;
    }

}