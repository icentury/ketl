package com.kni.util;

import java.io.UnsupportedEncodingException;

final public class Bytes {

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
        return pack4(Float.floatToIntBits(val));
    }

    public static byte[] packF8(double val) {
        return pack8(Double.doubleToLongBits(val));
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

}