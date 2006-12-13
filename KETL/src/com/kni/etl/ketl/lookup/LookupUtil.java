package com.kni.etl.ketl.lookup;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.util.Bytes;

final public class LookupUtil {

    public static byte[] getKey(Object[] tmp, Class[] pKeyTypes, int keyLen) throws Error {
        byte[] key = null;
        for (int i = 0; i < keyLen; i++) {
            if (tmp[i] == null)
                throw new KETLError("Key cannot contain null values, review key " + java.util.Arrays.toString(tmp));

            if (i > 0)
                key = Bytes.append(key, getAsKey(tmp[i], pKeyTypes[i]));
            else
                key = getAsKey(tmp[i], pKeyTypes[i]);
        }

        return key;
    }

    public static byte[] getKey(Object tmp, Class pKeyType) {
        return getAsKey(tmp, pKeyType);
    }

    private static byte[] getAsKey(Object obj, Class cl) throws Error {

        if (obj == null)
            return null;

        if (cl == Double.class)
            return Bytes.packF8((Double) obj);
        if (cl == Integer.class)
            return Bytes.pack4((Integer) obj);
        if (cl == String.class)
            try {
                return Bytes.packStr((String) obj, null);
            } catch (UnsupportedEncodingException e) {
                throw new KETLError(e);
            }
        if (cl == Long.class)
            return Bytes.pack8((Long) obj);
        if (cl == Short.class)
            return Bytes.pack2((Short) obj);
        if (cl == Float.class)
            return Bytes.packF4((Float) obj);
        if (cl == java.util.Date.class)
            return Bytes.pack8(((java.util.Date) obj).getTime());

        if (cl == java.sql.Date.class)
            return Bytes.pack8(((java.sql.Date) obj).getTime());
        if (cl == java.sql.Time.class)
            return Bytes.pack8(((java.sql.Time) obj).getTime());

        if (cl == java.sql.Timestamp.class)
            return Bytes.append(Bytes.pack8(((java.sql.Timestamp) obj).getTime()), Bytes
                    .pack4(((java.sql.Timestamp) obj).getNanos()));

        if (cl == byte[].class)
            return (byte[]) obj;
        if (cl == BigDecimal.class) {
            return Bytes.append(Bytes.packF8(((BigDecimal) obj).doubleValue()), Bytes.pack4(((BigDecimal) obj)
                    .hashCode()));
        }

        throw new KETLError("Unsupported key type " + cl.getName());
    }

}
