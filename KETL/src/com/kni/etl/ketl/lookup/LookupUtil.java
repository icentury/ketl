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
package com.kni.etl.ketl.lookup;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.util.Bytes;

// TODO: Auto-generated Javadoc
/**
 * The Class LookupUtil.
 */
final public class LookupUtil {

    /**
     * Gets the key.
     * 
     * @param tmp the tmp
     * @param pKeyTypes the key types
     * @param keyLen the key len
     * 
     * @return the key
     * 
     * @throws Error the error
     */
    public static byte[] getKey(Object[] tmp, Class[] pKeyTypes, int keyLen) throws Error {
        byte[] key = null;
        for (int i = 0; i < keyLen; i++) {
            if (tmp[i] == null)
                throw new KETLError("Key cannot contain null values, review key " + java.util.Arrays.toString(tmp));

            if (i > 0)
                key = Bytes.append(key, LookupUtil.getAsKey(tmp[i], pKeyTypes[i]));
            else
                key = LookupUtil.getAsKey(tmp[i], pKeyTypes[i]);
        }

        return key;
    }

    /**
     * Gets the key.
     * 
     * @param tmp the tmp
     * @param pKeyType the key type
     * 
     * @return the key
     */
    public static byte[] getKey(Object tmp, Class pKeyType) {
        return LookupUtil.getAsKey(tmp, pKeyType);
    }

    /**
     * Gets the as key.
     * 
     * @param obj the obj
     * @param cl the cl
     * 
     * @return the as key
     * 
     * @throws Error the error
     */
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
