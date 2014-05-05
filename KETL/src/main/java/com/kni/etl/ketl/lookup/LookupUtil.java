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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

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

    protected static ByteArrayWrapper objToWrappedByteArray(Object obj) throws IOException{
    	return new ByteArrayWrapper(objToByteArray(obj));
    }
    protected static Object byteArrayToObject(ByteArrayWrapper ba) throws IOException, ClassNotFoundException{
    	return byteArrayToObject(ba.data,0,ba.data.length);
    }
	/**
	 * Byte array to object.
	 * 
	 * @param buf
	 *            the buf
	 * @param off
	 *            the off
	 * @param length
	 *            the length
	 * 
	 * @return the object
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	protected static Object byteArrayToObject(final byte[] buf, int off, final int length) throws IOException,
			ClassNotFoundException {
	
		byte type = buf[off++];
	
		if (type == NULL)
			return null;
		if (type == ARRAY) {
			int len = buf[off++];
			Object[] ar = new Object[len];
			int pos = off;
			for (int i = 0; i < len; i++) {
				int size = buf[pos++];
				ar[i] = byteArrayToObject(buf, pos, size);
				pos += size;
			}
	
			return ar;
		}
		if (type == INT) {
			return Bytes.unpack4(buf, off);
		}
		if (type == STRING) {
			return new String(buf, off, length - 1);
		}
		if (type == SHORT) {
			return Bytes.unpack2(buf, off);
		}
		if (type == DOUBLE) {
			return Bytes.unpackF8(buf, off);
		}
		if (type == LONG) {
			return Bytes.unpack8(buf, off);
		}
		if (type == FLOAT) {
			return Bytes.unpackF4(buf, off);
		}
		if (type == TIMESTAMP) {
			long t = Bytes.unpack8(buf, off);
			java.sql.Timestamp res = new java.sql.Timestamp(t);
			res.setNanos(Bytes.unpack4(buf, off + 8));
			return res;
		}
		if (type == BIGDECIMAL) {
			byte[] tmp = new byte[length - 5];
			int scale = Bytes.unpack4(buf, off);
			System.arraycopy(buf, off + 4, tmp, 0, length - 5);
			BigInteger bi = new BigInteger(tmp);
			return new BigDecimal(bi, scale);
		}
		if (type == SQLDATE) {
			return new java.sql.Date(Bytes.unpack8(buf, off));
		}
		if (type == TIME) {
			return new java.sql.Time(Bytes.unpack8(buf, off));
		}
		if (type == OBJECT) {
			ByteArrayInputStream outStream = new ByteArrayInputStream(buf, off, length - 1);
			ObjectInputStream objStream = new ObjectInputStream(outStream);
			Object obj = objStream.readObject();
			objStream.close();
			outStream.close();
			return obj;
		}
		throw new IOException("Invalid type found in data stream");
	}

	
	public final static class ByteArrayWrapper
	{
	    private final byte[] data;

	    public ByteArrayWrapper(byte[] data)
	    {
	        if (data == null)
	        {
	            throw new NullPointerException();
	        }
	        this.data = data;
	    }

	    @Override
	    public boolean equals(Object other)
	    {
	        if (!(other instanceof ByteArrayWrapper))
	        {
	            return false;
	        }
	        return Arrays.equals(data, ((ByteArrayWrapper)other).data);
	    }

	    @Override
	    public int hashCode()
	    {
	        return Arrays.hashCode(data);
	    }
	}
	
	/**
	 * Obj to byte array.
	 * 
	 * @param obj
	 *            the obj
	 * 
	 * @return the byte[]
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	protected static byte[] objToByteArray(final Object obj) throws IOException {
	
		if (obj == null)
			return new byte[] { NULL };
	
		Class cl = obj.getClass();
		byte[] buf;
	
		if (cl.isArray()) {
			Object[] ar = (Object[]) obj;
			int len = ar.length;
	
			buf = new byte[] { ARRAY, (byte) len };
			for (int i = 0; i < len; i++) {
				byte[] tmp = objToByteArray(ar[i]);
				buf = Bytes.join(buf, new byte[] { (byte) tmp.length });
				buf = Bytes.join(buf, tmp);
			}
	
			return buf;
		}
	
		if (cl == Integer.class) {
			buf = new byte[5];
			buf[0] = INT;
			Bytes.pack4(buf, 1, ((Integer) obj));
		} else if (cl == String.class) {
			byte[] tmp = ((String) obj).getBytes();
			buf = new byte[tmp.length + 1];
			buf[0] = STRING;
			System.arraycopy(tmp, 0, buf, 1, tmp.length);
		} else if (cl == java.sql.Timestamp.class) {
			java.sql.Timestamp tmp = (java.sql.Timestamp) obj;
	
			buf = new byte[13];
			buf[0] = TIMESTAMP;
			Bytes.pack8(buf, 1, tmp.getTime());
			Bytes.pack4(buf, 9, tmp.getNanos());
		} else if (cl == java.math.BigDecimal.class) {
			BigDecimal bd = (BigDecimal) obj;
			byte[] tmp = bd.unscaledValue().toByteArray();
			buf = new byte[tmp.length + 5];
			buf[0] = BIGDECIMAL;
			int scale = bd.scale();
			Bytes.pack4(buf, 1, scale);
			System.arraycopy(tmp, 0, buf, 5, tmp.length);
		} else if (cl == java.sql.Date.class) {
			buf = new byte[9];
			buf[0] = SQLDATE;
			Bytes.pack8(buf, 1, ((java.sql.Date) obj).getTime());
		} else if (cl == java.sql.Time.class) {
			buf = new byte[9];
			buf[0] = TIME;
			Bytes.pack8(buf, 1, ((java.sql.Time) obj).getTime());
		} else if (cl == Short.class) {
			buf = new byte[3];
			buf[0] = SHORT;
			Bytes.pack2(buf, 1, ((Short) obj));
		} else if (cl == Long.class) {
			buf = new byte[9];
			buf[0] = LONG;
			Bytes.pack8(buf, 1, ((Long) obj));
		} else if (cl == Float.class) {
			buf = new byte[5];
			buf[0] = FLOAT;
			Bytes.packF4(buf, 1, ((Float) obj));
		} else if (cl == Double.class) {
			buf = new byte[9];
			buf[0] = DOUBLE;
			Bytes.packF8(buf, 1, ((Double) obj));
		} else {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			ObjectOutputStream objStream = new ObjectOutputStream(outStream);
			objStream.writeObject(obj);
			objStream.flush();
			outStream.flush();
			byte[] tmp = outStream.toByteArray();
			buf = new byte[tmp.length + 1];
			buf[0] = OBJECT;
			System.arraycopy(tmp, 0, buf, 1, tmp.length);
			objStream.close();
			outStream.close();
		}
		return buf;
	}

	/** The Constant ARRAY. */
	final static byte ARRAY = 8;
	/** The Constant BIGDECIMAL. */
	final static byte BIGDECIMAL = 13;
	/** The Constant DATE. */
	final static byte DATE = 9;
	/** The Constant DOUBLE. */
	final static byte DOUBLE = 4;
	/** The Constant FLOAT. */
	final static byte FLOAT = 3;
	/** The Constant INT. */
	final static byte INT = 1;
	/** The Constant NULL. */
	final static byte NULL = 7;
	/** The Constant OBJECT. */
	final static byte OBJECT = 6;
	/** The Constant SHORT. */
	final static byte SHORT = 0;
	/** The Constant SQLDATE. */
	final static byte SQLDATE = 11;
	/** The Constant STRING. */
	final static byte STRING = 5;
	/** The Constant TIME. */
	final static byte TIME = 10;
	/** The Constant TIMESTAMP. */
	final static byte TIMESTAMP = 12;
	/** The Constant LONG. */
	final static byte LONG = 2;

}
