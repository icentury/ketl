/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 14, 2006
 * 
 */
package com.kni.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
final public class AutoClassCaster {

	/** The Constant ARRAY. */
	final static byte ARRAY = 8;

	/** The Constant BIGDECIMAL. */
	final static byte BIGDECIMAL = 13;

	/** The Constant BIGDECIMAL. */
	final static byte BYTEARRAY = 14;

	/** The Constant DATE. */
	final static byte DATE = 9;

	/** The Constant DOUBLE. */
	final static byte DOUBLE = 4;

	/** The Constant FLOAT. */
	final static byte FLOAT = 3;

	/** The Constant INT. */
	final static byte INT = 1;

	/** The Constant LONG. */
	final static byte LONG = 2;

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

	public static void main(String[] args) throws IOException {
		int size = 500000;
		Object[] data = new Object[size];
		for (int i = 0; i < size; i++) {
			data[i] = new java.sql.Timestamp((long) i);
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			byte[] value = serialize(data[i]);
		}

		long end = System.currentTimeMillis();
		double dEnd = end, dStart = start, dSize = size;

		System.out.println("Standard Serialization Ops/Sec:" + (dSize / ((dEnd - dStart) / 1000)));

		start = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			byte[] value = serialize(data[i]);
		}
		end = System.currentTimeMillis();

		dEnd = end;
		dStart = start;
		dSize = size;

		System.out.println("Custom Serialization Ops/Sec:" + (dSize / ((dEnd - dStart) / 1000)));
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
	public static byte[] serialize(final Object obj) throws IOException {

		if (obj == null)
			return new byte[] { NULL };

		Class cl = obj.getClass();
		byte[] buf;

		if (obj instanceof byte[]) {
			byte[] tmp = (byte[]) obj;
			buf = new byte[tmp.length + 1];
			buf[0] = BYTEARRAY;
			System.arraycopy(tmp, 0, buf, 1, tmp.length);
			return buf;
		}

		if (cl.isArray()) {
			Object[] ar = (Object[]) obj;
			int len = ar.length;

			buf = new byte[] { ARRAY, (byte) len };
			for (int i = 0; i < len; i++) {
				byte[] tmp = serialize(ar[i]);
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
	public static Object deserialize(final byte[] buf, int off, final int length) throws IOException,
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
				ar[i] = deserialize(buf, pos, size);
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
		if (type == BYTEARRAY) {
			byte[] res = new byte[length - 1];
			System.arraycopy(buf, off, res, 0, length - 1);
			return res;
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

	static final public Object toObject(String pObject, Class pRequiredType, java.text.Format fmt)
			throws ParseException {

		if (pRequiredType == String.class)
			return pObject;

		if (pRequiredType == Integer.class) {
			return Integer.parseInt(pObject);
		}

		if (pRequiredType == Long.class) {
			return Long.parseLong(pObject);
		}

		if (pRequiredType == BigDecimal.class) {
			return new BigDecimal(pObject);
		}
		if (pRequiredType == Short.class) {
			return Short.valueOf(pObject);
		}

		if (pRequiredType == Float.class) {
			return Float.parseFloat(pObject);
		}

		if (pRequiredType == Double.class) {
			return Double.parseDouble(pObject);
		}

		if (pRequiredType == Boolean.class)
			return Boolean.parseBoolean(pObject);

		if (pRequiredType == java.util.Date.class) {
			if (fmt != null)
				return fmt.parseObject(pObject);

			java.util.Date d = new java.util.Date();
			d.setTime(Long.parseLong(pObject));
			return d;
		}

		if (pRequiredType == Timestamp.class) {
			if (fmt != null)
				return new Timestamp(((Date) fmt.parseObject(pObject)).getTime());

			Timestamp d = new Timestamp(Long.parseLong(pObject));
			return d;
		}

		if (pRequiredType == Time.class) {
			if (fmt != null)
				return new Time(((Date) fmt.parseObject(pObject)).getTime());

			Time d = new Time(Long.parseLong(pObject));
			return d;
		}

		throw new ClassCastException("Unknown datatype " + pRequiredType.getName());

	}

	static final public String toString(Object pObject, Class pClass) {

		if (pClass == String.class)
			return (String) pObject;

		if (pClass == Integer.class || pClass == Long.class || pClass == Float.class || pClass == Double.class
				|| pClass == Boolean.class || pClass == BigDecimal.class || pClass == Short.class)
			return pObject.toString();

		if (pClass == java.util.Date.class)
			return Long.toString(((java.util.Date) pObject).getTime());
		if (pClass == Timestamp.class)
			return Long.toString(((Timestamp) pObject).getTime());
		if (pClass == Time.class)
			return Long.toString(((Time) pObject).getTime());

		throw new ClassCastException("Unknown datatype " + pClass.getName());

	}

}
