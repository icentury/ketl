/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 14, 2006
 * 
 */
package com.kni.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
final public class FastSerializer {

	/** The Constant ARRAY. */
	final static byte ARRAY_1D = 8;
	/** The Constant ARRAY. */
	final static byte ARRAY_2D = 15;

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
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(File.createTempFile("drd","drd"))));
		Object[] data = new Object[size];
		for (int i = 0; i < size; i++) {
			data[i] = new java.sql.Timestamp((long) i);
		}

		ObjectOutputStream outb = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(File.createTempFile("drd","drx"))));
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			outb.writeObject(data[i]);
			outb.writeObject(i);
		}
		
		outb.flush();
		outb.close();

		long end = System.currentTimeMillis();
		double dEnd = end, dStart = start, dSize = size;

		System.out.println("Standard Serialization Ops/Sec:" + (dSize / ((dEnd - dStart) / 1000)));
		
		start = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			serialize(data[i],out);
			serialize(i,out);
			 
		}
		
		out.flush();
		out.close();
		end = System.currentTimeMillis();

		dEnd = end;
		dStart = start;
		dSize = size;

		out.close();
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
	public static void serialize(final Object obj,DataOutputStream out) throws IOException {

		if (obj == null) {	
			out.write(new byte[] { NULL });
			return;
		}

		Class cl = obj.getClass();
		byte[] buf;

		if (obj instanceof byte[]) {
			out.write(BYTEARRAY);
			out.writeInt(((byte[]) obj).length);
			out.write((byte[]) obj,0,((byte[]) obj).length);
			return ;
		}

		if (cl.isArray()) {
			if(cl == Object[][].class)
				out.write(ARRAY_2D);
			else
				out.write(ARRAY_1D);
			
			Object[] ar = (Object[]) obj;
			int len = ar.length;

			out.writeInt(len);
			
			for (int i = 0; i < len; i++) {
				serialize(ar[i],out);
			}

			return;
		}

		if (cl == Integer.class) {
			buf = new byte[5];
			out.write(INT);
			out.writeInt(((Integer) obj));			
		} else if (cl == String.class) {
			byte[] tmp = ((String) obj).getBytes();
			out.write( STRING);
			out.writeInt(tmp.length);
			out.write(tmp,0,tmp.length);			
		} else if (cl == java.sql.Timestamp.class) {
			java.sql.Timestamp tmp = (java.sql.Timestamp) obj;

			buf = new byte[13];
			out.write( TIMESTAMP);
			out.writeLong( tmp.getTime());
			out.writeInt( tmp.getNanos());
		} else if (cl == java.math.BigDecimal.class) {
			BigDecimal bd = (BigDecimal) obj;
			byte[] tmp = bd.unscaledValue().toByteArray();
			out.write( BIGDECIMAL);
			out.writeInt(tmp.length);
			out.writeInt(bd.scale());
			out.write(tmp,0,tmp.length);			
		} else if (cl == java.sql.Date.class) {
			out.write( SQLDATE);
			out.writeLong( ((java.sql.Date) obj).getTime());
		} else if (cl == java.sql.Time.class) {
			buf = new byte[9];
			out.write( TIME);
			out.writeLong(((java.sql.Time) obj).getTime());
		} else if (cl == Short.class) {
			buf = new byte[3];
			out.write(SHORT);
			out.writeShort( ((Short) obj));
		} else if (cl == Long.class) {
			buf = new byte[9];
			out.write( LONG);
			out.writeLong( ((Long) obj));
		} else if (cl == Float.class) {
			buf = new byte[5];
			out.write( FLOAT);
			out.writeFloat(((Float) obj));
		} else if (cl == Double.class) {
			buf = new byte[9];
			out.write( DOUBLE);
			out.writeDouble( ((Double) obj));
		} else {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			ObjectOutputStream objStream = new ObjectOutputStream(outStream);
			objStream.writeObject(obj);
			objStream.flush();
			outStream.flush();
			byte[] tmp = outStream.toByteArray();
			buf = new byte[tmp.length + 1];
			buf[0] = OBJECT;
			out.writeInt(tmp.length);
			out.write(tmp,0,tmp.length);
			objStream.close();
			outStream.close();
		}
	
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
	public static Object deserialize(DataInputStream in) throws IOException,
			ClassNotFoundException {

		byte type = in.readByte();

		if (type == NULL)
			return null;
		if (type == ARRAY_1D) {
			int len = in.readInt();
			Object[] ar = new Object[len];
			for (int i = 0; i < len; i++) {
				ar[i] = deserialize(in);			
			}
			return ar;
		}
		if (type == ARRAY_2D) {
			int len = in.readInt();
			Object[][] ar = new Object[len][];
			for (int i = 0; i < len; i++) {
				ar[i] = (Object[]) deserialize(in);			
			}
			return ar;
		}
		if (type == INT) {
			return in.readInt();
		}
		if (type == STRING) {
			int len = in.readInt();
			byte[] buf = new byte[len];
			in.read(buf,0,len);
			return new String(buf, 0, len);
		}
		if (type == BYTEARRAY) {
			int len = in.readInt();
			byte[] buf = new byte[len];
			in.read(buf,0,len);
			return buf;
		}
		if (type == SHORT) {
			return in.readShort();
		}
		if (type == DOUBLE) {
			return in.readDouble();
		}
		if (type == LONG) {
			return in.readLong();
		}
		if (type == FLOAT) {
			return in.readFloat();
		}
		if (type == TIMESTAMP) {
			long t = in.readLong();
			java.sql.Timestamp res = new java.sql.Timestamp(t);
			res.setNanos(in.readInt());
			return res;
		}
		if (type == BIGDECIMAL) {
			int length = in.readInt();
			byte[] tmp = new byte[length - 5];
			int scale = in.readInt();
			in.read(tmp,0,length);
			BigInteger bi = new BigInteger(tmp);
			return new BigDecimal(bi, scale);
		}
		if (type == SQLDATE) {
			return new java.sql.Date(in.readLong());
		}
		if (type == TIME) {
			return new java.sql.Time(in.readLong());
		}
		if (type == OBJECT) {
			int length = in.readInt();
			byte[] buf = new byte[length];
			in.read(buf, 0, length);
			ByteArrayInputStream outStream = new ByteArrayInputStream(buf, 0, length );
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
