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
package com.kni.etl.ketl.smp;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.GregorianCalendar;
import java.util.HashMap;

import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLJob;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.ketl.lookup.SCDValue;
import com.kni.etl.stringtools.FastSimpleDateFormat;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLCore.
 */
abstract public class ETLCore implements DefaultCore {

    /** The owner step. */
    private ETLWorker ownerStep;

    /**
     * Initialize core fields.
     */
    protected abstract void initializeCoreFields();

    /**
     * Gets the owner.
     * 
     * @return the owner
     */
    public ETLWorker getOwner() {
        return this.ownerStep;
    }

    /**
     * Sets the owner.
     * 
     * @param arg0 the new owner
     */
    public void setOwner(ETLWorker arg0) {
        this.ownerStep = arg0;
        this.initializeCoreFields();
    }

    /**
     * Lookup d.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pField the field
     * @param defaultValue the default value
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object lookupD(String pLookupName, Object pKey1, String pField, Object defaultValue)
            throws InterruptedException, KETLThreadException {
        Object res = this.lookup(pLookupName, pKey1, pField);
        if (res == null)
            return defaultValue;

        return res;
    }

    /**
     * Send email.
     * 
     * @param subject the subject
     * @param text the text
     * 
     * @return the object
     */
    final protected Object sendEmail(String subject, String text) {

        try {
            Metadata md = ResourcePool.getMetadata();
            md.sendEmail(((ETLStep) this.ownerStep).getJobExecutor().getCurrentETLJob().getJobID(), subject, text);
            return true;
        } catch (Exception e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, e.getMessage());
            return false;
        }
    }

    /**
     * To timestamp.
     * 
     * @param time the time
     * @param nanos the nanos
     * 
     * @return the java.sql. timestamp
     */
    final protected java.sql.Timestamp toTimestamp(long time, int nanos) {
        java.sql.Timestamp tms = new java.sql.Timestamp(time);
        tms.setNanos(nanos);
        return tms;
    }

    /**
     * To time.
     * 
     * @param time the time
     * 
     * @return the java.sql. time
     */
    final protected java.sql.Time toTime(long time) {
        java.sql.Time tms = new java.sql.Time(time);
        return tms;
    }

    /** The gregorian calendar. */
    GregorianCalendar mGregorianCalendar = new GregorianCalendar();

    /**
     * Gets the date part.
     * 
     * @param date the date
     * @param field the field
     * 
     * @return the date part
     */
    final protected Integer getDatePart(java.util.Date date, int field) {

        if (date == null)
            return null;

        this.mGregorianCalendar.setTime(date);
        return this.mGregorianCalendar.get(field);
    }

    /**
     * Generate exception.
     * 
     * @param message the message
     * @param critical the critical
     * 
     * @return the object
     * 
     * @throws KETLException the KETL exception
     */
    final protected Object generateException(String message, boolean critical) throws KETLException {
        if (critical)
            throw new KETLError(message);

        throw new KETLException(message);
    }

    /**
     * Lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pField the field
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object lookup(String pLookupName, Object pKey1, String pField) throws InterruptedException,
            KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);

        if (pField == null)
            return mp.get(new Object[] { pKey1 });
        try {
            return mp.get(new Object[] { pKey1 }, pField);
        } catch (Exception e) {
            throw new KETLThreadException("Lookup error " + e.getMessage(), e);
        }
    }

    /**
     * Lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pKey2 the key2
     * @param pField the field
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object lookup(String pLookupName, Object pKey1, Object pKey2, String pField)
            throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        if (pField == null)
            return mp.get(new Object[] { pKey1, pKey2 });
        try {
            return mp.get(new Object[] { pKey1, pKey2 }, pField);
        } catch (Exception e) {
            throw new KETLThreadException("Lookup error " + e.getMessage(), e);
        }

    }

    /**
     * Lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pKey2 the key2
     * @param pKey3 the key3
     * @param pField the field
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object lookup(String pLookupName, Object pKey1, Object pKey2, Object pKey3, String pField)
            throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        if (pField == null)
            return mp.get(new Object[] { pKey1, pKey2, pKey3 });
        try {
            return mp.get(new Object[] { pKey1, pKey2, pKey3 }, pField);
        } catch (Exception e) {
            throw new KETLThreadException("Lookup error " + e.getMessage(), e);
        }
    }

    /**
     * Lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pKey2 the key2
     * @param pKey3 the key3
     * @param pKey4 the key4
     * @param pField the field
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object lookup(String pLookupName, Object pKey1, Object pKey2, Object pKey3, Object pKey4,
            String pField) throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        if (pField == null)
            return mp.get(new Object[] { pKey1, pKey2, pKey3, pKey4 });
        try {
            return mp.get(new Object[] { pKey1, pKey2, pKey3, pKey4 }, pField);
        } catch (Exception e) {
            throw new KETLThreadException("Lookup error " + e.getMessage(), e);
        }
    }

    /** The local lookups. */
    private HashMap mLocalLookups = new HashMap();

    /**
     * Gets the lookup.
     * 
     * @param pLookupName the lookup name
     * 
     * @return the lookup
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final private PersistentMap getLookup(String pLookupName) throws InterruptedException, KETLThreadException {

        Object res = this.mLocalLookups.get(pLookupName);

        if (res == null) {
            res = ((KETLJob) ((ETLStep) this.getOwner()).getJobExecutor().getCurrentETLJob()).getSharedLookup(
                    pLookupName, this);
            this.mLocalLookups.put(pLookupName, res);
        }
        return (PersistentMap) res;
    }

    /**
     * To short.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the short
     */
    protected final static short toShort(String arg0, short arg1) {
        try {
            return Short.parseShort(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To int.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the int
     */
    protected final static int toInt(String arg0, int arg1) {
        try {
            return Integer.parseInt(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To long.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the long
     */
    protected final static long toLong(String arg0, long arg1) {
        try {
            return Long.parseLong(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To float.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the float
     */
    protected final static float toFloat(String arg0, float arg1) {
        try {
            return Float.parseFloat(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To double.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the double
     */
    protected final static double toDouble(String arg0, double arg1) {
        try {
            return Double.parseDouble(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To date.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * @param format the format
     * 
     * @return the java.util. date
     * 
     * @throws ParseException the parse exception
     */
    protected final static java.util.Date toDate(String arg0, String arg1, String format) throws ParseException {
        FastSimpleDateFormat fmt = new FastSimpleDateFormat(format);
        try {
            return fmt.parse(arg0);
        } catch (Exception e) {
            return fmt.parse(arg1);
        }
    }

    /**
     * To boolean.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return true, if successful
     */
    protected final static boolean toBoolean(String arg0, boolean arg1) {
        try {
            return Boolean.parseBoolean(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To byte.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the byte
     */
    protected final static byte toByte(String arg0, byte arg1) {
        try {
            return Byte.parseByte(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * To char.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the char
     */
    protected final static char toChar(String arg0, char arg1) {
        try {
            return arg0.charAt(0);
        } catch (Exception e) {
            return arg1;
        }
    }

    /**
     * _coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the object
     */
    final private static Object _coalesce(Object arg0, Object arg1) {
        return arg0 == null ? arg1 : arg0;
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the long
     */
    final public static Long coalesce(Long arg0, Long arg1) {
        return (Long) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the integer
     */
    final public static Integer coalesce(Integer arg0, Integer arg1) {
        return (Integer) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the short
     */
    final public static Short coalesce(Short arg0, Short arg1) {
        return (Short) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the double
     */
    final public static Double coalesce(Double arg0, Double arg1) {
        return (Double) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the float
     */
    final public static Float coalesce(Float arg0, Float arg1) {
        return (Float) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the string
     */
    final public static String coalesce(String arg0, String arg1) {
        return (String) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the object
     */
    final public static Object coalesce(Object arg0, Object arg1) {
        return ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the java.util. date
     */
    final public static java.util.Date coalesce(java.util.Date arg0, java.util.Date arg1) {
        return (java.util.Date) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the java.sql. date
     */
    final public static java.sql.Date coalesce(java.sql.Date arg0, java.sql.Date arg1) {
        return (Date) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the java.sql. timestamp
     */
    final public static java.sql.Timestamp coalesce(java.sql.Timestamp arg0, java.sql.Timestamp arg1) {
        return (Timestamp) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the java.sql. time
     */
    final public static java.sql.Time coalesce(java.sql.Time arg0, java.sql.Time arg1) {
        return (Time) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the byte[]
     */
    final public static byte[] coalesce(byte[] arg0, byte[] arg1) {
        return (byte[]) ETLCore._coalesce(arg0, arg1);
    }

    /**
     * Coalesce.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * 
     * @return the char[]
     */
    final public static char[] coalesce(char[] arg0, char[] arg1) {
        return (char[]) ETLCore._coalesce(arg0, arg1);
    }

    // scd lookups
    /**
     * Scd lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pEffectiveDate the effective date
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object scdLookup(String pLookupName, Object pKey1, java.util.Date pEffectiveDate)
            throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1 }), pEffectiveDate);
    }

    /**
     * Scd lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pKey2 the key2
     * @param pEffectiveDate the effective date
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object scdLookup(String pLookupName, Object pKey1, Object pKey2, java.util.Date pEffectiveDate)
            throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1, pKey2 }), pEffectiveDate);

    }

    /**
     * Scd lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pKey2 the key2
     * @param pKey3 the key3
     * @param pEffectiveDate the effective date
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object scdLookup(String pLookupName, Object pKey1, Object pKey2, Object pKey3,
            java.util.Date pEffectiveDate) throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1, pKey2, pKey3 }), pEffectiveDate);

    }

    /**
     * Scd lookup.
     * 
     * @param pLookupName the lookup name
     * @param pKey1 the key1
     * @param pKey2 the key2
     * @param pKey3 the key3
     * @param pKey4 the key4
     * @param pEffectiveDate the effective date
     * 
     * @return the object
     * 
     * @throws InterruptedException the interrupted exception
     * @throws KETLThreadException the KETL thread exception
     */
    final protected Object scdLookup(String pLookupName, Object pKey1, Object pKey2, Object pKey3, Object pKey4,
            java.util.Date pEffectiveDate) throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1, pKey2, pKey3, pKey4 }),
                pEffectiveDate);

    }

}
