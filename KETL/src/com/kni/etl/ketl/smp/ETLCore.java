package com.kni.etl.ketl.smp;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Calendar;
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

abstract public class ETLCore implements DefaultCore {

    private ETLWorker ownerStep;

    protected abstract void initializeCoreFields();

    public ETLWorker getOwner() {
        return ownerStep;
    }

    public void setOwner(ETLWorker arg0) {
        ownerStep = arg0;
        this.initializeCoreFields();
    }

    final protected Object lookupD(String pLookupName, Object pKey1, String pField, Object defaultValue)
            throws InterruptedException, KETLThreadException {
        Object res = lookup(pLookupName, pKey1, pField);
        if (res == null)
            return defaultValue;

        return res;
    }

    final protected Object sendEmail(String subject, String text) {

        try {
            Metadata md = ResourcePool.getMetadata();
            md.sendEmail(((ETLStep) ownerStep).getJobExecutor().getCurrentETLJob().getJobID(), subject, text);
            return true;
        } catch (Exception e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, e.getMessage());
            return false;
        }
    }

    final protected java.sql.Timestamp toTimestamp(long time, int nanos) {
        java.sql.Timestamp tms = new java.sql.Timestamp(time);
        tms.setNanos(nanos);
        return tms;
    }

    final protected java.sql.Time toTime(long time) {
        java.sql.Time tms = new java.sql.Time(time);
        return tms;
    }

    GregorianCalendar mGregorianCalendar = new GregorianCalendar();

    final protected Integer getDatePart(java.util.Date date, int field) {

        if (date == null)
            return null;

        this.mGregorianCalendar.setTime(date);
        return mGregorianCalendar.get(field);
    }

    final protected Object generateException(String message, boolean critical) throws KETLException {
        if (critical)
            throw new KETLError(message);

        throw new KETLException(message);
    }

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

    private HashMap mLocalLookups = new HashMap();

    final private PersistentMap getLookup(String pLookupName) throws InterruptedException, KETLThreadException {

        Object res = this.mLocalLookups.get(pLookupName);

        if (res == null) {
            res = ((KETLJob) ((ETLStep) this.getOwner()).getJobExecutor().getCurrentETLJob()).getSharedLookup(
                    pLookupName, this);
            this.mLocalLookups.put(pLookupName, res);
        }
        return (PersistentMap) res;
    }

    protected final static short toShort(String arg0, short arg1) {
        try {
            return Short.parseShort(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static int toInt(String arg0, int arg1) {
        try {
            return Integer.parseInt(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static long toLong(String arg0, long arg1) {
        try {
            return Long.parseLong(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static float toFloat(String arg0, float arg1) {
        try {
            return Float.parseFloat(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static double toDouble(String arg0, double arg1) {
        try {
            return Double.parseDouble(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static java.util.Date toDate(String arg0, String arg1, String format) throws ParseException {
        FastSimpleDateFormat fmt = new FastSimpleDateFormat(format);
        try {
            return fmt.parse(arg0);
        } catch (Exception e) {
            return fmt.parse(arg1);
        }
    }

    protected final static boolean toBoolean(String arg0, boolean arg1) {
        try {
            return Boolean.parseBoolean(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static byte toByte(String arg0, byte arg1) {
        try {
            return Byte.parseByte(arg0);
        } catch (Exception e) {
            return arg1;
        }
    }

    protected final static char toChar(String arg0, char arg1) {
        try {
            return arg0.charAt(0);
        } catch (Exception e) {
            return arg1;
        }
    }

    final private static Object _coalesce(Object arg0, Object arg1) {
        return arg0 == null ? arg1 : arg0;
    }

    final public static Long coalesce(Long arg0, Long arg1) {
        return (Long) _coalesce(arg0, arg1);
    }

    final public static Integer coalesce(Integer arg0, Integer arg1) {
        return (Integer) _coalesce(arg0, arg1);
    }

    final public static Short coalesce(Short arg0, Short arg1) {
        return (Short) _coalesce(arg0, arg1);
    }

    final public static Double coalesce(Double arg0, Double arg1) {
        return (Double) _coalesce(arg0, arg1);
    }

    final public static Float coalesce(Float arg0, Float arg1) {
        return (Float) _coalesce(arg0, arg1);
    }

    final public static String coalesce(String arg0, String arg1) {
        return (String) _coalesce(arg0, arg1);
    }

    final public static Object coalesce(Object arg0, Object arg1) {
        return _coalesce(arg0, arg1);
    }

    final public static java.util.Date coalesce(java.util.Date arg0, java.util.Date arg1) {
        return (java.util.Date) _coalesce(arg0, arg1);
    }

    final public static java.sql.Date coalesce(java.sql.Date arg0, java.sql.Date arg1) {
        return (Date) _coalesce(arg0, arg1);
    }

    final public static java.sql.Timestamp coalesce(java.sql.Timestamp arg0, java.sql.Timestamp arg1) {
        return (Timestamp) _coalesce(arg0, arg1);
    }

    final public static java.sql.Time coalesce(java.sql.Time arg0, java.sql.Time arg1) {
        return (Time) _coalesce(arg0, arg1);
    }

    final public static byte[] coalesce(byte[] arg0, byte[] arg1) {
        return (byte[]) _coalesce(arg0, arg1);
    }

    final public static char[] coalesce(char[] arg0, char[] arg1) {
        return (char[]) _coalesce(arg0, arg1);
    }

    // scd lookups
    final protected Object scdLookup(String pLookupName, Object pKey1, java.util.Date pEffectiveDate)
            throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1 }), pEffectiveDate);
    }

    final protected Object scdLookup(String pLookupName, Object pKey1, Object pKey2, java.util.Date pEffectiveDate)
            throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1, pKey2 }), pEffectiveDate);

    }

    final protected Object scdLookup(String pLookupName, Object pKey1, Object pKey2, Object pKey3,
            java.util.Date pEffectiveDate) throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1, pKey2, pKey3 }), pEffectiveDate);

    }

    final protected Object scdLookup(String pLookupName, Object pKey1, Object pKey2, Object pKey3, Object pKey4,
            java.util.Date pEffectiveDate) throws InterruptedException, KETLThreadException {
        PersistentMap mp = this.getLookup(pLookupName);
        return SCDValue.getNearestSCDValue((SCDValue) mp.get(new Object[] { pKey1, pKey2, pKey3, pKey4 }),
                pEffectiveDate);

    }

}
