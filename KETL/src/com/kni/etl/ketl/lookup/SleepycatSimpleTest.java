package com.kni.etl.ketl.lookup;

import java.io.File;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;

import junit.framework.TestCase;

import com.kni.etl.EngineConstants;
import com.kni.etl.stringtools.NumberFormatter;

public class SleepycatSimpleTest extends TestCase {

    public SleepycatSimpleTest(String name) {
        super(name);
    }

    PersistentMap map = this.getMap();

    public void testPutSQLTimestamp() {
        int i;
        map.clear();
        for (i = 0; i < 50000; i++) {
            Object[] key = new Object[] { i };

            java.sql.Timestamp tms = new java.sql.Timestamp(34000 + i);
            tms.setNanos(123);

            map.put(key, new Object[] { tms, new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }

        map.commit(true);

        int hits = 0;
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] { i };

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Lookups: " + i + ", Hits: " + hits);
            }
            Object obj;
            if ((obj = map.get(key, "a")) != null)
                hits++;
            obj = null;
        }

        System.out.println("Lookups: " + i);
        map.clear();

    }

    public void testPutBigDecimal() {
        int i;
        map.clear();
        for (i = 0; i < 50000; i++) {
            Object[] key = new Object[] { i };

            map.put(key, new Object[] { new BigDecimal(34.4553 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }

        map.commit(true);

        int hits = 0;
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] { i };

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Lookups: " + i + ", Hits: " + hits);
            }
            Object obj;
            if ((obj = map.get(key, "a")) != null)
                hits++;
            obj = null;
        }

        System.out.println("Lookups: " + i);
        map.clear();

    }

    public void testPutLargeSimple() {
        int i;
        map.clear();
        Date st = new Date();
        System.out.println("Write");
        int vals = 1000000;
        for (i = 0; i < vals; i++) {
            Object[] key = new Object[] { i };

            map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        float time = (new Date().getTime() - st.getTime()) / (float) 1000;

        System.out.println("Write Done: " + time + ", " + vals / time + "rec/s");

        st = new Date();
        // System.out.println(printSubtree(dbl.root, ""));
        System.out.println("Read");
        long lastTime = st.getTime();
        for (i = 0; i < vals; i++) {

            if (i % 1000 == 0) {
                long now = System.currentTimeMillis();
                if ((now - lastTime) > 5000) {
                    time = (now - st.getTime()) / (float) 1000;
                    System.out.println("Read rate: " + i / time + "rec/s");
                    lastTime = now;
                }
            }

            Object res = map.get(new Object[] { i }, "a");

            if (i % 200000 == 0) {
                System.out.println("Count = " + i + ": " + res);
            }
            // printSubtree(dbl.root, "");
            // System.out.println("Finished");
        }

        time = (new Date().getTime() - st.getTime()) / (float) 1000;
        System.out.println("Read Done: " + time + ", " + vals / time + "rec/s");

        Random rnd = new Random();
        st = new Date();
        // System.out.println(printSubtree(dbl.root, ""));
        System.out.println("Read");
        lastTime = st.getTime();
        for (i = 0; i < vals; i++) {

            if (i % 1000 == 0) {
                long now = System.currentTimeMillis();
                if ((now - lastTime) > 5000) {
                    time = (now - st.getTime()) / (float) 1000;
                    System.out.println("Read rate: " + i / time + "rec/s");
                    lastTime = now;
                }
            }

            Object res = map.get(new Object[] { rnd.nextInt(9000000) }, "a");

            if (i % 200000 == 0) {
                System.out.println("Count = " + i + ": " + res);
            }
            // printSubtree(dbl.root, "");
            // System.out.println("Finished");
        }

        time = (new Date().getTime() - st.getTime()) / (float) 1000;
        System.out.println("Read Done: " + time + ", " + vals / time + "rec/s");
        map.clear();

    }

    PersistentMap getMap() {
        EngineConstants.getSystemXML();
        return new SleepycatIndexedMap("test" + this.getName(), NumberFormatter.convertToBytes(EngineConstants
                .getDefaultCacheSize()), 0, System.getProperty("user.dir") + File.separator + "log",
                new Class[] { Integer.class }, new Class[] { String.class }, new String[] { "a" }, false);
    }

}
