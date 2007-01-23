package com.kni.etl.ketl.lookup;

import java.io.File;
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
 
    public void testPutLargeSimple() {
        int i;
        map.clear();
        Date st = new Date();
        System.out.println("Write");
        int vals = 8000000;
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
        return new SleepycatIndexedMap("test" + this.getName(), NumberFormatter.convertToBytes(EngineConstants.getDefaultCacheSize()), 0,
                System.getProperty("user.dir") + File.separator + "log", new Class[] { Integer.class },
                new Class[] { String.class }, new String[] { "a" }, false);
    }

}
