package com.kni.etl.ketl.lookup;

import java.math.BigDecimal;
import java.util.Random;

import junit.framework.TestCase;

abstract public class IndexedMapTest extends TestCase {

    PersistentMap map;

    
    public void testPutMedium() {
        int i;
        map.clear();
        for (i = 0; i < 500000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        System.out.println("Inserts: " + i);

        int hits = 0;
        Random rmd = new Random();
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] { new Long(34 + rmd.nextInt(5000000)), new Float(43) };

           
            if (i > 0 & i % 50000 == 0) {
                System.out.println("Lookups: " + i + ", Hits:" + hits);
            }
            
            if ((Long) map.get(key, "a") != null)
                hits++;

        }

        System.out.println("Lookups: " + i);

        map.clear();

    }

    public void testPutSmall() {
        int i;
        map.clear();
        for (i = 0; i < 5000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        System.out.println("Inserts: " + i);
        for (i = 0; i < 5000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }

        int hits = 0;
        Random rmd = new Random();
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] { new Long(34 + rmd.nextInt(4500000)), new Float(43) };

           
            if (i > 0 & i % 50000 == 0) {
                System.out.println("Lookups: " + i + ", Hits: " + hits);
            }
            
            if ((Long) map.get(key, "a") != null)
                hits++;

        }

        System.out.println("Lookups: " + i);
        map.clear();

    }
    


    public void testPutLarge() {
        int i;
        long start = System.currentTimeMillis();
        map.clear();
        for (i = 0; i < 5000000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Inserts: " + i + " in " + (end - start)/1000 + " seconds");
        
        int hits = 0;
        Random rmd = new Random();
        start = System.currentTimeMillis();
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] { new Long(34 + rmd.nextInt(5000000)), new Float(43) };

            
            if (i > 0 & i % 50000 == 0) {
                System.out.println("Lookups: " + i + " Hits: " + hits);
            }
            
            if ((Long) map.get(key, "a") != null)
                hits++;

        }

        end = System.currentTimeMillis();
        System.out.println("Lookups: " + i + " in " + (end - start)/1000 + " seconds");
        map.clear();

    }

    public IndexedMapTest(String name) {
        super(name);
    }

}
