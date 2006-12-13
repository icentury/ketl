package com.kni.etl.ketl.lookup;

import java.math.BigDecimal;
import java.util.Random;

import junit.framework.TestCase;

abstract public class DataTypeIndexedMapTest extends TestCase {

    PersistentMap map;

    
        
    public void testPutBigDecimal() {
        int i;
        map.clear();
        for (i = 0; i < 50000; i++) {
            Object[] key = new Object[] {  new BigDecimal(34.4553+i), new Float(43) };

            map.put(key, new Object[] { new BigDecimal(34.4553+i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        System.out.println("Inserts: " + i);
        for (i = 0; i < 5000; i++) {
            Object[] key = new Object[] {  new BigDecimal(34.4553+i), new Float(43) };

            map.put(key, new Object[] {  new BigDecimal(34.4553+i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }

        map.commit(true);
        
        int hits = 0;
        Random rmd = new Random();
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] {  new BigDecimal(34.4553 + rmd.nextInt(45000)), new Float(43) };

           
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

    
    public DataTypeIndexedMapTest(String name) {
        super(name);
    }

}
