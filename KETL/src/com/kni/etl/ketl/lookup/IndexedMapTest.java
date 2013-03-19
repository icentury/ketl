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

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class IndexedMapTest.
 */
abstract public class IndexedMapTest extends TestCase {

    /**
     * Gets the map.
     * 
     * @return the map
     */
    abstract PersistentMap getMap();

    /** The map. */
    PersistentMap map = this.getMap();

    /*
     * public void testPutLargeSimple() { int i; map.clear(); Date st = new Date(); System.out.println("Write"); int
     * vals = 8000000; for (i = 0; i < vals; i++) { Object[] key = new Object[] { new Long(34 + i), new Float(43) };
     * map.put(key, new Object[] { new Long(34 + i), new Float(43) }); if (i > 0 & i % 50000 == 0) {
     * System.out.println("Inserts: " + i); } } float time = (new Date().getTime() - st.getTime()) / (float) 1000;
     * System.out.println("Write Done: " + time + ", " + vals / time + "rec/s"); st = new Date(); //
     * System.out.println(printSubtree(dbl.root, "")); System.out.println("Read"); long lastTime = st.getTime(); for (i =
     * 0; i < vals; i++) { if (i % 1000 == 0) { long now = System.currentTimeMillis(); if ((now - lastTime) > 5000) {
     * time = (now - st.getTime()) / (float) 1000; System.out.println("Read rate: " + i / time + "rec/s"); lastTime =
     * now; } } Object res = map.get(new Object[] { new Long(34 + i), new Float(43) }, "a"); if (i % 200000 == 0) {
     * System.out.println("Count = " + i + ": " + res); } // printSubtree(dbl.root, ""); //
     * System.out.println("Finished"); } time = (new Date().getTime() - st.getTime()) / (float) 1000;
     * System.out.println("Read Done: " + time + ", " + vals / time + "rec/s"); Random rnd = new Random(); st = new
     * Date(); // System.out.println(printSubtree(dbl.root, "")); System.out.println("Read"); lastTime = st.getTime();
     * for (i = 0; i < vals; i++) { if (i % 1000 == 0) { long now = System.currentTimeMillis(); if ((now - lastTime) >
     * 5000) { time = (now - st.getTime()) / (float) 1000; System.out.println("Read rate: " + i / time + "rec/s");
     * lastTime = now; } } Object res = map.get(new Object[] { new Long(34 + rnd.nextInt(9000000)), new Float(43) },
     * "a"); if (i % 200000 == 0) { System.out.println("Count = " + i + ": " + res); } // printSubtree(dbl.root, ""); //
     * System.out.println("Finished"); } time = (new Date().getTime() - st.getTime()) / (float) 1000;
     * System.out.println("Read Done: " + time + ", " + vals / time + "rec/s"); map.clear(); }
     */
    /**
     * Test put medium.
     * @throws IOException 
     */
    public void testPutMedium() throws IOException {

        int i;
        this.map.clear();
        for (i = 0; i < 500000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            this.map.put(key, new Object[] { new Long(34 + i), new Float(43) });

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

            if ((Long) this.map.get(key, "a") != null)
                hits++;

        }

        System.out.println("Lookups: " + i);

        this.map.clear();

    }

    /**
     * Test put small.
     * @throws IOException 
     */
    public void testPutSmall() throws IOException {
        int i;
        this.map.clear();
        for (i = 0; i < 5000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            this.map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        System.out.println("Inserts: " + i);
        for (i = 0; i < 5000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            this.map.put(key, new Object[] { new Long(34 + i), new Float(43) });

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

            if ((Long) this.map.get(key, "a") != null)
                hits++;

        }

        System.out.println("Lookups: " + i);
        this.map.clear();

    }

    /**
     * Test put large.
     * @throws IOException 
     */
    public void testPutLarge() throws IOException {
        int i;
        long start = System.currentTimeMillis();
        this.map.clear();
        for (i = 0; i < 5000000; i++) {
            Object[] key = new Object[] { new Long(34 + i), new Float(43) };

            this.map.put(key, new Object[] { new Long(34 + i), new Float(43) });

            // Long l = (Long) map.get(key,"a");
            // Float f = (Float) map.get(key,"b");

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Inserts: " + i);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Inserts: " + i + " in " + (end - start) / 1000 + " seconds");

        int hits = 0;
        Random rmd = new Random();
        start = System.currentTimeMillis();
        for (i = 0; i < 1000000; i++) {

            Object[] key = new Object[] { new Long(34 + rmd.nextInt(5000000)), new Float(43) };

            if (i > 0 & i % 50000 == 0) {
                System.out.println("Lookups: " + i + " Hits: " + hits);
            }

            if ((Long) this.map.get(key, "a") != null)
                hits++;

        }

        end = System.currentTimeMillis();
        System.out.println("Lookups: " + i + " in " + (end - start) / 1000 + " seconds");
        this.map.clear();

    }

    /**
     * Instantiates a new indexed map test.
     * 
     * @param name the name
     */
    public IndexedMapTest(String name) {
        super(name);
    }

}
