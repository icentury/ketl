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

import java.io.File;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;

import junit.framework.TestCase;

import com.kni.etl.EngineConstants;
import com.kni.etl.stringtools.NumberFormatter;

// TODO: Auto-generated Javadoc
/**
 * The Class SleepycatSimpleTest.
 */
public class SleepycatSimpleTest extends TestCase {

	/**
	 * Instantiates a new sleepycat simple test.
	 * 
	 * @param name
	 *            the name
	 */
	public SleepycatSimpleTest(String name) {
		super(name);
	}

	/** The map. */
	PersistentMap map = this.getMap();

	/**
	 * Test put SQL timestamp.
	 */
	public void testPutSQLTimestamp() {
		int i;
		this.map.clear();
		for (i = 0; i < 50000; i++) {
			Object[] key = new Object[] { i };

			java.sql.Timestamp tms = new java.sql.Timestamp(34000 + i);
			tms.setNanos(123);

			this.map.put(key, new Object[] { tms, new Float(43) });

			// Long l = (Long) map.get(key,"a");
			// Float f = (Float) map.get(key,"b");

			if (i > 0 & i % 50000 == 0) {
				System.out.println("Inserts: " + i);
			}
		}

		this.map.commit(true);

		int hits = 0;
		for (i = 0; i < 1000000; i++) {

			Object[] key = new Object[] { i };

			if (i > 0 & i % 50000 == 0) {
				System.out.println("Lookups: " + i + ", Hits: " + hits);
			}
			if ((this.map.get(key, "a")) != null)
				hits++;
		}

		System.out.println("Lookups: " + i);
		this.map.clear();

	}

	/**
	 * Test put big decimal.
	 */
	public void testPutBigDecimal() {
		int i;
		this.map.clear();
		for (i = 0; i < 50000; i++) {
			Object[] key = new Object[] { i };

			this.map.put(key, new Object[] { new BigDecimal(34.4553 + i), new Float(43) });

			// Long l = (Long) map.get(key,"a");
			// Float f = (Float) map.get(key,"b");

			if (i > 0 & i % 50000 == 0) {
				System.out.println("Inserts: " + i);
			}
		}

		this.map.commit(true);

		int hits = 0;
		for (i = 0; i < 1000000; i++) {

			Object[] key = new Object[] { i };

			if (i > 0 & i % 50000 == 0) {
				System.out.println("Lookups: " + i + ", Hits: " + hits);
			}
			if ((this.map.get(key, "a")) != null)
				hits++;
		}

		System.out.println("Lookups: " + i);
		this.map.clear();

	}

	/**
	 * Test put large simple.
	 */
	public void testPutLargeSimple() {
		int i;
		this.map.clear();
		Date st = new Date();
		System.out.println("Write");
		int vals = 1000000;
		for (i = 0; i < vals; i++) {
			Object[] key = new Object[] { i };

			this.map.put(key, new Object[] { new Long(34 + i), new Float(43) });

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

			Object res = this.map.get(new Object[] { i }, "a");

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

			Object res = this.map.get(new Object[] { rnd.nextInt(9000000) }, "a");

			if (i % 200000 == 0) {
				System.out.println("Count = " + i + ": " + res);
			}
			// printSubtree(dbl.root, "");
			// System.out.println("Finished");
		}

		time = (new Date().getTime() - st.getTime()) / (float) 1000;
		System.out.println("Read Done: " + time + ", " + vals / time + "rec/s");
		this.map.clear();

	}

	/**
	 * Gets the map.
	 * 
	 * @return the map
	 */
	PersistentMap getMap() {
		EngineConstants.getSystemXML();
		return new SleepycatIndexedMap("test" + this.getName(), NumberFormatter.convertToBytes(EngineConstants
				.getDefaultCacheSize()), 0, System.getProperty("user.dir") + File.separator + "log",
				new Class[] { Integer.class }, new Class[] { String.class }, new String[] { "a" }, false);
	}

}
