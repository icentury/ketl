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

import java.math.BigDecimal;
import java.util.Random;

import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class DataTypeIndexedMapTest.
 */
abstract public class DataTypeIndexedMapTest extends TestCase {

	/** The map. */
	PersistentMap map;

	/**
	 * Test put big decimal.
	 */
	public void testPutBigDecimal() {
		int i;
		this.map.clear();
		for (i = 0; i < 50000; i++) {
			Object[] key = new Object[] { new BigDecimal(34.4553 + i), new Float(43) };

			this.map.put(key, new Object[] { new BigDecimal(34.4553 + i), new Float(43) });

			// Long l = (Long) map.get(key,"a");
			// Float f = (Float) map.get(key,"b");

			if (i > 0 & i % 50000 == 0) {
				System.out.println("Inserts: " + i);
			}
		}
		System.out.println("Inserts: " + i);
		for (i = 0; i < 5000; i++) {
			Object[] key = new Object[] { new BigDecimal(34.4553 + i), new Float(43) };

			this.map.put(key, new Object[] { new BigDecimal(34.4553 + i), new Float(43) });

			// Long l = (Long) map.get(key,"a");
			// Float f = (Float) map.get(key,"b");

			if (i > 0 & i % 50000 == 0) {
				System.out.println("Inserts: " + i);
			}
		}

		this.map.commit(true);

		int hits = 0;
		Random rmd = new Random();
		for (i = 0; i < 1000000; i++) {

			Object[] key = new Object[] { new BigDecimal(34.4553 + rmd.nextInt(45000)), new Float(43) };

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
	 * Instantiates a new data type indexed map test.
	 * 
	 * @param name
	 *            the name
	 */
	public DataTypeIndexedMapTest(String name) {
		super(name);
	}

}
