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
/*
 * Created on Aug 26, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

// TODO: Auto-generated Javadoc
/**
 * The Class DistinctCounter.
 */
final public class DistinctCounter {

    /** The counters. */
    int counters = 2;
    
    /** The counters. */
    Map[] mCounters = new Map[this.counters + 1];
    
    /** The Constant HASHMAP_MAXSIZE. */
    static final int HASHMAP_MAXSIZE = 1000;
    
    /** The Constant TREEMAP_MAXSIZE. */
    static final int TREEMAP_MAXSIZE = 10000;
    
    /** The current list max size. */
    int currentListMaxSize = DistinctCounter.HASHMAP_MAXSIZE;

    /**
     * Reset.
     */
    final public void reset() {
        for (Map element : this.mCounters) {
            if (!(element == null)) {
                element.clear();
            }
        }
    }

    /**
     * Add.
     * 
     * @param pRecord the record
     * @param pPortID the port ID
     * 
     * @return the int
     * 
     * @throws Exception the exception
     */
    final public int add(Object pRecord, int pPortID) throws Exception {
        if (pPortID > this.counters) {
            Map[] tmp = new Map[pPortID + 1];
            this.counters = pPortID + 1;
            System.arraycopy(this.mCounters, 0, tmp, 0, tmp.length);
            this.mCounters = tmp;
        }

        Map list = this.mCounters[pPortID];

        if (list == null) {
            list = new HashMap();
            this.mCounters[pPortID] = list;
            list.put(pRecord, null);

            return 1;
        }

        list.put(pRecord, null);

        int size = list.size();

        if (this.currentListMaxSize == DistinctCounter.TREEMAP_MAXSIZE && size > DistinctCounter.TREEMAP_MAXSIZE) {
            throw new Exception("Disk backed unique list still pending");
        }
        else if (this.currentListMaxSize == DistinctCounter.HASHMAP_MAXSIZE && size > DistinctCounter.HASHMAP_MAXSIZE) {
            this.currentListMaxSize = DistinctCounter.TREEMAP_MAXSIZE;
            this.mCounters[pPortID] = new TreeMap(list);
        }

        return size;
    }

    /**
     * Count.
     * 
     * @param pPortID the port ID
     * 
     * @return the int
     * 
     * @throws Exception the exception
     */
    final public int count(int pPortID) throws Exception {
        if (pPortID > this.counters) {
            Map[] tmp = new Map[pPortID + 1];
            this.counters = pPortID + 1;
            System.arraycopy(this.mCounters, 0, tmp, 0, tmp.length);
            this.mCounters = tmp;
        }

        Map list = this.mCounters[pPortID];

        if (list == null) {
            list = new HashMap();
            this.mCounters[pPortID] = list;

            return 0;
        }

        return list.size();
    }
}
