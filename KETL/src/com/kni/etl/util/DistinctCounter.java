/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

final public class DistinctCounter {

    int counters = 2;
    Map[] mCounters = new Map[this.counters + 1];
    static final int HASHMAP_MAXSIZE = 1000;
    static final int TREEMAP_MAXSIZE = 10000;
    int currentListMaxSize = DistinctCounter.HASHMAP_MAXSIZE;

    final public void reset() {
        for (Map element : this.mCounters) {
            if (!(element == null)) {
                element.clear();
            }
        }
    }

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
