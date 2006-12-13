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


final public class DistinctCounter
{
    int counters = 2;
    Map[] mCounters = new Map[counters + 1];
    static final int HASHMAP_MAXSIZE = 1000;
    static final int TREEMAP_MAXSIZE = 10000;
    int currentListMaxSize = HASHMAP_MAXSIZE;

    final public void reset()
    {
        for (int i = 0; i < mCounters.length; i++)
        {
            if (!(mCounters[i] == null))
            {
                mCounters[i].clear();
            }
        }
    }

    final public int add(Object pRecord, int pPortID) throws Exception
    {
        if (pPortID > counters)
        {
            Map[] tmp = new Map[pPortID + 1];
            counters = pPortID + 1;
            System.arraycopy(this.mCounters, 0, tmp, 0, tmp.length);
            this.mCounters = tmp;
        }

        Map list = mCounters[pPortID];

        if (list == null)
        {
            list = new HashMap();
            mCounters[pPortID] = list;
            list.put(pRecord, null);

            return 1;
        }

        list.put(pRecord, null);

        int size = list.size();

        if (currentListMaxSize == TREEMAP_MAXSIZE && size > TREEMAP_MAXSIZE )
        {
            throw new Exception("Disk backed unique list still pending");
        }
        else if (currentListMaxSize == HASHMAP_MAXSIZE && size > HASHMAP_MAXSIZE)
        {
            currentListMaxSize = TREEMAP_MAXSIZE;
            mCounters[pPortID] = new TreeMap(list);
        }

        return size;
    }

    final public int count(int pPortID) throws Exception
    {
        if (pPortID > counters)
        {
            Map[] tmp = new Map[pPortID + 1];
            counters = pPortID + 1;
            System.arraycopy(this.mCounters, 0, tmp, 0, tmp.length);
            this.mCounters = tmp;
        }

        Map list = mCounters[pPortID];

        if (list == null)
        {
            list = new HashMap();
            mCounters[pPortID] = list;

            return 0;
        }

        return list.size();
    }
}
