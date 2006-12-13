/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jun 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.sessionizer;

import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;


/**
 * @author nwakefield
 * Creation Date: Jun 17, 2003
 */
public class IDCounter
{
    /**
     *
     */
    private static final long serialVersionUID = 3257849878729340726L;
    private long id = 0;
    private long batchSize = 0;
    transient private Metadata md = null;
    private long idsLeft = 0;
    private String IDName;

    public IDCounter()
    {
        super();
    }

    public IDCounter(String strIDName, long param) throws Exception
    {
        super();
        batchSize = param;
        md = ResourcePool.getMetadata();
        idsLeft = batchSize;

        // Get max temp session id
        IDName = strIDName;

        try
        {
            //System.out.println("IDS " + Thread.currentThread().getName()  + ": ID in " + id + " batchsize:" + batchSize);
            id = md.getBatchOfIDValues(IDName, batchSize);

            //System.out.println("IDS " + Thread.currentThread().getName()  + ": ID out " + id + " batchsize:" + batchSize);
        }
        catch (Exception e)
        {
            md = null;
        }

        if (md == null)
        {
            ResourcePool.LogMessage(this, "Sequence " + strIDName + " defaulting to 0 as metadata not available!");
            id = 0;
        }
    }

    public synchronized final long incrementID()
    {
        idsLeft--;

        if (idsLeft <= 0)
        {
            if (md != null)
            {
                try
                {
                    //System.out.println("IDS " + Thread.currentThread().getName()  + ": ID in " + id + " batchsize:" + batchSize);
                    id = md.getBatchOfIDValues(IDName, batchSize);

                    //System.out.println("IDS " + Thread.currentThread().getName()  + ": ID out " + id + " batchsize:" + batchSize);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            idsLeft = batchSize - 1;
        }

        return id++;
    }

    public final long setToCurrentID()
    {
        if (md != null)
        {
            md.setMaxIDValue(IDName, id++);
        }

        return id;
    }

    public final long getID()
    {
        return id;
    }

    public final String toString()
    {
        return Long.toString(id);
    }
}
