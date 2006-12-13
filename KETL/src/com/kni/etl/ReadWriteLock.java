/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


/**
 * Insert the type's description here.
 * Creation date: (4/17/2002 6:04:43 PM)
 * @author: Administrator
 */
public class ReadWriteLock implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 3546076973559265077L;
    public static boolean TRACE = false;
    private int givenLocks = 0;
    private int waitingWriters = 0;
    transient private Object mutex = new Object();

    public void getReadLock()
    {
        synchronized (mutex)
        {
            try
            {
                while ((givenLocks == -1) || (waitingWriters != 0))
                {
                    if (TRACE)
                    {
                        System.out.println(Thread.currentThread().toString() + "waiting for readlock");
                    }

                    mutex.wait();
                }
            }
            catch (java.lang.InterruptedException e)
            {
                System.out.println(e);
            }

            givenLocks++;

            if (TRACE)
            {
                System.out.println(Thread.currentThread().toString() + " got readlock, GivenLocks = " + givenLocks);
            }
        }
    }

    public void getWriteLock()
    {
        synchronized (mutex)
        {
            waitingWriters++;

            try
            {
                while (givenLocks != 0)
                {
                    if (TRACE)
                    {
                        System.out.println(Thread.currentThread().toString() + "waiting for writelock");
                    }

                    mutex.wait();
                }
            }
            catch (java.lang.InterruptedException e)
            {
                System.out.println(e);
            }

            waitingWriters--;
            givenLocks = -1;

            if (TRACE)
            {
                System.out.println(Thread.currentThread().toString() + " got writelock, GivenLocks = " + givenLocks);
            }
        }
    }

    public void releaseLock()
    {
        synchronized (mutex)
        {
            if (givenLocks == 0)
            {
                return;
            }

            if (givenLocks == -1)
            {
                givenLocks = 0;
            }
            else
            {
                givenLocks--;
            }

            if (TRACE)
            {
                System.out.println(Thread.currentThread().toString() + " released lock, GivenLocks = " + givenLocks);
            }

            mutex.notifyAll();
        }
    }

    public boolean isLocked()
    {
        boolean res = false;

        synchronized (mutex)
        {
            if (givenLocks != 0)
            {
                res = true;
            }

            if (TRACE)
            {
                System.out.println(Thread.currentThread().toString() + " checked for lock, GivenLocks = " + givenLocks);
            }
        }

        return res;
    }

    private void writeObject(ObjectOutputStream s) throws IOException
    {
        s.defaultWriteObject();
    }

    private void readObject(ObjectInputStream s) throws IOException
    {
        try
        {
            s.defaultReadObject();
            mutex = new Object();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }
}
