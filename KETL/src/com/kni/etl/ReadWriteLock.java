/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Insert the type's description here. Creation date: (4/17/2002 6:04:43 PM)
 * 
 * @author: Administrator
 */
public class ReadWriteLock implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 3546076973559265077L;
    public static boolean TRACE = false;
    private int givenLocks = 0;
    private int waitingWriters = 0;
    transient private Object mutex = new Object();

    public void getReadLock() {
        synchronized (this.mutex) {
            try {
                while ((this.givenLocks == -1) || (this.waitingWriters != 0)) {
                    if (ReadWriteLock.TRACE) {
                        System.out.println(Thread.currentThread().toString() + "waiting for readlock");
                    }

                    this.mutex.wait();
                }
            } catch (java.lang.InterruptedException e) {
                System.out.println(e);
            }

            this.givenLocks++;

            if (ReadWriteLock.TRACE) {
                System.out
                        .println(Thread.currentThread().toString() + " got readlock, GivenLocks = " + this.givenLocks);
            }
        }
    }

    public void getWriteLock() {
        synchronized (this.mutex) {
            this.waitingWriters++;

            try {
                while (this.givenLocks != 0) {
                    if (ReadWriteLock.TRACE) {
                        System.out.println(Thread.currentThread().toString() + "waiting for writelock");
                    }

                    this.mutex.wait();
                }
            } catch (java.lang.InterruptedException e) {
                System.out.println(e);
            }

            this.waitingWriters--;
            this.givenLocks = -1;

            if (ReadWriteLock.TRACE) {
                System.out.println(Thread.currentThread().toString() + " got writelock, GivenLocks = "
                        + this.givenLocks);
            }
        }
    }

    public void releaseLock() {
        synchronized (this.mutex) {
            if (this.givenLocks == 0) {
                return;
            }

            if (this.givenLocks == -1) {
                this.givenLocks = 0;
            }
            else {
                this.givenLocks--;
            }

            if (ReadWriteLock.TRACE) {
                System.out.println(Thread.currentThread().toString() + " released lock, GivenLocks = "
                        + this.givenLocks);
            }

            this.mutex.notifyAll();
        }
    }

    public boolean isLocked() {
        boolean res = false;

        synchronized (this.mutex) {
            if (this.givenLocks != 0) {
                res = true;
            }

            if (ReadWriteLock.TRACE) {
                System.out.println(Thread.currentThread().toString() + " checked for lock, GivenLocks = "
                        + this.givenLocks);
            }
        }

        return res;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
    }

    private void readObject(ObjectInputStream s) throws IOException {
        try {
            s.defaultReadObject();
            this.mutex = new Object();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
