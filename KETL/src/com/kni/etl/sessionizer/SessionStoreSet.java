/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.io.Serializable;
import java.util.List;

import com.kni.etl.KNIHashMap;
import com.kni.etl.ReadWriteLock;


public class SessionStoreSet implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 3544390301348214320L;
    private KNIHashMap HashMapToMonitor;
    private ReadWriteLock HashMapReadWriteLock;
    private java.util.Date CurrentDate;
    private int AssociatedAlgorithmCode = 0;
    private boolean EnableFallBack = false;
    private List RemovedSessionsQueue;
    public int LastMatchTimeOut;
    public int SessionTimeOut;

    /**
     * SessionStoreBackgroundThread constructor comment.
     * @param pDestinationThread TODO
     */
    public SessionStoreSet(KNIHashMap pHashMapToMonitor, ReadWriteLock pHashMapReadWriteLock,
        java.util.Date pDate, int pSessionTimeOut, int pLastMatchTimeOut, int pAssociatedAlgorithmCode,
        List pRemovedSessionsQueue, boolean pKeepFallBack)
    {
        super();

        //this.setPriority(MIN_PRIORITY);
        this.CurrentDate = pDate;
        this.SessionTimeOut = pSessionTimeOut;
        this.HashMapToMonitor = pHashMapToMonitor;
        this.HashMapReadWriteLock = pHashMapReadWriteLock;
        this.AssociatedAlgorithmCode = pAssociatedAlgorithmCode;
        this.EnableFallBack = pKeepFallBack;
        this.LastMatchTimeOut = pLastMatchTimeOut;
        this.RemovedSessionsQueue = pRemovedSessionsQueue;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 11:34:59 PM)
     * @param pDontExpireSessions if true then non closed sessions will have null last
     * activity
     * @return int
     * @throws InterruptedException
     */
    public int closeOutAllSessions(boolean pDontExpireSessions)      
    {
        int removed = 0;

        this.HashMapReadWriteLock.getReadLock();

        // scan hashmap
        java.util.Set keySet = this.HashMapToMonitor.keySet();

        /* An iterator is used to move through the Keyset */
        Object[] sessionArray = keySet.toArray();

        for (Object element : sessionArray) {
            /* Get the individual key. We need to hold on to this key in case it needs to be removed */
            String key = (String) element;
            Session sessionToCheck = (Session) this.HashMapToMonitor.get(key);

            /* Is the cacheable object expired? */
            if (sessionToCheck != null)
            {
                // switch to write lock
                this.HashMapReadWriteLock.releaseLock();
                this.HashMapReadWriteLock.getWriteLock();

                // remove session, session expired
                keySet.remove(key);

                // release write lock
                this.HashMapReadWriteLock.releaseLock();

                // deindex session for this hashmap
                sessionToCheck.expireForCode(this.AssociatedAlgorithmCode);

                // Now check to if session not indexed anymore, if so add to done queue            
                if (sessionToCheck.isExpired() == true)
                {
                    if (pDontExpireSessions)
                    {
                        sessionToCheck.LastActivity = null;
                    }

                    this.RemovedSessionsQueue.add(sessionToCheck);
                }

                removed++;

                // switch to read only lock
                this.HashMapReadWriteLock.getReadLock();
            }

            /* if true release read lock and switch to write lock */
            /* when modification made release write lock and switch to read lock */
        }

        // release read lock
        this.HashMapReadWriteLock.releaseLock();

        return (removed);
    }

    public boolean validateSession(String key, java.util.Date activityDate, Session sessionToCheck, boolean invalidate)
        throws InterruptedException
    {
        boolean result = true;
        this.HashMapReadWriteLock.getReadLock();

        /* Is the cacheable object expired? */
        if ((sessionToCheck != null) &&
                ((invalidate == true) ||
                (sessionToCheck.isStillValid(activityDate, this.SessionTimeOut, this.LastMatchTimeOut,
                    this.AssociatedAlgorithmCode, this.EnableFallBack) == false)))
        {
            this.HashMapReadWriteLock.releaseLock();
            this.HashMapReadWriteLock.getWriteLock();

            // remove session, session expired
            this.HashMapToMonitor.remove(key);

            result = false;
            this.HashMapReadWriteLock.releaseLock();

            // deindex session for this hashmap
            sessionToCheck.expireForCode(this.AssociatedAlgorithmCode);

            // Now check to if session not indexed anymore, if so add to done queue            
            if (sessionToCheck.isExpired() == true)
            {
                this.RemovedSessionsQueue.add(sessionToCheck);
            }

            this.HashMapReadWriteLock.getReadLock();
        }

        // release read lock
        this.HashMapReadWriteLock.releaseLock();

        return result;
    }

    public int findInvalidSessions() throws Exception
    { // lock hashmap for reading

        int removed = 0;

        this.HashMapReadWriteLock.getReadLock();

        // scan hashmap
        java.util.Set keySet = this.HashMapToMonitor.keySet();

        /* An iterator is used to move through the Keyset */
        Object[] sessionArray = keySet.toArray();

        for (Object element : sessionArray) {
            /* Get the individual key. We need to hold on to this key in case it needs to be removed */
            String key = (String) element;
            Session sessionToCheck = (Session) this.HashMapToMonitor.get(key);

            /* Get the cacheable object associated with the key inside the cache */

            //Session sessionToCheck = (Session) HashMapToMonitor.get(key);

            /* Is the cacheable object expired? */
            if ((sessionToCheck != null) &&
                    (sessionToCheck.isStillValid(this.CurrentDate, this.SessionTimeOut, this.LastMatchTimeOut,
                        this.AssociatedAlgorithmCode, this.EnableFallBack) == false))
            {
                this.HashMapReadWriteLock.releaseLock();
                this.HashMapReadWriteLock.getWriteLock();

                // System.out.println("HashMap monitor thread: Removing timed out session->" + sessionToCheck.LastActivity + ", " + sessionToCheck.LastActivity);
                // System.out.println(".");
                // remove session, session expired
                keySet.remove(key);

                this.HashMapReadWriteLock.releaseLock();

                // deindex session for this hashmap
                sessionToCheck.expireForCode(this.AssociatedAlgorithmCode);

                // Now check to if session not indexed anymore, if so add to done queue            
                if (sessionToCheck.isExpired() == true)
                {
                    this.RemovedSessionsQueue.add(sessionToCheck);
                }

                removed++;

                this.HashMapReadWriteLock.getReadLock();
            }

            /* if true release read lock and switch to write lock */
            /* when modification made release write lock and switch to read lock */
        }

        // release read lock
        this.HashMapReadWriteLock.releaseLock();

        return (removed);
    }



    public void setHashMapToMonitor(KNIHashMap pNewHashMapToMonitor, ReadWriteLock pHashMapReadWriteLock)
    {
        this.HashMapToMonitor = pNewHashMapToMonitor;
        this.HashMapReadWriteLock = pHashMapReadWriteLock;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/18/2002 9:58:44 AM)
     */
    public void stopScanning()
    {
        this.HashMapToMonitor = null;
    }
}
