/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.io.Serializable;
import java.util.Vector;

import com.kni.etl.KNIHashMap;
import com.kni.etl.ReadWriteLock;


public class AnalyzeSessionStoreSessions implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 3544390301348214320L;
    private KNIHashMap HashMapToMonitor;
    private int SleepTime = 5000;
    private ReadWriteLock HashMapReadWriteLock;
    private java.util.Date CurrentDate;
    public int LastMatchTimeOut;
    public int SessionTimeOut;
    private int AssociatedAlgorithmCode = 0;
    private boolean EnableFallBack = false;
    private Vector RemovedSessionsQueue;

    /**
     * SessionStoreBackgroundThread constructor comment.
     */
    public AnalyzeSessionStoreSessions(KNIHashMap pHashMapToMonitor, ReadWriteLock pHashMapReadWriteLock,
        java.util.Date pDate, int pSleepTime, int pSessionTimeOut, int pLastMatchTimeOut, int pAssociatedAlgorithmCode,
        Vector pRemovedSessionsQueue, boolean pKeepFallBack)
    {
        super();

        //this.setPriority(MIN_PRIORITY);
        CurrentDate = pDate;
        SleepTime = pSleepTime;
        SessionTimeOut = pSessionTimeOut;
        HashMapToMonitor = pHashMapToMonitor;
        HashMapReadWriteLock = pHashMapReadWriteLock;
        AssociatedAlgorithmCode = pAssociatedAlgorithmCode;
        EnableFallBack = pKeepFallBack;
        LastMatchTimeOut = pLastMatchTimeOut;
        RemovedSessionsQueue = pRemovedSessionsQueue;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 11:34:59 PM)
     * @param pLastActivityNull if true then non closed sessions will have null last
     * activity
     * @return int
     */
    public int closeOutAllSessions(boolean pLastActivityNull)
    {
        int removed = 0;

        HashMapReadWriteLock.getReadLock();

        // scan hashmap
        java.util.Set keySet = HashMapToMonitor.keySet();

        /* An iterator is used to move through the Keyset */
        Object[] sessionArray = keySet.toArray();

        /* Sets up a loop that will iterate through each key in the KeySet */
        for (int index = 0; index < sessionArray.length; index++)
        {
            /* Get the individual key. We need to hold on to this key in case it needs to be removed */
            String key = (String) sessionArray[index];
            Session sessionToCheck = (Session) HashMapToMonitor.get(key);

            /* Is the cacheable object expired? */
            if (sessionToCheck != null)
            {
                // switch to write lock
                HashMapReadWriteLock.releaseLock();
                HashMapReadWriteLock.getWriteLock();

                // remove session, session expired
                keySet.remove(key);

                // release write lock
                HashMapReadWriteLock.releaseLock();

                // deindex session for this hashmap
                sessionToCheck.expireForCode(AssociatedAlgorithmCode);

                // Now check to if session not indexed anymore, if so add to done queue            
                if (sessionToCheck.isExpired() == true)
                {
                    if (pLastActivityNull)
                    {
                        sessionToCheck.LastActivity = null;
                    }

                    RemovedSessionsQueue.add(sessionToCheck);
                }

                removed++;

                // switch to read only lock
                HashMapReadWriteLock.getReadLock();
            }

            /* if true release read lock and switch to write lock */
            /* when modification made release write lock and switch to read lock */
        }

        // release read lock
        HashMapReadWriteLock.releaseLock();

        return (removed);
    }

    public boolean validateSession(String key, java.util.Date activityDate, Session sessionToCheck, boolean invalidate)
    {
        boolean result = true;
        HashMapReadWriteLock.getReadLock();

        /* Is the cacheable object expired? */
        if ((sessionToCheck != null) &&
                ((invalidate == true) ||
                (sessionToCheck.isStillValid(activityDate, this.SessionTimeOut, this.LastMatchTimeOut,
                    this.AssociatedAlgorithmCode, this.EnableFallBack) == false)))
        {
            HashMapReadWriteLock.releaseLock();
            HashMapReadWriteLock.getWriteLock();

            // remove session, session expired
            HashMapToMonitor.remove(key);

            result = false;
            HashMapReadWriteLock.releaseLock();

            // deindex session for this hashmap
            sessionToCheck.expireForCode(AssociatedAlgorithmCode);

            // Now check to if session not indexed anymore, if so add to done queue            
            if (sessionToCheck.isExpired() == true)
            {
                RemovedSessionsQueue.add(sessionToCheck);
            }

            HashMapReadWriteLock.getReadLock();
        }

        // release read lock
        HashMapReadWriteLock.releaseLock();

        return result;
    }

    public int findInvalidSessions()
    { // lock hashmap for reading

        int removed = 0;

        HashMapReadWriteLock.getReadLock();

        // scan hashmap
        java.util.Set keySet = HashMapToMonitor.keySet();

        /* An iterator is used to move through the Keyset */
        Object[] sessionArray = keySet.toArray();

        /* Sets up a loop that will iterate through each key in the KeySet */
        for (int index = 0; index < sessionArray.length; index++)
        {
            /* Get the individual key. We need to hold on to this key in case it needs to be removed */
            String key = (String) sessionArray[index];
            Session sessionToCheck = (Session) HashMapToMonitor.get(key);

            /* Get the cacheable object associated with the key inside the cache */

            //Session sessionToCheck = (Session) HashMapToMonitor.get(key);

            /* Is the cacheable object expired? */
            if ((sessionToCheck != null) &&
                    (sessionToCheck.isStillValid(this.CurrentDate, this.SessionTimeOut, this.LastMatchTimeOut,
                        this.AssociatedAlgorithmCode, this.EnableFallBack) == false))
            {
                HashMapReadWriteLock.releaseLock();
                HashMapReadWriteLock.getWriteLock();

                // System.out.println("HashMap monitor thread: Removing timed out session->" + sessionToCheck.LastActivity + ", " + sessionToCheck.LastActivity);
                // System.out.println(".");
                // remove session, session expired
                keySet.remove(key);

                HashMapReadWriteLock.releaseLock();

                // deindex session for this hashmap
                sessionToCheck.expireForCode(AssociatedAlgorithmCode);

                // Now check to if session not indexed anymore, if so add to done queue            
                if (sessionToCheck.isExpired() == true)
                {
                    RemovedSessionsQueue.add(sessionToCheck);
                }

                removed++;

                HashMapReadWriteLock.getReadLock();
            }

            /* if true release read lock and switch to write lock */
            /* when modification made release write lock and switch to read lock */
        }

        // release read lock
        HashMapReadWriteLock.releaseLock();

        return (removed);
    }

    public void run()
    {
        while (HashMapToMonitor != null)
        {
            try
            {
                // best to sleep first as hashmap is probably empty
                Thread.sleep(SleepTime);

                findInvalidSessions();
            }
            catch (InterruptedException e)
            {
                // the VM doesn't want us to sleep anymore,
                // so get back to work
                //int t = 0;
            }
            catch (Exception e)
            {
                // hashmap probably set to null
                System.out.println("HashMap monitor thread stopping:" + e);

                return;
            }

            //System.out.println("@");
        }
    }

    public void setHashMapToMonitor(KNIHashMap pNewHashMapToMonitor, ReadWriteLock pHashMapReadWriteLock)
    {
        HashMapToMonitor = pNewHashMapToMonitor;
        HashMapReadWriteLock = pHashMapReadWriteLock;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/18/2002 9:58:44 AM)
     */
    public void stopScanning()
    {
        HashMapToMonitor = null;
    }
}
