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
package com.kni.etl.sessionizer;

import java.io.Serializable;
import java.util.List;

import com.kni.etl.KNIHashMap;
import com.kni.etl.ReadWriteLock;

// TODO: Auto-generated Javadoc
/**
 * The Class SessionStoreSet.
 */
public class SessionStoreSet implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3544390301348214320L;
    
    /** The Hash map to monitor. */
    private KNIHashMap HashMapToMonitor;
    
    /** The Hash map read write lock. */
    private ReadWriteLock HashMapReadWriteLock;
    
    /** The Current date. */
    private java.util.Date CurrentDate;
    
    /** The Associated algorithm code. */
    private int AssociatedAlgorithmCode = 0;
    
    /** The Enable fall back. */
    private boolean EnableFallBack = false;
    
    /** The Removed sessions queue. */
    private List RemovedSessionsQueue;
    
    /** The Last match time out. */
    public int LastMatchTimeOut;
    
    /** The Session time out. */
    public int SessionTimeOut;

    /**
     * SessionStoreBackgroundThread constructor comment.
     * 
     * @param pHashMapToMonitor the hash map to monitor
     * @param pHashMapReadWriteLock the hash map read write lock
     * @param pDate the date
     * @param pSessionTimeOut the session time out
     * @param pLastMatchTimeOut the last match time out
     * @param pAssociatedAlgorithmCode the associated algorithm code
     * @param pRemovedSessionsQueue the removed sessions queue
     * @param pKeepFallBack the keep fall back
     */
    public SessionStoreSet(KNIHashMap pHashMapToMonitor, ReadWriteLock pHashMapReadWriteLock, java.util.Date pDate,
            int pSessionTimeOut, int pLastMatchTimeOut, int pAssociatedAlgorithmCode, List pRemovedSessionsQueue,
            boolean pKeepFallBack) {
        super();

        // this.setPriority(MIN_PRIORITY);
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
     * Insert the method's description here. Creation date: (5/13/2002 11:34:59 PM)
     * 
     * @param pDontExpireSessions if true then non closed sessions will have null last activity
     * 
     * @return int
     * 
     * @throws InterruptedException      */
    public int closeOutAllSessions(boolean pDontExpireSessions) {
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
            if (sessionToCheck != null) {
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
                if (sessionToCheck.isExpired() == true) {
                    if (pDontExpireSessions) {
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

    /**
     * Validate session.
     * 
     * @param key the key
     * @param activityDate the activity date
     * @param sessionToCheck the session to check
     * @param invalidate the invalidate
     * 
     * @return true, if successful
     * 
     * @throws InterruptedException the interrupted exception
     */
    public boolean validateSession(String key, java.util.Date activityDate, Session sessionToCheck, boolean invalidate)
            throws InterruptedException {
        boolean result = true;
        this.HashMapReadWriteLock.getReadLock();

        /* Is the cacheable object expired? */
        if ((sessionToCheck != null)
                && ((invalidate == true) || (sessionToCheck.isStillValid(activityDate, this.SessionTimeOut,
                        this.LastMatchTimeOut, this.AssociatedAlgorithmCode, this.EnableFallBack) == false))) {
            this.HashMapReadWriteLock.releaseLock();
            this.HashMapReadWriteLock.getWriteLock();

            // remove session, session expired
            this.HashMapToMonitor.remove(key);

            result = false;
            this.HashMapReadWriteLock.releaseLock();

            // deindex session for this hashmap
            sessionToCheck.expireForCode(this.AssociatedAlgorithmCode);

            // Now check to if session not indexed anymore, if so add to done queue
            if (sessionToCheck.isExpired() == true) {
                this.RemovedSessionsQueue.add(sessionToCheck);
            }

            this.HashMapReadWriteLock.getReadLock();
        }

        // release read lock
        this.HashMapReadWriteLock.releaseLock();

        return result;
    }

    /**
     * Find invalid sessions.
     * 
     * @return the int
     * 
     * @throws Exception the exception
     */
    public int findInvalidSessions() throws Exception { // lock hashmap for reading

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

            // Session sessionToCheck = (Session) HashMapToMonitor.get(key);
            /* Is the cacheable object expired? */
            if ((sessionToCheck != null)
                    && (sessionToCheck.isStillValid(this.CurrentDate, this.SessionTimeOut, this.LastMatchTimeOut,
                            this.AssociatedAlgorithmCode, this.EnableFallBack) == false)) {
                this.HashMapReadWriteLock.releaseLock();
                this.HashMapReadWriteLock.getWriteLock();

                // System.out.println("HashMap monitor thread: Removing timed out session->" +
                // sessionToCheck.LastActivity + ", " + sessionToCheck.LastActivity);
                // System.out.println(".");
                // remove session, session expired
                keySet.remove(key);

                this.HashMapReadWriteLock.releaseLock();

                // deindex session for this hashmap
                sessionToCheck.expireForCode(this.AssociatedAlgorithmCode);

                // Now check to if session not indexed anymore, if so add to done queue
                if (sessionToCheck.isExpired() == true) {
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

    /**
     * Sets the hash map to monitor.
     * 
     * @param pNewHashMapToMonitor the new hash map to monitor
     * @param pHashMapReadWriteLock the hash map read write lock
     */
    public void setHashMapToMonitor(KNIHashMap pNewHashMapToMonitor, ReadWriteLock pHashMapReadWriteLock) {
        this.HashMapToMonitor = pNewHashMapToMonitor;
        this.HashMapReadWriteLock = pHashMapReadWriteLock;
    }

    /**
     * Insert the method's description here. Creation date: (4/18/2002 9:58:44 AM)
     */
    public void stopScanning() {
        this.HashMapToMonitor = null;
    }
}
