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
import com.kni.etl.ketl.IDCounter;

// TODO: Auto-generated Javadoc
/**
 * The Class SessionStore.
 */
public class SessionStore implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3257008756763342901L;
    
    /** The tmp session ID. */
    transient private IDCounter tmpSessionID = null;

    // IP and Browser session identifiers
    /** The Sessions by IP address and browser hash map. */
    private KNIHashMap SessionsByIPAddressAndBrowserHashMap;
    
    /** The Sessions by IP address and browser read write lock. */
    private ReadWriteLock SessionsByIPAddressAndBrowserReadWriteLock;
    
    /** The Sessions by IP address and browser background thread. */
    private SessionStoreSet SessionsByIPAddressAndBrowserBackgroundThread;

    // Main session identifiers
    /** The Sessions by main session identifier hash map. */
    private KNIHashMap SessionsByMainSessionIdentifierHashMap;
    
    /** The Sessions by main session identifier read write lock. */
    private ReadWriteLock SessionsByMainSessionIdentifierReadWriteLock;
    
    /** The Sessions by main session identifier background thread. */
    private SessionStoreSet SessionsByMainSessionIdentifierBackgroundThread;

    // first click session identifiers
    /** The Sessions by first click session identifier hash map. */
    private KNIHashMap SessionsByFirstClickSessionIdentifierHashMap;
    
    /** The Sessions by first click session identifier read write lock. */
    private ReadWriteLock SessionsByFirstClickSessionIdentifierReadWriteLock;
    
    /** The Sessions by first click session identifier background thread. */
    private SessionStoreSet SessionsByFirstClickSessionIdentifierBackgroundThread;

    // persistant session identifiers
    /** The Sessions by persistant identifier hash map. */
    private KNIHashMap SessionsByPersistantIdentifierHashMap;
    
    /** The Sessions by persistant identifier read write lock. */
    private ReadWriteLock SessionsByPersistantIdentifierReadWriteLock;
    
    /** The Sessions by persistant identifier background thread. */
    private SessionStoreSet SessionsByPersistantIdentifierBackgroundThread;
    
    /** The Current date. */
    java.util.Date CurrentDate;
    
    /** The Removed sessions queue. */
    private List RemovedSessionsQueue;

    /**
     * SessionStore constructor comment.
     * 
     * @param pStartSessionID the start session ID
     * @param list the list
     */
    public SessionStore(IDCounter pStartSessionID, List list) {
        super();

        this.setIDCounter(pStartSessionID);
        this.RemovedSessionsQueue = list;
        this.CurrentDate = new java.util.Date(1);

    }

    /**
     * Sets the ID counter.
     * 
     * @param pStartSessionID the new ID counter
     */
    public void setIDCounter(IDCounter pStartSessionID) {
        this.tmpSessionID = pStartSessionID;
    }

    /**
     * Insert the method's description here. Creation date: (4/8/2002 3:25:45 PM)
     * 
     * @param pSession the session
     * 
     * @return int
     */
    public Session addSession(Session pSession) {
        Session duplicateSession = (Session) pSession.clone();

        duplicateSession.setID(this.tmpSessionID.incrementID());

        // mark if repeat visitor!
        if (duplicateSession.PersistantIdentifier != null) {
            duplicateSession.setRepeatVisitor(true);
        }

        // add session to main identifier hashmap so it can be called back later.
        duplicateSession.setMainSessionIdentifierIndexed(this.putSession(duplicateSession,
                this.SessionsByMainSessionIdentifierHashMap, this.SessionsByMainSessionIdentifierReadWriteLock,
                duplicateSession.MainSessionIdentifier));

        // --------------------------------------------------------------
        // add session to ip address and browser hashmap
        duplicateSession.setIPAddressAndBrowserIndexed(this.putSession(duplicateSession,
                this.SessionsByIPAddressAndBrowserHashMap, this.SessionsByIPAddressAndBrowserReadWriteLock,
                duplicateSession.IPAddress + duplicateSession.Browser));

        // --------------------------------------------------------------
        // add session to first click hashmap
        duplicateSession.setFirstClickSessionIdentifierIndexed(this.putSession(duplicateSession,
                this.SessionsByFirstClickSessionIdentifierHashMap,
                this.SessionsByFirstClickSessionIdentifierReadWriteLock, duplicateSession.FirstClickSessionIdentifier));

        // ---------------------------------------------------------------
        // add session to peristant identifier hashmap
        duplicateSession.setPersistantIdentifierIndexed(this.putSession(duplicateSession,
                this.SessionsByPersistantIdentifierHashMap, this.SessionsByPersistantIdentifierReadWriteLock,
                duplicateSession.PersistantIdentifier));

        return duplicateSession;
    }

    /**
     * Insert the method's description here. Creation date: (4/16/2002 2:01:46 PM)
     * 
     * @param pPeakSessionsAnHour the peak sessions an hour
     * @param pSessionTimeOutSeconds the session time out seconds
     * 
     * @return java.util.HashMap
     */
    private KNIHashMap createHashMap(int pPeakSessionsAnHour, int pSessionTimeOutSeconds) {
        // based on time out and sessions an hhour we can determine optimum map size.
        // e.g 1000 sessions an hour with a timeout of 30 minutes.
        // equates to (1000/(60*60)) * (30*60) = 500 points in our hashmap.
        // you may want to increase by a factor of x if rehashing occurs to much
        return new KNIHashMap(pSessionTimeOutSeconds * (pPeakSessionsAnHour / 3600));
    }

    /**
     * Insert the method's description here. Creation date: (4/19/2002 3:54:34 PM)
     * 
     * @return the int
     * 
     * @throws InterruptedException      * @throws Exception the exception
     */
    public int findStaleSessions() throws Exception {
        int removed = 0;
        int val;

        val = this.SessionsByIPAddressAndBrowserBackgroundThread.findInvalidSessions();

        // System.out.print("Removed IP And B:" + val);
        val = this.SessionsByMainSessionIdentifierBackgroundThread.findInvalidSessions();

        // System.out.print(", Main ID:" + val);
        removed = val + removed;
        val = this.SessionsByFirstClickSessionIdentifierBackgroundThread.findInvalidSessions();

        // System.out.print(", First Click:" + val);
        removed = val + removed;
        val = this.SessionsByPersistantIdentifierBackgroundThread.findInvalidSessions();

        // System.out.println(", Persistant:" + val);
        removed = val + removed;

        return (removed);
    }

    /**
     * Insert the method's description here. Creation date: (4/18/2002 8:26:05 PM)
     * 
     * @return java.util.Date
     */
    public java.util.Date getCurrentDate() {
        return this.CurrentDate;
    }

    /**
     * Insert the method's description here. Creation date: (4/8/2002 3:25:45 PM)
     * 
     * @param pHashMap the hash map
     * @param pReadWriteLock the read write lock
     * @param pKey the key
     * 
     * @return int
     */
    public Session getSession(KNIHashMap pHashMap, ReadWriteLock pReadWriteLock, String pKey) {
        // lock for writing
        pReadWriteLock.getReadLock();

        // add object to hashmap
        Session result = (Session) pHashMap.get(pKey);

        /*
         * if (result != null) { System.out.println("Get: " + pKey + " found"); } else System.out.println("Get: " + pKey + "
         * not found");
         */
        // release write lock
        pReadWriteLock.releaseLock();

        return (result);
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 4:03:36 PM)
     * 
     * @param pSessionToFind datasources.Session Session Matching Values BIT MASK 1 = MainSessionIdentifier 2 =
     * FirstClickIdentifier 4 = Browser 8 = IP Only
     * @param pSessionMatchingAlgorithmToUse the session matching algorithm to use
     * 
     * @return boolean
     * 
     * @throws InterruptedException the interrupted exception
     */
    public Session getSessionBySelectedAlgorithm(Session pSessionToFind, int pSessionMatchingAlgorithmToUse)
            throws InterruptedException {
        Session tmpSession = null;

        // possible added slow code
        switch (pSessionMatchingAlgorithmToUse) {
        case 1: // |3|5|7:
            tmpSession = this.getSession(this.SessionsByMainSessionIdentifierHashMap,
                    this.SessionsByMainSessionIdentifierReadWriteLock, pSessionToFind.MainSessionIdentifier);

            if ((tmpSession != null)
                    && (this.SessionsByMainSessionIdentifierBackgroundThread.validateSession(
                            tmpSession.MainSessionIdentifier, pSessionToFind.LastActivity, tmpSession,
                            this.sessionDefinition.matchInValid(tmpSession.getBestMatchedByCode(),
                                    pSessionMatchingAlgorithmToUse)) == false)) {
                tmpSession = null;
            }
            else if (tmpSession != null) {
                pSessionToFind.setPreviouslyFoundOnMainSessionIdentifier(true);
            }

            break;

        case 2: // |6:
            tmpSession = this
                    .getSession(this.SessionsByFirstClickSessionIdentifierHashMap,
                            this.SessionsByFirstClickSessionIdentifierReadWriteLock,
                            pSessionToFind.FirstClickSessionIdentifier);

            if ((tmpSession != null)
                    && (this.SessionsByFirstClickSessionIdentifierBackgroundThread.validateSession(
                            tmpSession.FirstClickSessionIdentifier, pSessionToFind.LastActivity, tmpSession,
                            this.sessionDefinition.matchInValid(tmpSession.getBestMatchedByCode(),
                                    pSessionMatchingAlgorithmToUse)) == false)) {
                tmpSession = null;
            }

            break;

        case 4:
            tmpSession = this.getSession(this.SessionsByPersistantIdentifierHashMap,
                    this.SessionsByPersistantIdentifierReadWriteLock, pSessionToFind.PersistantIdentifier);

            if ((tmpSession != null)
                    && (this.SessionsByPersistantIdentifierBackgroundThread.validateSession(
                            tmpSession.PersistantIdentifier, pSessionToFind.LastActivity, tmpSession,
                            this.sessionDefinition.matchInValid(tmpSession.getBestMatchedByCode(),
                                    pSessionMatchingAlgorithmToUse)) == false)) {
                tmpSession = null;
            }

            break;

        case 24:

            String key = pSessionToFind.IPAddress + pSessionToFind.Browser;
            tmpSession = this.getSession(this.SessionsByIPAddressAndBrowserHashMap,
                    this.SessionsByIPAddressAndBrowserReadWriteLock, key);

            if ((tmpSession != null)
                    && (this.SessionsByIPAddressAndBrowserBackgroundThread.validateSession(key,
                            pSessionToFind.LastActivity, tmpSession, this.sessionDefinition.matchInValid(tmpSession
                                    .getBestMatchedByCode(), pSessionMatchingAlgorithmToUse)) == false)) {
                tmpSession = null;
            }

            break;
        }

        if (tmpSession != null) {
            tmpSession.setMatchingAlgorithm(pSessionMatchingAlgorithmToUse);
        }

        return tmpSession;
    }

    /**
     * Insert the method's description here. Creation date: (4/8/2002 3:25:45 PM)
     * 
     * @param pSession the session
     * @param pHashMap the hash map
     * @param pReadWriteLock the read write lock
     * @param pKey the key
     * 
     * @return int
     */
    public boolean putSession(Session pSession, KNIHashMap pHashMap, ReadWriteLock pReadWriteLock, String pKey) {
        if ((pSession == null) || (pKey == null)) {
            return (false);
        }

        // lock for writing
        pReadWriteLock.getWriteLock();

        // add object to hashmap
        // System.out.println("Put: " +pKey);
        pHashMap.put(pKey, pSession);

        // release write lock
        pReadWriteLock.releaseLock();

        return (true);
    }

    /**
     * Insert the method's description here. Creation date: (4/8/2002 3:25:45 PM)
     * 
     * @param pHashMap the hash map
     * @param pReadWriteLock the read write lock
     * @param pKey the key
     * 
     * @return int
     */
    public void removeSession(KNIHashMap pHashMap, ReadWriteLock pReadWriteLock, String pKey) {
        // lock for writing
        pReadWriteLock.getWriteLock();

        // add object to hashmap
        // System.out.println("Rem: " + pKey);
        pHashMap.remove(pKey);

        // release write lock
        pReadWriteLock.releaseLock();
    }

    /**
     * Insert the method's description here. Creation date: (4/18/2002 8:26:05 PM)
     * 
     * @param newCurrentTime the new current time
     */
    public void setCurrentDate(long newCurrentTime) {
        if (this.CurrentDate == null) {
            this.CurrentDate = new java.util.Date(newCurrentTime);
        }
        else {
            synchronized (this.CurrentDate) {
                this.CurrentDate.setTime(newCurrentTime);
            }
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/11/2002 12:55:00 PM)
     * 
     * @param pSessionToSync datasources.Session
     * @param pSessionToSyncWith datasources.Session update session with new information from session to sync with
     */
    public void updateSessionStore(Session pSessionToSync, Session pSessionToSyncWith) {
        // if (pSessionToSync.FirstClickSessionIdentifier != null &&
        // pSessionToSync.FirstClickSessionIdentifier.indexOf("226.199.161.208681024730146880") > 0) {
        // int rs = 0;
        // rs = 1;
        // }
        // update session activity times
        if ((pSessionToSync.FirstActivity == null)
                || pSessionToSync.FirstActivity.after(pSessionToSyncWith.LastActivity)) {
            pSessionToSync.FirstActivity = pSessionToSyncWith.LastActivity;
        }

        if (pSessionToSync.customField1 == null) {
            pSessionToSync.customField1 = pSessionToSyncWith.customField1;
        }

        if (pSessionToSync.customField2 == null) {
            pSessionToSync.customField2 = pSessionToSyncWith.customField2;
        }

        if (pSessionToSync.customField3 == null) {
            pSessionToSync.customField3 = pSessionToSyncWith.customField3;
        }

        if ((pSessionToSync.LastActivity == null)
                || pSessionToSync.LastActivity.before(pSessionToSyncWith.LastActivity)) {
            pSessionToSync.LastActivity = pSessionToSyncWith.LastActivity;
        }

        // setting last match date for code
        pSessionToSync.setMatchedByDateForCode(pSessionToSync.getSessionMatchedByCode(),
                pSessionToSyncWith.LastActivity);

        // if fallback enabled then set last match for fallback enabled
        // identifier to last activity date
        if (this.sessionDefinition.FirstClickIdentifierFallbackEnabled) {
            pSessionToSync.setMatchedByDateForCode(2, pSessionToSyncWith.LastActivity);
        }

        if (this.sessionDefinition.IPBrowserFallbackEnabled) {
            pSessionToSync.setMatchedByDateForCode(24, pSessionToSyncWith.LastActivity);
        }

        if (this.sessionDefinition.MainIdentifierFallbackEnabled) {
            pSessionToSync.setMatchedByDateForCode(1, pSessionToSyncWith.LastActivity);
        }

        if (this.sessionDefinition.PersistantIdentifierFallbackEnabled) {
            pSessionToSync.setMatchedByDateForCode(4, pSessionToSyncWith.LastActivity);
        }

        // update keep variables -- needs code to append onto current list
        if ((pSessionToSync.CookieKeepVariables == null) && (pSessionToSyncWith.CookieKeepVariables != null)) {
            pSessionToSync.CookieKeepVariables = pSessionToSyncWith.CookieKeepVariables;
        }
        else if ((pSessionToSync.CookieKeepVariables != null) && (pSessionToSyncWith.CookieKeepVariables != null)
                && (pSessionToSync.CookieKeepVariables != pSessionToSyncWith.CookieKeepVariables)) {
            int syncFieldCnt = pSessionToSync.CookieKeepVariables.length;
            int withFieldCnt = pSessionToSyncWith.CookieKeepVariables.length;

            int variableNotFound = -1;

            for (int i = 0; i < withFieldCnt; i++) {
                // pSessionToSyncWith.CookieKeepVariables[i][0]
                variableNotFound = i;

                for (int ix = 0; ix < syncFieldCnt; ix++) {
                    if (pSessionToSyncWith.CookieKeepVariables[i][0] == pSessionToSync.CookieKeepVariables[ix][0]) {
                        variableNotFound = -1;
                        ix = syncFieldCnt;
                    }
                }

                // if variable not found append variable
                if (variableNotFound > -1) {
                    /** got this far so we must allocate space for variable */
                    String[][] tmp = new String[syncFieldCnt + 1][2];

                    for (int ix = 0; ix < syncFieldCnt; ix++) {
                        tmp[ix][0] = pSessionToSync.CookieKeepVariables[ix][0];
                        tmp[ix][1] = pSessionToSync.CookieKeepVariables[ix][1];
                    }

                    pSessionToSync.CookieKeepVariables = tmp;

                    pSessionToSync.CookieKeepVariables[syncFieldCnt][0] = pSessionToSyncWith.CookieKeepVariables[variableNotFound][0];
                    pSessionToSync.CookieKeepVariables[syncFieldCnt][1] = pSessionToSyncWith.CookieKeepVariables[variableNotFound][1];
                }
            }
        }

        pSessionToSync.LastActivity = pSessionToSyncWith.LastActivity;
        pSessionToSync.PreviouslyFoundOnMainSessionIdentifier = pSessionToSyncWith.PreviouslyFoundOnMainSessionIdentifier;

        // Main Session Identifier ----------------------------------------------------------------------------
        // if a main session identifier has been found then overwrite current one
        if ((pSessionToSyncWith.MainSessionIdentifier != null) && (pSessionToSync.MainSessionIdentifier != null)
                && (pSessionToSync.MainSessionIdentifier.equals(pSessionToSyncWith.MainSessionIdentifier) == false)) {
            // update hashmap with new index value
            if (pSessionToSync.isMainSessionIdentifierIndexed() == true) {
                // remove key from hashmap
                this.removeSession(this.SessionsByMainSessionIdentifierHashMap,
                        this.SessionsByMainSessionIdentifierReadWriteLock, pSessionToSync.MainSessionIdentifier);
                pSessionToSync.setMainSessionIdentifierIndexed(false);
            }

            // set session key to new key
            pSessionToSync.MainSessionIdentifier = pSessionToSyncWith.MainSessionIdentifier;
        }
        else if ((pSessionToSyncWith.MainSessionIdentifier != null) && (pSessionToSync.MainSessionIdentifier == null)) {
            pSessionToSync.MainSessionIdentifier = pSessionToSyncWith.MainSessionIdentifier;
            pSessionToSync.setMainSessionIdentifierIndexed(false);
        }

        // index item if not indexed already
        if ((pSessionToSync.isMainSessionIdentifierIndexed() == false)
                && (pSessionToSync.MainSessionIdentifier != null)
                && (pSessionToSync.isMainSessionIdentifierAllowIndex() == true)
                && ((pSessionToSync.getBestMatchedByCode() == 0)
                        || this.sessionDefinition.MainIdentifierFallbackEnabled || (pSessionToSync
                        .getBestMatchedByCode() >= 1))) {
            // add session back into hashmap
            // System.out.println("Update: " + pSessionToSync.MainSessionIdentifier);
            pSessionToSync.setMainSessionIdentifierIndexed(this.putSession(pSessionToSync,
                    this.SessionsByMainSessionIdentifierHashMap, this.SessionsByMainSessionIdentifierReadWriteLock,
                    pSessionToSync.MainSessionIdentifier));

            // set session as indexed
        }

        // Session Browser and IP Identifier ------------------------------------------------------------------------???
        // if no browser and object to sync with has browser then copy it
        if (((pSessionToSyncWith.Browser != null) && (pSessionToSync.Browser != null) && (pSessionToSync.Browser
                .equals(pSessionToSyncWith.Browser) == false))
                || ((pSessionToSyncWith.IPAddress != null) && (pSessionToSync.IPAddress != null) && (pSessionToSync.IPAddress
                        .equals(pSessionToSyncWith.IPAddress) == false))) {
            // update hashmap with new index value
            if (pSessionToSync.isIPAddressAndBrowserIndexed() == true) {
                // remove key from hashmap
                this.removeSession(this.SessionsByIPAddressAndBrowserHashMap,
                        this.SessionsByIPAddressAndBrowserReadWriteLock, pSessionToSync.IPAddress
                                + pSessionToSync.Browser);
                pSessionToSync.setIPAddressAndBrowserIndexed(false);
            }

            // set session key to new key
            pSessionToSync.Browser = pSessionToSyncWith.Browser;
            pSessionToSync.IPAddress = pSessionToSyncWith.IPAddress;
        }
        else if (((pSessionToSyncWith.Browser != null) && (pSessionToSync.Browser == null))
                || ((pSessionToSyncWith.Browser != null) && (pSessionToSync.Browser == null))) {
            pSessionToSync.IPAddress = pSessionToSyncWith.Browser;
            pSessionToSync.IPAddress = pSessionToSyncWith.IPAddress;
            pSessionToSync.setIPAddressAndBrowserIndexed(false);
        }

        // index item if not indexed already
        if ((pSessionToSync.isIPAddressAndBrowserIndexed() == false)
                && (pSessionToSync.IPAddress != null)
                && (pSessionToSync.Browser != null)
                && (pSessionToSync.isIPAddressAndBrowserAllowIndex() == true)
                && ((pSessionToSync.getBestMatchedByCode() == 0) || this.sessionDefinition.IPBrowserFallbackEnabled || (pSessionToSync
                        .getBestMatchedByCode() >= 24))) {
            // System.out.println("Update: " + pSessionToSync.IPAddress + pSessionToSync.Browser);
            // add session back into hashmap
            pSessionToSync.setIPAddressAndBrowserIndexed(this.putSession(pSessionToSync,
                    this.SessionsByIPAddressAndBrowserHashMap, this.SessionsByIPAddressAndBrowserReadWriteLock,
                    pSessionToSync.IPAddress + pSessionToSync.Browser));
        }

        // Session FirstClickIdentifier Identifier
        // ----------------------------------------------------------------------------
        // if no browser and object to sync with has browser then copy it
        if ((pSessionToSync.isPreviouslyFoundOnMainSessionIdentifier() == false)
                && (pSessionToSync.isMainSessionIdentifierIndexed() == true)) {
            if ((pSessionToSyncWith.FirstClickSessionIdentifier != null)
                    && (pSessionToSync.FirstClickSessionIdentifier != null)
                    && (pSessionToSync.FirstClickSessionIdentifier
                            .equals(pSessionToSyncWith.FirstClickSessionIdentifier) == false)) {
                // update hashmap with new index value
                if (pSessionToSync.isFirstClickSessionIdentifierIndexed() == true) {
                    // remove key from hashmap
                    this.removeSession(this.SessionsByFirstClickSessionIdentifierHashMap,
                            this.SessionsByFirstClickSessionIdentifierReadWriteLock,
                            pSessionToSync.FirstClickSessionIdentifier);
                    pSessionToSync.setFirstClickSessionIdentifierIndexed(false);
                }

                // set session key to new key
                pSessionToSync.FirstClickSessionIdentifier = pSessionToSyncWith.FirstClickSessionIdentifier;
            }
            else if ((pSessionToSyncWith.FirstClickSessionIdentifier != null)
                    && (pSessionToSync.FirstClickSessionIdentifier == null)) {
                pSessionToSync.FirstClickSessionIdentifier = pSessionToSyncWith.FirstClickSessionIdentifier;
                pSessionToSync.setFirstClickSessionIdentifierIndexed(false);
            }

            // index item if not indexed already
            if ((pSessionToSync.isFirstClickSessionIdentifierIndexed() == false)
                    && (pSessionToSync.FirstClickSessionIdentifier != null)
                    && (pSessionToSync.isFirstClickSessionIdentifierAllowIndex() == true)
                    && ((pSessionToSync.getBestMatchedByCode() == 0)
                            || this.sessionDefinition.FirstClickIdentifierFallbackEnabled || (pSessionToSync
                            .getBestMatchedByCode() >= 2))) {
                // System.out.println("Update: " + pSessionToSync.FirstClickSessionIdentifier);
                // add session back into hashmap
                pSessionToSync.setFirstClickSessionIdentifierIndexed(this.putSession(pSessionToSync,
                        this.SessionsByFirstClickSessionIdentifierHashMap,
                        this.SessionsByFirstClickSessionIdentifierReadWriteLock,
                        pSessionToSync.FirstClickSessionIdentifier));
            }
        }

        // Session Persistant Identifier ----------------------------------------------------------------------------
        // if no persistant identifier
        if ((pSessionToSyncWith.PersistantIdentifier != null) && (pSessionToSync.PersistantIdentifier != null)
                && (pSessionToSync.PersistantIdentifier.equals(pSessionToSyncWith.PersistantIdentifier) == false)) {
            // update hashmap with new index value
            if (pSessionToSync.isPersistantIdentifierIndexed() == true) {
                // remove key from hashmap
                this.removeSession(this.SessionsByPersistantIdentifierHashMap,
                        this.SessionsByPersistantIdentifierReadWriteLock, pSessionToSync.PersistantIdentifier);
                pSessionToSync.setPersistantIdentifierIndexed(false);
            }

            // set session key to new key
            pSessionToSync.PersistantIdentifier = pSessionToSyncWith.PersistantIdentifier;
        }
        else if ((pSessionToSyncWith.PersistantIdentifier != null) && (pSessionToSync.PersistantIdentifier == null)) {
            pSessionToSync.PersistantIdentifier = pSessionToSyncWith.PersistantIdentifier;
            pSessionToSync.setPersistantIdentifierIndexed(false);
        }

        // index item if not indexed already
        if ((pSessionToSync.isPersistantIdentifierIndexed() == false)
                && (pSessionToSync.PersistantIdentifier != null)
                && (pSessionToSync.isPersistantIdentifierAllowIndex() == true)
                && ((pSessionToSync.getBestMatchedByCode() == 0)
                        || this.sessionDefinition.PersistantIdentifierFallbackEnabled || (pSessionToSync
                        .getBestMatchedByCode() >= 4))) {
            // System.out.println("Update: " + pSessionToSync.PersistantIdentifier);
            // add session back into hashmap
            pSessionToSync.setPersistantIdentifierIndexed(this.putSession(pSessionToSync,
                    this.SessionsByPersistantIdentifierHashMap, this.SessionsByPersistantIdentifierReadWriteLock,
                    pSessionToSync.PersistantIdentifier));
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 11:33:19 PM)
     * 
     * @param pLastActivityNull if true then non closed sessions will have null last activity
     * 
     * @throws InterruptedException      */
    public void closeOutAllSessions(boolean pLastActivityNull)

    {
        this.SessionsByIPAddressAndBrowserBackgroundThread.closeOutAllSessions(pLastActivityNull);
        this.SessionsByMainSessionIdentifierBackgroundThread.closeOutAllSessions(pLastActivityNull);
        this.SessionsByFirstClickSessionIdentifierBackgroundThread.closeOutAllSessions(pLastActivityNull);
        this.SessionsByPersistantIdentifierBackgroundThread.closeOutAllSessions(pLastActivityNull);

        return;
    }

    /**
     * Insert the method's description here. Creation date: (5/15/2002 2:02:18 PM)
     * 
     * @return double
     */
    public IDCounter getLastSessionID() {
        return this.tmpSessionID;
    }

    /** The session definition. */
    transient SessionDefinition sessionDefinition = null;

    /**
     * Creates the hash maps.
     * 
     * @param pSessionDefinition the session definition
     */
    public void createHashMaps(SessionDefinition pSessionDefinition) {
        this.sessionDefinition = pSessionDefinition;

        // create hashmaps, hashmap locks and management threads
        // ip address and browser
        if (this.SessionsByIPAddressAndBrowserHashMap == null) {
            this.SessionsByIPAddressAndBrowserHashMap = this.createHashMap(this.sessionDefinition.PeakSessionsAnHour,
                    this.sessionDefinition.IPBrowserTimeOut);
        }

        if (this.SessionsByIPAddressAndBrowserReadWriteLock == null) {
            this.SessionsByIPAddressAndBrowserReadWriteLock = new ReadWriteLock();
        }

        this.SessionsByIPAddressAndBrowserBackgroundThread = new SessionStoreSet(
                this.SessionsByIPAddressAndBrowserHashMap, this.SessionsByIPAddressAndBrowserReadWriteLock,
                this.CurrentDate, this.sessionDefinition.TimeOut, this.sessionDefinition.IPBrowserTimeOut, 24,
                this.RemovedSessionsQueue, this.sessionDefinition.IPBrowserFallbackEnabled);

        // first click
        if (this.SessionsByFirstClickSessionIdentifierHashMap == null) {
            this.SessionsByFirstClickSessionIdentifierHashMap = this.createHashMap(
                    this.sessionDefinition.PeakSessionsAnHour, this.sessionDefinition.FirstClickIdentifierTimeOut);
        }

        if (this.SessionsByFirstClickSessionIdentifierReadWriteLock == null) {
            this.SessionsByFirstClickSessionIdentifierReadWriteLock = new ReadWriteLock();
        }

        this.SessionsByFirstClickSessionIdentifierBackgroundThread = new SessionStoreSet(
                this.SessionsByFirstClickSessionIdentifierHashMap,
                this.SessionsByFirstClickSessionIdentifierReadWriteLock, this.CurrentDate,
                this.sessionDefinition.TimeOut, this.sessionDefinition.FirstClickIdentifierTimeOut, 2,
                this.RemovedSessionsQueue, this.sessionDefinition.FirstClickIdentifierFallbackEnabled);

        // persistant identifier
        if (this.SessionsByPersistantIdentifierHashMap == null) {
            this.SessionsByPersistantIdentifierHashMap = this.createHashMap(this.sessionDefinition.PeakSessionsAnHour,
                    this.sessionDefinition.PersistantIdentifierTimeOut);
        }

        if (this.SessionsByPersistantIdentifierReadWriteLock == null) {
            this.SessionsByPersistantIdentifierReadWriteLock = new ReadWriteLock();
        }

        this.SessionsByPersistantIdentifierBackgroundThread = new SessionStoreSet(
                this.SessionsByPersistantIdentifierHashMap, this.SessionsByPersistantIdentifierReadWriteLock,
                this.CurrentDate, this.sessionDefinition.TimeOut, this.sessionDefinition.PersistantIdentifierTimeOut,
                4, this.RemovedSessionsQueue, this.sessionDefinition.PersistantIdentifierFallbackEnabled);

        // main session identifier
        if (this.SessionsByMainSessionIdentifierHashMap == null) {
            this.SessionsByMainSessionIdentifierHashMap = this.createHashMap(this.sessionDefinition.PeakSessionsAnHour,
                    this.sessionDefinition.MainIdentifierTimeOut);
        }

        if (this.SessionsByMainSessionIdentifierReadWriteLock == null) {
            this.SessionsByMainSessionIdentifierReadWriteLock = new ReadWriteLock();
        }

        this.SessionsByMainSessionIdentifierBackgroundThread = new SessionStoreSet(
                this.SessionsByMainSessionIdentifierHashMap, this.SessionsByMainSessionIdentifierReadWriteLock,
                this.CurrentDate, this.sessionDefinition.TimeOut, this.sessionDefinition.MainIdentifierTimeOut, 1,
                this.RemovedSessionsQueue, this.sessionDefinition.MainIdentifierFallbackEnabled);
    }
}
