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

/**
 * Insert the type's description here. Creation date: (4/8/2002 3:27:55 PM)
 * 
 * @author: Administrator Session Matching Values BIT MASK 1 = MainSessionIdentifier 2 = FirstClickIdentifier 4 =
 *          Browser 8 = IP Only
 */
import java.util.Date;

import com.kni.etl.EngineConstants;
import com.kni.etl.ReadWriteLock;

// TODO: Auto-generated Javadoc
/**
 * The Class Session.
 */
public class Session implements Cloneable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3977299923580500018L;

    // public attributes
    /** The ID. */
    public long ID;
    
    /** The last hit stored. */
    public boolean lastHitStored = false;
    
    /** The expired. */
    public boolean expired = false;
    
    /** The First activity. */
    public java.util.Date FirstActivity;
    
    /** The Last activity. */
    public java.util.Date LastActivity;
    
    /** The Browser. */
    public String Browser;
    
    /** The IP address. */
    public String IPAddress;
    
    /** The Main session identifier. */
    public String MainSessionIdentifier;
    
    /** The Start persistant identifier. */
    public String StartPersistantIdentifier;
    
    /** The Persistant identifier. */
    public String PersistantIdentifier;
    
    /** The First click session identifier. */
    public String FirstClickSessionIdentifier;
    
    /** The custom field3. */
    public String customField3;
    
    /** The custom field2. */
    public String customField2;
    
    /** The custom field1. */
    public String customField1;
    
    /** The last hit. */
    public Object[] lastHit = null;
    
    /** The Cookie keep variables. */
    public java.lang.String[][] CookieKeepVariables;
    
    /** The Hit. */
    public long Hit = 0;
    
    /** The Page views. */
    public long PageViews = 0;
    
    /** The D m_ LOA d_ ID. */
    public int DM_LOAD_ID = -1;
    
    /** The LOA d_ ID. */
    public int LOAD_ID = -1;
    
    /** The Previously found on main session identifier. */
    public boolean PreviouslyFoundOnMainSessionIdentifier = false;
    
    /** The Repeat visitor. */
    public boolean RepeatVisitor = false;
    
    /** The Referrer. */
    public String Referrer = null;

    // private attributes
    /** The Session matched by. */
    private int SessionMatchedBy = 0;
    
    /** The Best session matched by. */
    private int BestSessionMatchedBy = 0;
    
    /** The IP address and browser indexed. */
    private boolean IPAddressAndBrowserIndexed = false;
    
    /** The Main session identifier indexed. */
    private boolean MainSessionIdentifierIndexed = false;
    
    /** The First click session identifier indexed. */
    private boolean FirstClickSessionIdentifierIndexed = false;
    
    /** The Persistant identifier indexed. */
    private boolean PersistantIdentifierIndexed = false;
    
    /** The IP address and browser allow index. */
    private boolean IPAddressAndBrowserAllowIndex = true;
    
    /** The Main session identifier allow index. */
    private boolean MainSessionIdentifierAllowIndex = true;
    
    /** The First click session identifier allow index. */
    private boolean FirstClickSessionIdentifierAllowIndex = true;
    
    /** The Persistant identifier allow index. */
    private boolean PersistantIdentifierAllowIndex = true;
    
    /** The IP address and browser last match. */
    private java.util.Date IPAddressAndBrowserLastMatch = null;
    
    /** The Main session identifier last match. */
    private java.util.Date MainSessionIdentifierLastMatch = null;
    
    /** The First click session identifier last match. */
    private java.util.Date FirstClickSessionIdentifierLastMatch = null;
    
    /** The Persistant identifier last match. */
    private java.util.Date PersistantIdentifierLastMatch = null;
    
    /** The Index lock. */
    private ReadWriteLock IndexLock;
    
    /** The Constant SESSION. */
    public static final int SESSION = 1;
    
    /** The last referrer URL page date. */
    java.util.Date mLastReferrerURLPageDate = null;

    /**
     * Session constructor comment.
     */
    public Session() {

        this.IndexLock = new ReadWriteLock();
    }

    /**
     * Sync referrer.
     * 
     * @param pReferrerURL the referrer URL
     * @param pPageDate the page date
     */
    public void syncReferrer(String pReferrerURL, Date pPageDate) {
        if ((this.mLastReferrerURLPageDate == null) || pPageDate.before(this.mLastReferrerURLPageDate)) {
            this.Referrer = pReferrerURL;
            this.mLastReferrerURLPageDate = pPageDate;
        }
    }

    /**
     * Adds the session identifier.
     * 
     * @param pValue the value
     * @param pDestinationObjectType the destination object type
     * 
     * @return true, if successful
     */
    public boolean addSessionIdentifier(String pValue, int pDestinationObjectType) {
        // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS
        switch (pDestinationObjectType) {
        case EngineConstants.SESSION_MAIN_SESSION_IDENTIFIER:
            this.MainSessionIdentifier = pValue;

            return (true);

        case EngineConstants.SESSION_FIRST_CLICK_IDENTIFIER:
            this.FirstClickSessionIdentifier = pValue;

            return (true);

        case EngineConstants.SESSION_BROWSER:
            this.Browser = pValue;

            return (true);

        case EngineConstants.SESSION_IP_ADDRESS:
            this.IPAddress = pValue;

            return (true);

        case EngineConstants.SESSION_PERSISTANT_IDENTIFIER:

            if (this.StartPersistantIdentifier == null) {
                this.StartPersistantIdentifier = pValue;
            }

            this.PersistantIdentifier = pValue;

            return (true);
        }

        return (false);
    }

    /**
     * Insert the method's description here. Creation date: (4/11/2002 3:16:30 PM)
     * 
     * @param pValue java.lang.String
     * @param pVariableName java.lang.String
     * @param pDestinationObjectType the destination object type
     * 
     * @return true, if add session variable
     */
    public boolean addSessionVariable(String pValue, int pDestinationObjectType, String pVariableName) {
        // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS
        switch (pDestinationObjectType) {
        case EngineConstants.KEEP_COOKIE_VARIABLE:

            int fieldCnt = 0;

            if (this.CookieKeepVariables == null) {
                this.CookieKeepVariables = new String[1][2];
            }
            else {
                fieldCnt = this.CookieKeepVariables.length;

                /** if already found keep first value * */
                for (int i = 0; i < fieldCnt; i++) {
                    if (this.CookieKeepVariables[i][0] == pVariableName) {
                        return (true);
                    }
                }

                /** got this far so we must allocate space for variable */
                String[][] tmp = new String[fieldCnt + 1][2];

                for (int i = 0; i < fieldCnt; i++) {
                    tmp[i][0] = this.CookieKeepVariables[i][0];
                    tmp[i][1] = this.CookieKeepVariables[i][1];
                }

                this.CookieKeepVariables = tmp;
            }

            this.CookieKeepVariables[fieldCnt][0] = pVariableName;
            this.CookieKeepVariables[fieldCnt][1] = pValue;

            return (true);
        }

        return (false);
    }

    /**
     * Insert the method's description here. Creation date: (4/15/2002 10:23:51 AM)
     * 
     * @return datasources.Session
     */
    @Override
    public Object clone() {
        Session copySession;

        try {
            copySession = (Session) super.clone();
        } catch (CloneNotSupportedException e) {
            // this should never happen
            throw new InternalError(e.toString());
        }

        // copy string

        /*
         * if (Browser != null) copySession.Browser = new String(Browser); if (IPAddress != null) copySession.IPAddress =
         * new String(IPAddress); if (FirstClickSessionIdentifier != null) copySession.FirstClickSessionIdentifier = new
         * String(FirstClickSessionIdentifier); if (MainSessionIdentifier != null) copySession.MainSessionIdentifier =
         * new String(MainSessionIdentifier); if (PersistantIdentifier != null) copySession.PersistantIdentifier = new
         * String(PersistantIdentifier);
         */

        // copy string
        if (this.Browser != null) {
            copySession.Browser = this.Browser;
        }

        if (this.IPAddress != null) {
            copySession.IPAddress = this.IPAddress;
        }

        if (this.FirstClickSessionIdentifier != null) {
            copySession.FirstClickSessionIdentifier = this.FirstClickSessionIdentifier;
        }

        if (this.MainSessionIdentifier != null) {
            copySession.MainSessionIdentifier = this.MainSessionIdentifier;
        }

        if (this.PersistantIdentifier != null) {
            copySession.PersistantIdentifier = this.PersistantIdentifier;
        }

        if (this.StartPersistantIdentifier != null) {
            copySession.StartPersistantIdentifier = this.StartPersistantIdentifier;
        }

        // clone custom fields
        if (this.customField1 != null) {
            copySession.customField1 = this.customField1;
        }

        if (this.customField2 != null) {
            copySession.customField2 = this.customField2;
        }

        if (this.customField3 != null) {
            copySession.customField3 = this.customField3;
        }

        // clone dates
        if (this.FirstActivity != null) {
            copySession.FirstActivity = (java.util.Date) this.FirstActivity.clone();
        }

        if (this.LastActivity != null) {
            copySession.LastActivity = (java.util.Date) this.LastActivity.clone();
        }

        if (this.CookieKeepVariables != null) {
            copySession.CookieKeepVariables = this.CookieKeepVariables;
        }

        return copySession;
    }

    /**
     * Insert the method's description here. Creation date: (9/17/2002 6:12:11 PM)
     * 
     * @param pAlgorithmToConsider int
     * @param pDate the date
     * @param pLastMatchTimeOut the last match time out
     * 
     * @return boolean
     */
    private boolean disallowMatchingByThisAlgorithm(java.util.Date pDate, int pLastMatchTimeOut,
            int pAlgorithmToConsider) {
        java.util.Date dDateToMatch = null;

        switch (pAlgorithmToConsider) {
        case 1: // |3|5|7: // main session identifier
            dDateToMatch = this.MainSessionIdentifierLastMatch;

            break;

        case 2: // |6: // first click session identifier
            dDateToMatch = this.FirstClickSessionIdentifierLastMatch;

            break;

        case 4: // persistant identifier
            dDateToMatch = this.PersistantIdentifierLastMatch;

            break;

        case 24: // ip and browser
            dDateToMatch = this.IPAddressAndBrowserLastMatch;

            break;
        }

        if (dDateToMatch == null) {
            return (false);
        }

        // then
        if ((pAlgorithmToConsider > this.BestSessionMatchedBy) && (this.BestSessionMatchedBy != 0)
                && (this.SessionMatchedBy != 0)
                && ((pDate.getTime() - dDateToMatch.getTime()) >= (pLastMatchTimeOut * 1000))) {
            return true;
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:04:28 PM)
     * 
     * @param pCode int
     */
    public void expireForCode(int pCode) {
        switch (pCode) {
        case 1:
            this.setMainSessionIdentifierIndexed(false);

            break;

        case 2:
            this.setFirstClickSessionIdentifierIndexed(false);

            break;

        case 4:
            this.setPersistantIdentifierIndexed(false);

            break;

        case 24:
            this.setIPAddressAndBrowserIndexed(false);

            break;
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 10:21:47 AM)
     * 
     * @return int
     */
    public int getBestMatchedByCode() {
        return this.BestSessionMatchedBy;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 8:48:05 PM)
     * 
     * @return double
     */
    public final long getID() {
        return this.ID;
    }

    /**
     * Insert the method's description here. Creation date: (4/16/2002 4:29:08 PM)
     * 
     * @param pDestinationObjectType the destination object type
     * 
     * @return int
     */
    public int getMatchingAlgorithmCode(int pDestinationObjectType) {
        // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS, 13 =
        // PERSISTANT_IDENTIFIER
        switch (pDestinationObjectType) {
        case 9:
            return (1);

        case 10:
            return (2);

        case 13:
            return (4);

        case 11:
            return (8);

        case 12:
            return (16);
        }

        return (0);
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 10:21:47 AM)
     * 
     * @return int
     */
    public int getSessionMatchedByCode() {
        return this.SessionMatchedBy;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:04:06 PM)
     * 
     * @return boolean
     */
    public boolean isExpired() {
        boolean res = false;
        this.IndexLock.getReadLock();

        if ((this.isMainSessionIdentifierIndexed() == false) && (this.isPersistantIdentifierIndexed() == false)
                && (this.isIPAddressAndBrowserIndexed() == false)
                && (this.isFirstClickSessionIdentifierIndexed() == false)) {
            res = true;
        }
        else {
            res = false;
        }

        this.IndexLock.releaseLock();
        this.expired = true;

        return res;
    }

    /**
     * Insert the method's description here. Creation date: (9/18/2002 11:47:55 AM)
     * 
     * @return boolean
     */
    public boolean isFirstClickSessionIdentifierAllowIndex() {
        return this.FirstClickSessionIdentifierAllowIndex;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @return boolean
     */
    public boolean isFirstClickSessionIdentifierIndexed() {
        return this.FirstClickSessionIdentifierIndexed;
    }

    /**
     * Insert the method's description here. Creation date: (9/18/2002 11:47:55 AM)
     * 
     * @return boolean
     */
    public boolean isIPAddressAndBrowserAllowIndex() {
        return this.IPAddressAndBrowserAllowIndex;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @return boolean
     */
    public boolean isIPAddressAndBrowserIndexed() {
        return this.IPAddressAndBrowserIndexed;
    }

    /**
     * Insert the method's description here. Creation date: (9/18/2002 11:47:55 AM)
     * 
     * @return boolean
     */
    public boolean isMainSessionIdentifierAllowIndex() {
        return this.MainSessionIdentifierAllowIndex;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @return boolean
     */
    public boolean isMainSessionIdentifierIndexed() {
        return this.MainSessionIdentifierIndexed;
    }

    /**
     * Insert the method's description here. Creation date: (9/18/2002 11:47:55 AM)
     * 
     * @return boolean
     */
    public boolean isPersistantIdentifierAllowIndex() {
        return this.PersistantIdentifierAllowIndex;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @return boolean
     */
    public boolean isPersistantIdentifierIndexed() {
        return this.PersistantIdentifierIndexed;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @return boolean
     */
    public boolean isPreviouslyFoundOnMainSessionIdentifier() {
        return this.PreviouslyFoundOnMainSessionIdentifier;
    }

    /**
     * Insert the method's description here. Creation date: (4/21/2002 9:53:09 AM)
     * 
     * @return boolean
     */
    public boolean isRepeatVisitor() {
        return this.RepeatVisitor;
    }

    /**
     * Insert the method's description here. Creation date: (4/18/2002 8:18:31 PM)
     * 
     * @param pDate java.util.Date
     * @param pTimeOut int
     * 
     * @return boolean
     */
    public boolean isStillValid(java.util.Date pDate, int pTimeOut) {
        if ((pDate.getTime() - this.LastActivity.getTime()) <= (pTimeOut * 1000)) {
            return (true);
        }

        return (false);
    }

    /**
     * Insert the method's description here. Creation date: (4/18/2002 8:18:31 PM)
     * 
     * @param pDate java.util.Date
     * @param pTimeOut int
     * @param pLastMatchTimeOut the last match time out
     * @param pAlgorithmToUse the algorithm to use
     * @param pEnableFallback the enable fallback
     * 
     * @return boolean
     */
    public boolean isStillValid(java.util.Date pDate, int pTimeOut, int pLastMatchTimeOut, int pAlgorithmToUse,
            boolean pEnableFallback) {
        // if session timeout exceeded then timeout session as normal
        if ((pDate.getTime() - this.LastActivity.getTime()) >= (pTimeOut * 1000)) {
            // if (this.FirstClickSessionIdentifier != null &&
            // this.FirstClickSessionIdentifier.indexOf("226.199.161.208681024730146880") > 0) {
            // int rs = 0;
            // rs = 1;
            // }
            return (false);

            // if session has been matched by something better and the last match on this algorithm
            // was greater than last match timeout then mark session as false and not to use this
            // id again for life of session
        }
        else if ((pAlgorithmToUse > this.BestSessionMatchedBy) && (this.BestSessionMatchedBy != 0)
                && (this.SessionMatchedBy != 0)
                && (this.disallowMatchingByThisAlgorithm(pDate, pLastMatchTimeOut, pAlgorithmToUse) == true)) {
            // if session timed out on lack of match using given algorithm then do not allow
            // matching on this algorithm again - protects again joining sessions
            if (pEnableFallback == false) {
                this.toggleIndex(false, pAlgorithmToUse);
            }

            return (false);
        }
        else {
            return (true);
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:46:20 PM)
     */
    public void resetIndexes() {
        this.setPersistantIdentifierIndexed(false);
        this.setIPAddressAndBrowserIndexed(false);
        this.setMainSessionIdentifierIndexed(false);
        this.setFirstClickSessionIdentifierIndexed(false);
        this.setPreviouslyFoundOnMainSessionIdentifier(false);
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @param newFirstClickSessionIdentifierIndexed boolean
     */
    public void setFirstClickSessionIdentifierIndexed(boolean newFirstClickSessionIdentifierIndexed) {
        this.IndexLock.getWriteLock();

        this.FirstClickSessionIdentifierIndexed = newFirstClickSessionIdentifierIndexed;

        this.IndexLock.releaseLock();

        if (newFirstClickSessionIdentifierIndexed) {
            // set first matched by to lastactivity as the object is not
            this.FirstClickSessionIdentifierLastMatch = this.LastActivity;
        }
        else {
            this.FirstClickSessionIdentifierLastMatch = null;
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 8:48:05 PM)
     * 
     * @param newID double
     */
    public void setID(long newID) {
        this.ID = newID;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @param newIPAddressAndBrowserIndexed boolean
     */
    public void setIPAddressAndBrowserIndexed(boolean newIPAddressAndBrowserIndexed) {
        this.IndexLock.getWriteLock();

        this.IPAddressAndBrowserIndexed = newIPAddressAndBrowserIndexed;

        this.IndexLock.releaseLock();

        if (newIPAddressAndBrowserIndexed) {
            // set first matched by to lastactivity as the object is not
            this.IPAddressAndBrowserLastMatch = this.LastActivity;
        }

        // else
        // this.IPAddressAndBrowserLastMatch = null;
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @param newMainSessionIdentifierIndexed boolean
     */
    public void setMainSessionIdentifierIndexed(boolean newMainSessionIdentifierIndexed) {
        this.IndexLock.getWriteLock();
        this.MainSessionIdentifierIndexed = newMainSessionIdentifierIndexed;
        this.IndexLock.releaseLock();

        if (newMainSessionIdentifierIndexed) {
            // set first matched by to lastactivity as the object is not
            this.MainSessionIdentifierLastMatch = this.LastActivity;
        }
        else {
            this.MainSessionIdentifierLastMatch = null;
        }
    }

    /**
     * Insert the method's description here. Creation date: (9/17/2002 6:51:39 PM)
     * 
     * @param pCode int
     * @param pDate java.util.Date
     */
    public void setMatchedByDateForCode(int pCode, Date pDate) {
        switch (pCode) {
        case 1: // |3|5|7: // main session identifier
            this.MainSessionIdentifierLastMatch = pDate;

            break;

        case 2: // |6: // first click session identifier
            this.FirstClickSessionIdentifierLastMatch = pDate;

            break;

        case 4: // persistant identifier
            this.PersistantIdentifierLastMatch = pDate;

            break;

        case 24: // ip and browser
            this.IPAddressAndBrowserLastMatch = pDate;

            break;
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/19/2002 8:04:28 PM)
     * 
     * @param pCode int
     */
    public void setMatchingAlgorithm(int pCode) {
        this.SessionMatchedBy = pCode;

        if ((pCode < this.BestSessionMatchedBy) || (this.BestSessionMatchedBy == 0)) {
            this.BestSessionMatchedBy = pCode;
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @param newPersistantIdentifierIndexed boolean
     */
    public void setPersistantIdentifierIndexed(boolean newPersistantIdentifierIndexed) {
        this.IndexLock.getWriteLock();

        this.PersistantIdentifierIndexed = newPersistantIdentifierIndexed;

        this.IndexLock.releaseLock();

        if (newPersistantIdentifierIndexed) {
            // set first matched by to lastactivity as the object is not
            this.PersistantIdentifierLastMatch = this.LastActivity;
        }
        else {
            this.PersistantIdentifierLastMatch = null;
        }
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 2:56:43 PM)
     * 
     * @param newPreviouslyFoundOnMainSessionIdentifier boolean
     */
    public void setPreviouslyFoundOnMainSessionIdentifier(boolean newPreviouslyFoundOnMainSessionIdentifier) {
        this.PreviouslyFoundOnMainSessionIdentifier = newPreviouslyFoundOnMainSessionIdentifier;
    }

    /**
     * Insert the method's description here. Creation date: (4/21/2002 9:53:09 AM)
     * 
     * @param newRepeatVisitor boolean
     */
    public void setRepeatVisitor(boolean newRepeatVisitor) {
        this.RepeatVisitor = newRepeatVisitor;
    }

    /**
     * Insert the method's description here. Creation date: (9/17/2002 5:52:48 PM)
     * 
     * @param pAllowIndex boolean
     * @param pForAlgorithm the for algorithm
     */
    private void toggleIndex(boolean pAllowIndex, int pForAlgorithm) {
        switch (pForAlgorithm) {
        case 1: // |3|5|7: // main session identifier
            this.MainSessionIdentifierAllowIndex = pAllowIndex;

            break;

        case 2: // |6: // first click session identifier
            this.FirstClickSessionIdentifierAllowIndex = pAllowIndex;

            break;

        case 4: // persistant identifier
            this.PersistantIdentifierAllowIndex = pAllowIndex;

            break;

        case 24: // ip and browser
            this.IPAddressAndBrowserAllowIndex = pAllowIndex;

            break;
        }
    }

}
