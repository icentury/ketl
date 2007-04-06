/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;


/**
 * Insert the type's description here.
 * Creation date: (4/8/2002 3:27:55 PM)
 * @author: Administrator
 *
 * Session Matching Values BIT MASK
 * 1 = MainSessionIdentifier
 * 2 = FirstClickIdentifier
 * 4 = Browser
 * 8 = IP Only
 *
 */
import java.util.Date;

import com.kni.etl.EngineConstants;
import com.kni.etl.ReadWriteLock;
import com.kni.etl.ketl_v1.ResultRecord;


public class Session extends ResultRecord
{
    /**
     *
     */
    private static final long serialVersionUID = 3977299923580500018L;

    // public attributes
    public long ID;
    public boolean lastHitStored = false;
    public boolean expired = false;
    public java.util.Date FirstActivity;
    public java.util.Date LastActivity;
    public String Browser;
    public String IPAddress;
    public String MainSessionIdentifier;
    public String StartPersistantIdentifier;
    public String PersistantIdentifier;
    public String FirstClickSessionIdentifier;
    public String customField3;
    public String customField2;
    public String customField1;
    public Object[] lastHit = null;
    public java.lang.String[][] CookieKeepVariables;
    public long Hit = 0;
    public long PageViews = 0;
    public int DM_LOAD_ID = -1;
    public int LOAD_ID = -1;
    public boolean PreviouslyFoundOnMainSessionIdentifier = false;
    public boolean RepeatVisitor = false;
    public String Referrer = null;

    // private attributes
    private int SessionMatchedBy = 0;
    private int BestSessionMatchedBy = 0;
    private boolean IPAddressAndBrowserIndexed = false;
    private boolean MainSessionIdentifierIndexed = false;
    private boolean FirstClickSessionIdentifierIndexed = false;
    private boolean PersistantIdentifierIndexed = false;
    private boolean IPAddressAndBrowserAllowIndex = true;
    private boolean MainSessionIdentifierAllowIndex = true;
    private boolean FirstClickSessionIdentifierAllowIndex = true;
    private boolean PersistantIdentifierAllowIndex = true;
    private java.util.Date IPAddressAndBrowserLastMatch = null;
    private java.util.Date MainSessionIdentifierLastMatch = null;
    private java.util.Date FirstClickSessionIdentifierLastMatch = null;
    private java.util.Date PersistantIdentifierLastMatch = null;
    private ReadWriteLock IndexLock;
    public static final int SESSION = 1;
    java.util.Date mLastReferrerURLPageDate = null;

    

    /**
     * Session constructor comment.
     */
    public Session()
    {
        super();

        this.Type = Session.SESSION;

        this.IndexLock = new ReadWriteLock();
    }

    public void syncReferrer(String pReferrerURL, Date pPageDate)
    {
        if ((this.mLastReferrerURLPageDate == null) || pPageDate.before(this.mLastReferrerURLPageDate))
        {
            this.Referrer = pReferrerURL;
            this.mLastReferrerURLPageDate = pPageDate;
        }
    }

    public boolean addSessionIdentifier(String pValue, int pDestinationObjectType)
    {
        // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS
        switch (pDestinationObjectType)
        {
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

            if (this.StartPersistantIdentifier == null)
            {
                this.StartPersistantIdentifier = pValue;
            }

            this.PersistantIdentifier = pValue;

            return (true);
        }

        return (false);
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/11/2002 3:16:30 PM)
     * @param pValue java.lang.String
     * @param pVariableName java.lang.String
     *
     *
     *
     */
    public boolean addSessionVariable(String pValue, int pDestinationObjectType, String pVariableName)
    {
        // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS
        switch (pDestinationObjectType)
        {
        case EngineConstants.KEEP_COOKIE_VARIABLE:

            int fieldCnt = 0;

            if (this.CookieKeepVariables == null)
            {
                this.CookieKeepVariables = new String[1][2];
            }
            else
            {
                fieldCnt = this.CookieKeepVariables.length;

                /** if already found keep first value **/
                for (int i = 0; i < fieldCnt; i++)
                {
                    if (this.CookieKeepVariables[i][0] == pVariableName)
                    {
                        return (true);
                    }
                }

                /** got this far so we must allocate space for variable */
                String[][] tmp = new String[fieldCnt + 1][2];

                for (int i = 0; i < fieldCnt; i++)
                {
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
     * Insert the method's description here.
     * Creation date: (4/15/2002 10:23:51 AM)
     * @return datasources.Session
     */
    @Override
    public Object clone()
    {
        Session copySession = null;

        try
        {
            copySession = (Session) super.clone();
        }
        catch (CloneNotSupportedException e)
        {
            // this should never happen
            throw new InternalError(e.toString());
        }

        // copy string

        /*
        if (Browser != null)
            copySession.Browser = new String(Browser);
        if (IPAddress != null)
            copySession.IPAddress = new String(IPAddress);
        if (FirstClickSessionIdentifier != null)
            copySession.FirstClickSessionIdentifier = new String(FirstClickSessionIdentifier);
        if (MainSessionIdentifier != null)
            copySession.MainSessionIdentifier = new String(MainSessionIdentifier);
        if (PersistantIdentifier != null)
            copySession.PersistantIdentifier = new String(PersistantIdentifier);
        */

        // copy string
        if (this.Browser != null)
        {
            copySession.Browser = this.Browser;
        }

        if (this.IPAddress != null)
        {
            copySession.IPAddress = this.IPAddress;
        }

        if (this.FirstClickSessionIdentifier != null)
        {
            copySession.FirstClickSessionIdentifier = this.FirstClickSessionIdentifier;
        }

        if (this.MainSessionIdentifier != null)
        {
            copySession.MainSessionIdentifier = this.MainSessionIdentifier;
        }

        if (this.PersistantIdentifier != null)
        {
            copySession.PersistantIdentifier = this.PersistantIdentifier;
        }

        if (this.StartPersistantIdentifier != null)
        {
            copySession.StartPersistantIdentifier = this.StartPersistantIdentifier;
        }

        // clone custom fields
        if (this.customField1 != null)
        {
            copySession.customField1 = this.customField1;
        }

        if (this.customField2 != null)
        {
            copySession.customField2 = this.customField2;
        }

        if (this.customField3 != null)
        {
            copySession.customField3 = this.customField3;
        }

        // clone dates
        if (this.FirstActivity != null)
        {
            copySession.FirstActivity = (java.util.Date) this.FirstActivity.clone();
        }

        if (this.LastActivity != null)
        {
            copySession.LastActivity = (java.util.Date) this.LastActivity.clone();
        }

        if (this.CookieKeepVariables != null)
        {
            copySession.CookieKeepVariables = this.CookieKeepVariables;
        }

        return copySession;
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/17/2002 6:12:11 PM)
     * @return boolean
     * @param pAlgorithmToConsider int
     */
    private boolean disallowMatchingByThisAlgorithm(java.util.Date pDate, int pLastMatchTimeOut,
        int pAlgorithmToConsider)
    {
        java.util.Date dDateToMatch = null;

        switch (pAlgorithmToConsider)
        {
        case 1: //|3|5|7:		// main session identifier
            dDateToMatch = this.MainSessionIdentifierLastMatch;

            break;

        case 2: //|6:  // first click session identifier
            dDateToMatch = this.FirstClickSessionIdentifierLastMatch;

            break;

        case 4: // persistant identifier
            dDateToMatch = this.PersistantIdentifierLastMatch;

            break;

        case 24: // ip and browser
            dDateToMatch = this.IPAddressAndBrowserLastMatch;

            break;
        }

        if (dDateToMatch == null)
        {
            return (false);
        }

        // then 
        if ((pAlgorithmToConsider > this.BestSessionMatchedBy) && (this.BestSessionMatchedBy != 0) &&
                (this.SessionMatchedBy != 0) &&
                ((pDate.getTime() - dDateToMatch.getTime()) >= (pLastMatchTimeOut * 1000)))
        {
            return true;
        }

        return false;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:04:28 PM)
     * @param pCode int
     */
    public void expireForCode(int pCode)
    {
        switch (pCode)
        {
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
     * Insert the method's description here.
     * Creation date: (4/20/2002 10:21:47 AM)
     * @return int
     */
    public int getBestMatchedByCode()
    {
        return this.BestSessionMatchedBy;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:48:05 PM)
     * @return double
     */
    public final long getID()
    {
        return this.ID;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/16/2002 4:29:08 PM)
     * @return int
     * @param DestinationObjectType int
     */
    public int getMatchingAlgorithmCode(int pDestinationObjectType)
    {
        // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS, 13 = PERSISTANT_IDENTIFIER
        switch (pDestinationObjectType)
        {
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
     * Insert the method's description here.
     * Creation date: (4/20/2002 10:21:47 AM)
     * @return int
     */
    public int getSessionMatchedByCode()
    {
        return this.SessionMatchedBy;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:04:06 PM)
     * @return boolean
     */
    public boolean isExpired()
    {
        boolean res = false;
        this.IndexLock.getReadLock();

        if ((this.isMainSessionIdentifierIndexed() == false) && (this.isPersistantIdentifierIndexed() == false) &&
                (this.isIPAddressAndBrowserIndexed() == false) && (this.isFirstClickSessionIdentifierIndexed() == false))
        {
            res = true;
        }
        else
        {
            res = false;
        }

        this.IndexLock.releaseLock();
        this.expired = true;

        return res;
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/18/2002 11:47:55 AM)
     * @return boolean
     */
    public boolean isFirstClickSessionIdentifierAllowIndex()
    {
        return this.FirstClickSessionIdentifierAllowIndex;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @return boolean
     */
    public boolean isFirstClickSessionIdentifierIndexed()
    {
        return this.FirstClickSessionIdentifierIndexed;
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/18/2002 11:47:55 AM)
     * @return boolean
     */
    public boolean isIPAddressAndBrowserAllowIndex()
    {
        return this.IPAddressAndBrowserAllowIndex;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @return boolean
     */
    public boolean isIPAddressAndBrowserIndexed()
    {
        return this.IPAddressAndBrowserIndexed;
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/18/2002 11:47:55 AM)
     * @return boolean
     */
    public boolean isMainSessionIdentifierAllowIndex()
    {
        return this.MainSessionIdentifierAllowIndex;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @return boolean
     */
    public boolean isMainSessionIdentifierIndexed()
    {
        return this.MainSessionIdentifierIndexed;
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/18/2002 11:47:55 AM)
     * @return boolean
     */
    public boolean isPersistantIdentifierAllowIndex()
    {
        return this.PersistantIdentifierAllowIndex;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @return boolean
     */
    public boolean isPersistantIdentifierIndexed()
    {
        return this.PersistantIdentifierIndexed;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @return boolean
     */
    public boolean isPreviouslyFoundOnMainSessionIdentifier()
    {
        return this.PreviouslyFoundOnMainSessionIdentifier;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/21/2002 9:53:09 AM)
     * @return boolean
     */
    public boolean isRepeatVisitor()
    {
        return this.RepeatVisitor;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/18/2002 8:18:31 PM)
     * @return boolean
     * @param pDate java.util.Date
     * @param pTimeOut int
     */
    public boolean isStillValid(java.util.Date pDate, int pTimeOut)
    {
        if ((pDate.getTime() - this.LastActivity.getTime()) <= (pTimeOut * 1000))
        {
            return (true);
        }

        return (false);
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/18/2002 8:18:31 PM)
     * @return boolean
     * @param pDate java.util.Date
     * @param pTimeOut int
     */
    public boolean isStillValid(java.util.Date pDate, int pTimeOut, int pLastMatchTimeOut, int pAlgorithmToUse,
        boolean pEnableFallback)
    {
        // if session timeout exceeded then timeout session as normal
        if ((pDate.getTime() - this.LastActivity.getTime()) >= (pTimeOut * 1000))
        {
            // if (this.FirstClickSessionIdentifier != null && this.FirstClickSessionIdentifier.indexOf("226.199.161.208681024730146880") > 0) {
            //     int rs = 0;
            //     rs = 1;
            // }
            return (false);

            // if session has been matched by something better and the last match on this algorithm
            // was greater than last match timeout then mark session as false and not to use this
            // id again for life of session
        }
        else if ((pAlgorithmToUse > this.BestSessionMatchedBy) && (this.BestSessionMatchedBy != 0) && (this.SessionMatchedBy != 0) &&
                (this.disallowMatchingByThisAlgorithm(pDate, pLastMatchTimeOut, pAlgorithmToUse) == true))
        {
            // if session timed out on lack of match using given algorithm then do not allow
            // matching on this algorithm again - protects again joining sessions
            if (pEnableFallback == false)
            {
                this.toggleIndex(false, pAlgorithmToUse);
            }

            return (false);
        }
        else
        {
            return (true);
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:46:20 PM)
     */
    public void resetIndexes()
    {
        this.setPersistantIdentifierIndexed(false);
        this.setIPAddressAndBrowserIndexed(false);
        this.setMainSessionIdentifierIndexed(false);
        this.setFirstClickSessionIdentifierIndexed(false);
        this.setPreviouslyFoundOnMainSessionIdentifier(false);
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @param newFirstClickSessionIdentifierIndexed boolean
     */
    public void setFirstClickSessionIdentifierIndexed(boolean newFirstClickSessionIdentifierIndexed)
    {
        this.IndexLock.getWriteLock();

        this.FirstClickSessionIdentifierIndexed = newFirstClickSessionIdentifierIndexed;

        this.IndexLock.releaseLock();

        if (newFirstClickSessionIdentifierIndexed)
        {
            // set first matched by to lastactivity as the object is not
            this.FirstClickSessionIdentifierLastMatch = this.LastActivity;
        }
        else
        {
            this.FirstClickSessionIdentifierLastMatch = null;
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 8:48:05 PM)
     * @param newID double
     */
    public void setID(long newID)
    {
        this.ID = newID;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @param newIPAddressAndBrowserIndexed boolean
     */
    public void setIPAddressAndBrowserIndexed(boolean newIPAddressAndBrowserIndexed)
    {
        this.IndexLock.getWriteLock();

        this.IPAddressAndBrowserIndexed = newIPAddressAndBrowserIndexed;

        this.IndexLock.releaseLock();

        if (newIPAddressAndBrowserIndexed)
        {
            // set first matched by to lastactivity as the object is not
            this.IPAddressAndBrowserLastMatch = this.LastActivity;
        }

        //else
        //   this.IPAddressAndBrowserLastMatch = null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @param newMainSessionIdentifierIndexed boolean
     */
    public void setMainSessionIdentifierIndexed(boolean newMainSessionIdentifierIndexed)
    {
        this.IndexLock.getWriteLock();
        this.MainSessionIdentifierIndexed = newMainSessionIdentifierIndexed;
        this.IndexLock.releaseLock();

        if (newMainSessionIdentifierIndexed)
        {
            // set first matched by to lastactivity as the object is not
            this.MainSessionIdentifierLastMatch = this.LastActivity;
        }
        else
        {
            this.MainSessionIdentifierLastMatch = null;
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/17/2002 6:51:39 PM)
     * @param pCode int
     * @param pDate java.util.Date
     */
    public void setMatchedByDateForCode(int pCode, Date pDate)
    {
        switch (pCode)
        {
        case 1: //|3|5|7:		// main session identifier
            this.MainSessionIdentifierLastMatch = pDate;

            break;

        case 2: //|6:  // first click session identifier
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
     * Insert the method's description here.
     * Creation date: (4/19/2002 8:04:28 PM)
     * @param pCode int
     */
    public void setMatchingAlgorithm(int pCode)
    {
        this.SessionMatchedBy = pCode;

        if ((pCode < this.BestSessionMatchedBy) || (this.BestSessionMatchedBy == 0))
        {
            this.BestSessionMatchedBy = pCode;
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @param newPersistantIdentifierIndexed boolean
     */
    public void setPersistantIdentifierIndexed(boolean newPersistantIdentifierIndexed)
    {
        this.IndexLock.getWriteLock();

        this.PersistantIdentifierIndexed = newPersistantIdentifierIndexed;

        this.IndexLock.releaseLock();

        if (newPersistantIdentifierIndexed)
        {
            // set first matched by to lastactivity as the object is not
            this.PersistantIdentifierLastMatch = this.LastActivity;
        }
        else
        {
            this.PersistantIdentifierLastMatch = null;
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 2:56:43 PM)
     * @param newPreviouslyFoundOnMainSessionIdentifier boolean
     */
    public void setPreviouslyFoundOnMainSessionIdentifier(boolean newPreviouslyFoundOnMainSessionIdentifier)
    {
        this.PreviouslyFoundOnMainSessionIdentifier = newPreviouslyFoundOnMainSessionIdentifier;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/21/2002 9:53:09 AM)
     * @param newRepeatVisitor boolean
     */
    public void setRepeatVisitor(boolean newRepeatVisitor)
    {
        this.RepeatVisitor = newRepeatVisitor;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 7:49:02 PM)
     */
    @Override
    public void sync()
    {
        super.sync();

        // add items linefields
        //   if (MainSessionIdentifier != null) {
        //   
        //  }
    }

    /**
     * Insert the method's description here.
     * Creation date: (9/17/2002 5:52:48 PM)
     * @param pAllowIndex boolean
     */
    private void toggleIndex(boolean pAllowIndex, int pForAlgorithm)
    {
        switch (pForAlgorithm)
        {
        case 1: //|3|5|7:		// main session identifier
            this.MainSessionIdentifierAllowIndex = pAllowIndex;

            break;

        case 2: //|6:  // first click session identifier
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

    /* (non-Javadoc)
     * @see com.kni.etl.ResultRecord#CopyTo(com.kni.etl.ResultRecord)
     */
    @Override
    public ResultRecord CopyTo(ResultRecord newRecord)
    {
        Session rec = (Session) newRecord;
        rec.ID = this.ID;
        rec.lastHitStored = this.lastHitStored;
        rec.expired = this.expired;
        rec.FirstActivity = this.FirstActivity;
        rec.LastActivity = this.LastActivity;
        rec.Browser = this.Browser;
        rec.IPAddress = this.IPAddress;
        rec.MainSessionIdentifier = this.MainSessionIdentifier;
        rec.StartPersistantIdentifier = this.StartPersistantIdentifier;
        rec.PersistantIdentifier = this.PersistantIdentifier;
        rec.FirstClickSessionIdentifier = this.FirstClickSessionIdentifier;
        rec.customField3 = this.customField3;
        rec.customField2 = this.customField2;
        rec.customField1 = this.customField1;
        rec.lastHit = this.lastHit;
        rec.CookieKeepVariables = this.CookieKeepVariables;
        rec.Hit = this.Hit;
        rec.PageViews = this.PageViews;
        rec.DM_LOAD_ID = this.DM_LOAD_ID;
        rec.LOAD_ID = this.LOAD_ID;
        rec.PreviouslyFoundOnMainSessionIdentifier = this.PreviouslyFoundOnMainSessionIdentifier;
        rec.RepeatVisitor = this.RepeatVisitor;
        rec.Referrer = this.Referrer;

        // private attributes
        rec.SessionMatchedBy = this.SessionMatchedBy;
        rec.BestSessionMatchedBy = this.BestSessionMatchedBy;
        rec.IPAddressAndBrowserIndexed = this.IPAddressAndBrowserIndexed;
        rec.MainSessionIdentifierIndexed = this.MainSessionIdentifierIndexed;
        rec.FirstClickSessionIdentifierIndexed = this.FirstClickSessionIdentifierIndexed;
        rec.PersistantIdentifierIndexed = this.PersistantIdentifierIndexed;
        rec.IPAddressAndBrowserAllowIndex = this.IPAddressAndBrowserAllowIndex;
        rec.MainSessionIdentifierAllowIndex = this.MainSessionIdentifierAllowIndex;
        rec.FirstClickSessionIdentifierAllowIndex = this.FirstClickSessionIdentifierAllowIndex;
        rec.PersistantIdentifierAllowIndex = this.PersistantIdentifierAllowIndex;
        rec.IPAddressAndBrowserLastMatch = this.IPAddressAndBrowserLastMatch;
        rec.MainSessionIdentifierLastMatch = this.MainSessionIdentifierLastMatch;
        rec.FirstClickSessionIdentifierLastMatch = this.FirstClickSessionIdentifierLastMatch;
        rec.PersistantIdentifierLastMatch = this.PersistantIdentifierLastMatch;
        rec.IndexLock = this.IndexLock;
        rec.mLastReferrerURLPageDate = this.mLastReferrerURLPageDate;

        return newRecord;
    }
}
