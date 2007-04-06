/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;
import com.kni.etl.stringtools.BoyerMooreAlgorithm;
import com.kni.etl.stringtools.StringManipulator;
import com.kni.etl.urltools.URLCleaner;


public class AnalyzeSessionBuilder implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 3617291220687402039L;
    transient private SessionDefinition ActiveSessionDefinition;
    transient private StringManipulator strMan = new StringManipulator();
    private AnalyzeSessionStore ActiveSessionStore;
    transient private Session CurrentSession = new Session();
    private int DM_LOAD_ID = -1;
    private int LOAD_ID = -1;
    transient private WebServerSettings[] mWebServerSettings;
    static final int MAXSEARCHBUFFER = 1000;
    static final int WEIGHT = 0;
    static final int SESSIONIDENTIFIER = 1;
    static final int ENDOFLIST = -1;
    transient char[] searchBuffer = new char[AnalyzeSessionBuilder.MAXSEARCHBUFFER];
    int[][] sessionWeightIndex = null;
    int sessionWeightIndexLength;
    int[][] sessionIdentifierObjectIndex = null;
    boolean mbPagesOnly = false;
    boolean mbHitsCanBeSkipped = true;
    transient PageParserPageDefinition[] mPageParserDefinition;

    public void setLoadIDs(int pLoadID, int pdmLoadID)
    {
        this.DM_LOAD_ID = pdmLoadID;
    }

    public void setIDCounter(IDCounter pStartSessionID)
    {
        this.ActiveSessionStore.setIDCounter(pStartSessionID);
    }

    public void preLoadWebServerSettings()
    {
        //      --------------- Preload End Marker and Seperators as
        // Strings---------------
        // Apache
        this.mWebServerSettings = new WebServerSettings[EngineConstants.MAX_WEBSERVERS];
        this.mWebServerSettings[EngineConstants.APACHE] = new WebServerSettings();
        this.mWebServerSettings[EngineConstants.NETSCAPE] = new WebServerSettings();

        // apache cookie seps
        String[] a2 = { "=", ":" };

        // url netscape end marker
        String[] b1 = { "?", " ", "&", "%3F", "%24" };

        // netscape url seperator
        String[] b2 = { "=", "$", "%3D", "%24" };

        String[] c1 = { " " };

        // netscape cookie seperator
        String[] d2 = { "=" };

        // apache url end markers
        String[] e1 = { "?", " " };

        // apache netscape cookie endmark
        String[] e2 = { ";", " " };

        this.mWebServerSettings[EngineConstants.APACHE].addWebServerPair(WebServerSettings.URL, e1, a2);
        this.mWebServerSettings[EngineConstants.NETSCAPE].addWebServerPair(WebServerSettings.URL, b1, b2);
        this.mWebServerSettings[EngineConstants.APACHE].addWebServerPair(WebServerSettings.COOKIE, e2, a2);
        this.mWebServerSettings[EngineConstants.NETSCAPE].addWebServerPair(WebServerSettings.COOKIE, e2, d2);
        this.mWebServerSettings[EngineConstants.APACHE].addWebServerPair(WebServerSettings.OTHER, c1, e2);
        this.mWebServerSettings[EngineConstants.NETSCAPE].addWebServerPair(WebServerSettings.OTHER, c1, e2);
    }

    /**
     * SessionBuilder constructor comment.
     */
    /**
     * SessionBuilder constructor comment.
     */
    public AnalyzeSessionBuilder(int pLoadID, int pdmLoadID, IDCounter pStartSessionID)
    {
        super();

        this.setLoadIDs(pLoadID, pdmLoadID);

        this.ActiveSessionStore = new AnalyzeSessionStore(pStartSessionID);
        this.preLoadWebServerSettings();
    }

    public void setPagesOnly(PageParserPageDefinition[] mpDef, boolean pmbPagesOnly, boolean pmbHitCountRequired)
    {
        this.mbPagesOnly = pmbPagesOnly;
        this.mPageParserDefinition = mpDef;

        if (pmbHitCountRequired)
        {
            this.mbHitsCanBeSkipped = false;
        }
        else
        {
            this.mbHitsCanBeSkipped = true;
        }
    }

    int idxGetRequestField = -1;
    int idxErrorCodeField = -1;
    transient URLCleaner urlCleaner = null;
    public boolean ignoreHit = false;

    /**
     * Insert the method's description here. Creation date: (4/8/2002 2:40:02
     * PM)
     *
     * @return int
     * @param pRecord
     *            datasources.ResultRecord
     *
     * ObjectType(s) 1 : IP Address 2 : In Cookie 3 : out Cookie 4 : URL Request
     * 5 : Browser identification 6 : HTML request error 7 : Default 8 : Data
     * and Time of Hit
     *
     */
    public final Session analyzeHit(ResultRecord pRecord)
    {
        // ActiveSessionDefinition
        boolean HitAnalyzed = false;
        boolean SessionLocated = false;
        Session MatchingSession = null;
        int recordFields = pRecord.LineFields.length;

        // set start weight to 1
        int pos = 0;
        int weight = 1;

        int SessionMatchingAlgorithmToUse = 0;

        // purge current session if used previously
        this.purgeCurrentSession();

        // record source file
        this.CurrentSession.SourceFile = pRecord.SourceFile;

        // set timing attributes of session, when hit occured
        this.setEstimatedTime(pRecord, recordFields);

        // set custom fields if any
        this.setCustomFields(pRecord, recordFields);

        // if last activity is null then hit is invalid, probably error in log
        // file
        if (this.CurrentSession.LastActivity == null)
        {
            return null;
        }

        String cleansedURL = null;

        // if pages only then parse url to see if it is a valid page
        // if it is record string and set request to string post session recognition
        // if it isn't then ignore page and move on.
        if (this.mbPagesOnly && this.mbHitsCanBeSkipped)
        {
            // if get request field not found then locate it
            // and record its pos for future recognition
            if (this.idxGetRequestField == -1)
            {
                for (int index = 0; index < recordFields; index++)
                {
                    if (pRecord.LineFields[index].ObjectType == EngineConstants.GET_REQUEST)
                    {
                        this.idxGetRequestField = index;
                    }

                    if (pRecord.LineFields[index].ObjectType == EngineConstants.HTML_ERROR_CODE)
                    {
                        this.idxErrorCodeField = index;
                    }
                }
            }

            if (this.urlCleaner == null)
            {
                this.urlCleaner = new URLCleaner();
                this.urlCleaner.setPageParserDefinitions(this.mPageParserDefinition);
            }

            cleansedURL = pRecord.LineFields[this.idxGetRequestField].getString();

            int errorCode = pRecord.LineFields[this.idxErrorCodeField].getInteger();

            try
            {
                cleansedURL = this.urlCleaner.cleanHTTPRequest(cleansedURL, errorCode, 0,
                        EngineConstants.MAX_REQUEST_LENGTH, true);
            }
            catch (Exception e)
            {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "URL Decoder exception: " + e);
            }

            if (this.urlCleaner.cleansed == true)
            {
                this.ignoreHit = false;
            }
            else
            {
                this.ignoreHit = true;

                return null;
            }
        }

        while (HitAnalyzed == false)
        {
            // get session key id
            if (this.ActiveSessionDefinition.SessionIdentifiers[pos].Weight == weight)
            {
                for (int index = 0; index < recordFields; index++)
                {
                    if (pRecord.LineFields[index].ObjectType == this.ActiveSessionDefinition.SessionIdentifiers[pos].ObjectType)
                    {
                        String strValue = null;

                        switch (pRecord.LineFields[index].ObjectType)
                        {
                        case EngineConstants.IN_COOKIE:
                        case EngineConstants.OUT_COOKIE:
                        case EngineConstants.GET_REQUEST:
                        case EngineConstants.SESSION_MAIN_SESSION_IDENTIFIER:
                        case EngineConstants.SESSION_FIRST_CLICK_IDENTIFIER: // these

                            // strings
                            // contain
                            // variables
                            // which
                            // need
                            // extracting
                            if (pRecord.LineFields[index].getString() == null)
                            {
                                strValue = null;
                            }
                            else if (this.ActiveSessionDefinition.SessionIdentifiers[pos].VariableName != null)
                            {
                                int len = pRecord.LineFields[index].getString().length();

                                if (len >= this.searchBuffer.length)
                                {
                                    this.searchBuffer = new char[len];
                                }

                                pRecord.LineFields[index].getString().getChars(0, len, this.searchBuffer, 0);

                                try
                                {
                                    strValue = this.strMan.getVariableByName(this.searchBuffer, len,
                                            this.ActiveSessionDefinition.SessionIdentifiers[pos].searchAccelerator,
                                            this.getSeperatorsAsBoyerMoore(this.ActiveSessionDefinition.WebServerType,
                                                pRecord.LineFields[index].ObjectType),
                                            this.getEndMarkersAsBoyerMoore(this.ActiveSessionDefinition.WebServerType,
                                                pRecord.LineFields[index].ObjectType),
                                            this.ActiveSessionDefinition.SessionIdentifiers[pos].CaseSensitive);
                                }
                                catch (Exception e)
                                {
                                    String str = new String(this.searchBuffer) + ":" + len;

                                    System.out.println("AnalyzeHit getVariableByName:" + e.getMessage() + ", File " +
                                        pRecord.SourceFile + ":" + pRecord.SourceLine + ", Params:" + str);
                                    System.out.println();
                                }
                            }
                            else
                            {
                                strValue = pRecord.LineFields[index].getString();
                            }

                            // need to tidy this little hack up, if string =
                            // -invalid- then kick it out.
                            if ((strValue != null) &&
                                    (strValue.compareTo(EngineConstants.INVALID_MAIN_SESSION_IDENTIFIER_STRING) == 0))
                            {
                                strValue = null;
                            }

                            break;

                        case EngineConstants.IP_ADDRESS:
                        case EngineConstants.BROWSER: // these strings are

                            // direct copies
                            strValue = pRecord.LineFields[index].getString();

                            break;
                        }

                        if ((strValue != null) &&
                                (this.CurrentSession.addSessionIdentifier(strValue,
                                    this.ActiveSessionDefinition.SessionIdentifiers[pos].DestinationObjectType) == true))
                        {
                            index = pRecord.LineFields.length;

                            SessionMatchingAlgorithmToUse = SessionMatchingAlgorithmToUse +
                                this.CurrentSession.getMatchingAlgorithmCode(this.ActiveSessionDefinition.SessionIdentifiers[pos].DestinationObjectType);
                        }
                        else if (strValue != null)
                        {
                            /** * record variable if required ** */
                            this.CurrentSession.addSessionVariable(strValue,
                                this.ActiveSessionDefinition.SessionIdentifiers[pos].DestinationObjectType,
                                this.ActiveSessionDefinition.SessionIdentifiers[pos].VariableName);
                        }
                    }
                }

                // Move to next identifier
                pos++;

                if (pos >= this.ActiveSessionDefinition.SessionIdentifiers.length)
                {
                    HitAnalyzed = true;

                    // Identify session if not found already
                    if (SessionLocated == false)
                    {
                        // identify session using bitmask of algorithm
                        MatchingSession = this.ActiveSessionStore.getSessionBySelectedAlgorithm(this.CurrentSession,
                                SessionMatchingAlgorithmToUse);

                        if (MatchingSession != null)
                        {
                            // if main session identifier is different then user
                            // has started a new session
                            // despite being matched
                            //if (MatchingSession.MainSessionIdentifier != null
                            //    || CurrentSession.MainSessionIdentifier != null
                            //    || CurrentSession.MainSessionIdentifier !=
                            // MatchingSession.MainSessionIdentifier) {
                            //    SessionLocated = false;
                            //} else
                            SessionLocated = true;
                        }

                        SessionMatchingAlgorithmToUse = 0;
                    }

                    // if session was not identified therefore create a session
                    if (SessionLocated == false)
                    {
                        // assign a load and individual job id to the new
                        // session for logging
                        this.CurrentSession.DM_LOAD_ID = this.DM_LOAD_ID;
                        this.CurrentSession.LOAD_ID = this.LOAD_ID;
                        MatchingSession = this.ActiveSessionStore.addSession(this.CurrentSession);
                    }
                    else
                    {
                        // update current session with remainding information if
                        // matching session does not have it.
                        // update MatchingSession with any missing values
                        this.ActiveSessionStore.updateSessionStore(MatchingSession, this.CurrentSession);
                    }
                }
            }
            else if (this.ActiveSessionDefinition.SessionIdentifiers[pos].Weight > weight)
            {
                // Identify session if not found already
                if (SessionLocated == false)
                {
                    // identify session using bitmask of algorithm
                    MatchingSession = this.ActiveSessionStore.getSessionBySelectedAlgorithm(this.CurrentSession,
                            SessionMatchingAlgorithmToUse);
                }

                if (MatchingSession != null)
                {
                    // if main session identifier is different then user has
                    // started a new session
                    // despite being matched
                    //if (MatchingSession.MainSessionIdentifier == null
                    //    || CurrentSession.MainSessionIdentifier == null
                    //    || CurrentSession.MainSessionIdentifier ==
                    // MatchingSession.MainSessionIdentifier) {
                    SessionLocated = true;

                    //} else
                    //    SessionMatchingAlgorithmToUse = 0;
                }
                else
                {
                    SessionMatchingAlgorithmToUse = 0;
                }

                weight++;
            }
        }

        // store parsed url if pages only option was enabled, preventing reparsing
        if (this.mbPagesOnly && this.mbHitsCanBeSkipped)
        {
            pRecord.LineFields[this.idxGetRequestField].setString(cleansedURL);
        }

        return MatchingSession;
    }

    /**
     * @param pRecord
     * @param recordFields
     */
    private final void setEstimatedTime(ResultRecord pRecord, int recordFields)
    {
        // set timing attributes of session, when hit occured
        for (int index = 0; index < recordFields; index++)
        {
            if ((pRecord.LineFields[index].ObjectType == EngineConstants.HIT_DATE_TIME) &&
                    (pRecord.LineFields[index].isNull() == false))
            {
                this.CurrentSession.LastActivity = pRecord.LineFields[index].getDate();

                if (this.CurrentSession.FirstActivity == null)
                {
                    this.CurrentSession.FirstActivity = pRecord.LineFields[index].getDate();
                }

                // if hits are at a new date update current date in session
                // store if date is five seconds later
                // TODO: sometimes incoming data isn't clean change this to be
                // either side of 5 seconds
                if ((this.ActiveSessionStore.getCurrentDate() == null) ||
                        (this.CurrentSession.LastActivity.getTime() > (this.ActiveSessionStore.CurrentDate.getTime() + 30000)))
                {
                    this.ActiveSessionStore.setCurrentDate(this.CurrentSession.LastActivity.getTime());

                    this.ActiveSessionStore.findStaleSessions();
                }

                //	date time found then don't need to search anymore.
                index = recordFields;
            }
        }
    }

    private final void setCustomFields(ResultRecord pRecord, int recordFields)
    {
        // set timing attributes of session, when hit occured
        for (int index = 0; index < recordFields; index++)
        {
            if (pRecord.LineFields[index].isNull() == false)
            {
                switch (pRecord.LineFields[index].ObjectType)
                {
                case EngineConstants.CUSTOM_FIELD_1:
                    this.CurrentSession.customField1 = pRecord.LineFields[index].getString();

                    break;

                case EngineConstants.CUSTOM_FIELD_2:
                    this.CurrentSession.customField2 = pRecord.LineFields[index].getString();

                    break;

                case EngineConstants.CUSTOM_FIELD_3:
                    this.CurrentSession.customField3 = pRecord.LineFields[index].getString();

                    break;
                }
            }
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 11:31:33
     * PM)
     * @param pLastActivityNull if true then non closed sessions will have null last
     * activity
     */
    public final void closeOutAllSessions(boolean pLastActivityNull)
    {
        // set timing attributes of session, when hit occured
        this.ActiveSessionStore.closeOutAllSessions(pLastActivityNull);
    }

    /**
     * Insert the method's description here. Creation date: (4/20/2002 8:51:15
     * PM)
     */
    public final Vector getDoneSessionsQueue()
    {
        return (this.ActiveSessionStore.RemovedSessionsQueue);
    }

    /**
     * Insert the method's description here. Creation date: (4/16/2002 6:19:49
     * PM)
     *
     * @return char[]
     * @param pWebServerType
     *            int
     * @param pObjectType
     *            int
     */
    private final BoyerMooreAlgorithm[] getEndMarkersAsBoyerMoore(int pWebServerType, int pObjectType)
    {
        switch (pObjectType)
        {
        case EngineConstants.IN_COOKIE:
        case EngineConstants.OUT_COOKIE:
        case EngineConstants.SESSION_MAIN_SESSION_IDENTIFIER:
        case EngineConstants.SESSION_FIRST_CLICK_IDENTIFIER:
            return (this.mWebServerSettings[pWebServerType].getEndMarkersAsBoyerMoore(WebServerSettings.COOKIE));

        case EngineConstants.GET_REQUEST: // url variables
            return (this.mWebServerSettings[pWebServerType].getEndMarkersAsBoyerMoore(WebServerSettings.URL));
        }

        return (this.mWebServerSettings[pWebServerType].getEndMarkersAsBoyerMoore(WebServerSettings.OTHER));
    }

    public final IDCounter getLastSessionID()
    {
        if (this.ActiveSessionStore != null)
        {
            return this.ActiveSessionStore.getLastSessionID();
        }

        return (null);
    }

    private final BoyerMooreAlgorithm[] getSeperatorsAsBoyerMoore(int pWebServerType, int pObjectType)
    {
        switch (pObjectType)
        {
        case EngineConstants.IN_COOKIE:
        case EngineConstants.OUT_COOKIE:
        case EngineConstants.SESSION_MAIN_SESSION_IDENTIFIER:
        case EngineConstants.SESSION_FIRST_CLICK_IDENTIFIER:
            return (this.mWebServerSettings[pWebServerType].getSeperatorsAsBoyerMoore(WebServerSettings.COOKIE));

        case EngineConstants.GET_REQUEST: // url
            return (this.mWebServerSettings[pWebServerType].getSeperatorsAsBoyerMoore(WebServerSettings.URL));
        }

        // return a default of a space
        return (this.mWebServerSettings[pWebServerType].getSeperatorsAsBoyerMoore(WebServerSettings.OTHER));
    }

    /**
     * Insert the method's description here. Creation date: (4/16/2002 5:13:15
     * PM)
     */
    private final void purgeCurrentSession()
    {
        this.CurrentSession.setID(-1);
        this.CurrentSession.FirstActivity = null;
        this.CurrentSession.LastActivity = null;
        this.CurrentSession.customField1 = null;
        this.CurrentSession.customField2 = null;
        this.CurrentSession.customField3 = null;

        this.CurrentSession.Browser = null;
        this.CurrentSession.IPAddress = null;
        this.CurrentSession.MainSessionIdentifier = null;
        this.CurrentSession.FirstClickSessionIdentifier = null;
        this.CurrentSession.PersistantIdentifier = null;
        this.CurrentSession.StartPersistantIdentifier = null;

        if (this.CurrentSession.CookieKeepVariables != null)
        {
            this.CurrentSession.CookieKeepVariables = null;
        }

        this.CurrentSession.resetIndexes();
        this.CurrentSession.setID(-1);
    }

    /**
     * Insert the method's description here. Creation date: (4/9/2002 10:25:20
     * AM)
     *
     * @param pSessionDefinition
     *            datasources.SessionDefinition
     */
    public final void setSessionDefinition(SessionDefinition pSessionDefinition)
    {
        // set session first click timeout to be the same as main session
        // identifier!!
        if (this.ActiveSessionDefinition == null)
        {
            this.ActiveSessionDefinition = pSessionDefinition;
        }

        // initialize hashmaps to store session identifiers
        this.ActiveSessionStore.createHashMaps(pSessionDefinition);

        this.sessionWeightIndex = new int[this.ActiveSessionDefinition.SessionIdentifiers.length][2];

        int iWeight = 1;
        int iWeightPos = 0;

        // get session definition order
        while (iWeightPos < this.ActiveSessionDefinition.SessionIdentifiers.length)
        {
            for (int indexPosItem = 0; indexPosItem < this.ActiveSessionDefinition.SessionIdentifiers.length;
                    indexPosItem++)
            {
                if (this.ActiveSessionDefinition.SessionIdentifiers[indexPosItem].Weight == iWeight)
                {
                    this.sessionWeightIndex[iWeightPos][AnalyzeSessionBuilder.WEIGHT] = iWeight;
                    this.sessionWeightIndex[iWeightPos][AnalyzeSessionBuilder.SESSIONIDENTIFIER] = indexPosItem;
                    iWeightPos++;
                }
            }

            iWeight++;
        }

        this.sessionWeightIndexLength = this.sessionWeightIndex.length;
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
            this.CurrentSession = new Session();
            this.ActiveSessionStore.setCurrentDate(new java.util.Date(1).getTime());
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
