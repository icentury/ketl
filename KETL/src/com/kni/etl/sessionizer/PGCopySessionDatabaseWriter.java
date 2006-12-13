/*
 * LMR, Version 2.0
 *
 * Copyright (C) 2003 by Metapa, Inc. All Rights Reserved.
 *
 * $Id: PGCopySessionDatabaseWriter.java,v 1.1 2006/12/13 07:06:42 nwakefield Exp $
 */
package com.kni.etl.sessionizer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.GregorianCalendar;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;

/**
 * A concrete implementation of the CDBSessionizaationRoot class; this class allows to store session information
 * identified by KISessionizer into the temp_session table in CDB database.
 * 
 * @author <a href="mohit@metapa.net">Mohit Kumar</a>
 * @version $Revision: 1.1 $
 * @TODO Currently does not build against the KETL.jar. Need to get updated KETL.jar to build this.
 */
public class PGCopySessionDatabaseWriter extends PGCopySessionizationWriterRoot {

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#setDatabase(java.lang.String[])
     */
    public boolean setDatabase(String[] pFields) {
        // TODO Auto-generated method stub
        return false;
    }

    // PG writer requires the order of parameter setting to be consistent with the order in the code,
    // this array reflects the code order and is used to generate the load command in the correct order
    public static final int[] definedColumnOrder = { LOAD_ID, DM_LOAD_ID, TEMP_SESSION_ID,
            FIRST_CLICK_SESSION_IDENTIFIER, PERSISTANT_IDENTIFIER, MAIN_SESSION_IDENTIFIER, IP_ADDRESS, REFERRER,
            FIRST_SESSION_ACTIVITY, LAST_SESSION_ACTIVITY, BROWSER, REPEAT_VISITOR, HITS, PAGEVIEWS, KEEP_VARIABLES,
            START_PERSISTANT_IDENTIFIER, DNS_ADDRESS, DNS_ADDRESS_TRANSLATION_LEVEL, SOURCE_FILE, CUSTOMFIELD1,
            CUSTOMFIELD2, CUSTOMFIELD3 };
    static final String IP_ADDRESS_NOT_TRANSLATED = null;
    static final String IP_ADDRESS_TRANSLATED = "T";
    static final String IP_ADDRESS_PARTIALLY_TRANSLATED = "P";
    static final String IP_ADDRESS_NOT_FOUND = "N";
    String sDNSTranslationLevel = IP_ADDRESS_NOT_TRANSLATED;

    /**
     * Constant specifying MAIN_SESSION_IDENTIFIER column size
     */
    public static final int MAIN_SESSION_IDENTIFIER_SIZE = 100;

    /**
     * Constant specifying FIRST_CLICK_SESSION_IDENTIFIER column size
     */
    public static final int FIRST_CLICK_SESSION_IDENTIFIER_SIZE = 100;

    /**
     * Constant specifying FIRST_CLICK_SESSION_IDENTIFIER column size
     */
    public static final int START_PERSISTANT_IDENTIFIER_SIZE = 100;

    /**
     * Constant specifying PERSISTANT_IDENTIFIER column size
     */
    public static final int PERSISTANT_IDENTIFIER_SIZE = 100;

    /**
     * Constant specifying IP_ADDRESS column size
     */
    public static final int IP_ADDRESS_SIZE = 500;

    /**
     * Constant specifying DNS_ADDRESS column size
     */
    public static final int DNS_ADDRESS_SIZE = 256;

    /**
     * Constant specifying SOURCE_FILE column size
     */
    public static final int SOURCE_FILE_SIZE = 255;

    /**
     * Constant specifying DNS_ADDRESS_TRANSLATION column size
     */
    public static final int DNS_ADDRESS_TRANSLATION_LEVEL_SIZE = 256;

    /**
     * Constant specifying KEEP_VARIABLES column size
     */
    public static final int KEEP_VARIABLES_SIZE = 255;

    /**
     * Constant specifying BROWSER column size
     */
    public static final int BROWSER_SIZE = 256;

    /**
     * StringBuffer
     */
    StringBuffer sBuf = null;

    /**
     * The attribute is used for the purpose of converting timestamp to calendar objects for use within rowLoader API
     */
    GregorianCalendar calendar = new GregorianCalendar();
    boolean notBuilt = true;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#writeRecord(com.kni.etl.ResultRecord)
     */
    void writeRecord(ResultRecord resultRecord) throws Exception {
        if (notBuilt) {
            this.resolveSessionColumnMaps();

            this.buildStatement();

            notBuilt = false;
        }

        if ((resultRecord != null) && (resultRecord.Type == Session.SESSION)) {
            Session sessionToStore = (Session) resultRecord;

            if (maHitParameterPosition[LOAD_ID] != 0) {
                stmt.setInt(maHitParameterPosition[LOAD_ID], (LoadID));
            }

            if (maHitParameterPosition[DM_LOAD_ID] != 0) {
                stmt.setInt(maHitParameterPosition[DM_LOAD_ID], (DMLoadID));
            }

            if (maHitParameterPosition[TEMP_SESSION_ID] != 0) {
                stmt.setLong(maHitParameterPosition[TEMP_SESSION_ID], sessionToStore.getID());
            }

            if (maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER] != 0) {
                if (sessionToStore.FirstClickSessionIdentifier != null) {
                    stmt.setString(maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER],
                            sessionToStore.FirstClickSessionIdentifier);
                }
                else {
                    stmt.setNull(maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[PERSISTANT_IDENTIFIER] != 0) {
                if (sessionToStore.PersistantIdentifier != null) {
                    stmt.setString(maHitParameterPosition[PERSISTANT_IDENTIFIER], sessionToStore.PersistantIdentifier);
                }
                else {
                    stmt.setNull(maHitParameterPosition[PERSISTANT_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[MAIN_SESSION_IDENTIFIER] != 0) {
                if (sessionToStore.MainSessionIdentifier != null) {
                    stmt.setString(maHitParameterPosition[MAIN_SESSION_IDENTIFIER],
                            sessionToStore.MainSessionIdentifier);
                }
                else {
                    stmt.setNull(maHitParameterPosition[MAIN_SESSION_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[IP_ADDRESS] != 0) {
                if (sessionToStore.IPAddress != null) {
                    stmt.setString(maHitParameterPosition[IP_ADDRESS], sessionToStore.IPAddress);
                }
                else {
                    stmt.setNull(maHitParameterPosition[IP_ADDRESS], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[REFERRER] != 0) {
                if (sessionToStore.Referrer == null) {
                    stmt.setNull(maHitParameterPosition[REFERRER], java.sql.Types.VARCHAR);
                }
                else {
                    stmt.setString(maHitParameterPosition[REFERRER], sessionToStore.Referrer);
                }
            }

            if (maHitParameterPosition[FIRST_SESSION_ACTIVITY] != 0) {
                if (sessionToStore.FirstActivity != null) {
                    stmt.setTimestamp(maHitParameterPosition[FIRST_SESSION_ACTIVITY], sessionToStore.FirstActivity);
                }
                else {
                    stmt.setNull(maHitParameterPosition[FIRST_SESSION_ACTIVITY], java.sql.Types.TIMESTAMP);
                }
            }

            if (maHitParameterPosition[LAST_SESSION_ACTIVITY] != 0) {
                if (sessionToStore.LastActivity != null) {
                    stmt.setTimestamp(maHitParameterPosition[LAST_SESSION_ACTIVITY], sessionToStore.LastActivity);
                }
                else {
                    stmt.setNull(maHitParameterPosition[LAST_SESSION_ACTIVITY], java.sql.Types.TIMESTAMP);
                }
            }

            if (maHitParameterPosition[BROWSER] != 0) {
                if (sessionToStore.Browser != null) {
                    stmt.setString(maHitParameterPosition[BROWSER], sessionToStore.Browser);
                }
                else {
                    stmt.setNull(maHitParameterPosition[BROWSER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[REPEAT_VISITOR] != 0) {
                // missing referrer
                if (sessionToStore.isRepeatVisitor() == true) {
                    stmt.setInt(maHitParameterPosition[REPEAT_VISITOR], (1));
                }
                else {
                    stmt.setInt(maHitParameterPosition[REPEAT_VISITOR], (0));
                }
            }

            if (maHitParameterPosition[HITS] != 0) {
                stmt.setLong(maHitParameterPosition[HITS], sessionToStore.Hit);
            }

            if (maHitParameterPosition[PAGEVIEWS] != 0) {
                stmt.setLong(maHitParameterPosition[PAGEVIEWS], sessionToStore.PageViews);
            }

            if (maHitParameterPosition[KEEP_VARIABLES] != 0) {
                if ((sessionToStore.CookieKeepVariables != null) && (sessionToStore.CookieKeepVariables.length > 0)) {
                    if (sBuf == null) {
                        sBuf = new StringBuffer(EngineConstants.MAX_KEEP_VARIABLE_LENGTH);
                    }
                    else {
                        sBuf.delete(0, EngineConstants.MAX_KEEP_VARIABLE_LENGTH - 1);
                    }

                    for (int i = sessionToStore.CookieKeepVariables.length - 1; i >= 0; i--) {
                        sBuf.append(sessionToStore.CookieKeepVariables[i][0]).append('=').append(
                                sessionToStore.CookieKeepVariables[i][1]).append(';');
                    }

                    stmt.setString(maHitParameterPosition[KEEP_VARIABLES], sBuf.toString());
                }
                else {
                    stmt.setNull(maHitParameterPosition[KEEP_VARIABLES], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[START_PERSISTANT_IDENTIFIER] != 0) {
                if (sessionToStore.StartPersistantIdentifier != null) {
                    stmt.setString(maHitParameterPosition[START_PERSISTANT_IDENTIFIER],
                            sessionToStore.StartPersistantIdentifier);
                }
                else {
                    stmt.setNull(maHitParameterPosition[START_PERSISTANT_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            // reset to not translated
            sDNSTranslationLevel = IP_ADDRESS_NOT_TRANSLATED;

            if (maHitParameterPosition[DNS_ADDRESS] != 0) {
                if (sessionToStore.IPAddress != null) {
                    InetAddress addr;

                    try {
                        addr = InetAddress.getByName(sessionToStore.IPAddress);

                        sDNSTranslationLevel = IP_ADDRESS_TRANSLATED;

                        String sAddr = addr.getHostName();

                        stmt.setString(maHitParameterPosition[DNS_ADDRESS], sAddr);
                    } catch (UnknownHostException e) {
                        sDNSTranslationLevel = IP_ADDRESS_NOT_FOUND;
                        stmt.setNull(maHitParameterPosition[DNS_ADDRESS], java.sql.Types.VARCHAR);
                    }
                }
                else {
                    stmt.setNull(maHitParameterPosition[DNS_ADDRESS], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL] != 0) {
                if (sessionToStore.IPAddress != null) {
                    stmt.setString(maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL], sDNSTranslationLevel);
                }
                else {
                    stmt.setNull(maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[SOURCE_FILE] != 0) {
                if (sessionToStore.SourceFile != null) {
                    stmt.setString(maHitParameterPosition[SOURCE_FILE], sessionToStore.SourceFile);
                }
                else {
                    stmt.setNull(maHitParameterPosition[SOURCE_FILE], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[CUSTOMFIELD1] != 0) {
                if (sessionToStore.customField1 != null) {
                    stmt.setString(maHitParameterPosition[CUSTOMFIELD1], sessionToStore.customField1);
                }
                else {
                    stmt.setNull(maHitParameterPosition[CUSTOMFIELD1], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[CUSTOMFIELD2] != 0) {
                if (sessionToStore.customField2 != null) {
                    stmt.setString(maHitParameterPosition[CUSTOMFIELD2], sessionToStore.customField2);
                }
                else {
                    stmt.setNull(maHitParameterPosition[CUSTOMFIELD2], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[CUSTOMFIELD3] != 0) {
                if (sessionToStore.customField3 != null) {
                    stmt.setString(maHitParameterPosition[CUSTOMFIELD3], sessionToStore.customField3);
                }
                else {
                    stmt.setNull(maHitParameterPosition[CUSTOMFIELD3], java.sql.Types.VARCHAR);
                }
            }

            // execute statement
            if (this.isSkipInserts() == false) {
                try {
                    stmt.addBatch();
                } catch (SQLException e) {
                    ResourcePool.LogMessage(e.toString());
                    this.fatalError = true;
                    run = null;
                }
            }

            insertCnt++;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#numberOfTimestampsPerStatement()
     */
    int numberOfTimestampsPerStatement() {
        return 3;
    }

    void sortColumnsForStatementCreation() {
        this.sortColumns(definedColumnOrder);
    }
}
