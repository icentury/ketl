/*
 * LMR, Version 2.0
 *
 * Copyright (C) 2003 by Metapa, Inc. All Rights Reserved.
 *
 * $Id: PGCopyHitDatabaseWriter.java,v 1.1 2006/12/13 07:06:43 nwakefield Exp $
 */
package com.kni.etl.sessionizer;

import java.sql.SQLException;
import java.sql.Types;
import java.util.GregorianCalendar;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;
import com.kni.etl.urltools.URLCleaner;

/**
 * A concrete implementation of the CDBSessionizaationRoot class; this class allows to store hit information identified
 * by KISessionizer into the temp_hit table in CDB database.
 * 
 * @author <a href="mohit@metapa.net">Mohit Kumar</a>
 * @version $Revision: 1.1 $
 * @TODO Currently does not build against the KETL.jar. Need to get updated KETL.jar to build this.
 */
public class PGCopyHitDatabaseWriter extends PGCopySessionizationWriterRoot {

    /**
     * Constant specifying GET_REQUEST column size
     */
    public static final int GET_REQUEST_SIZE = 1000;

    /**
     * Constant specifying REFERRER_URL column size
     */
    public static final int REFERRER_URL_SIZE = 1000;

    // PG writer requires the order of parameter setting to be consistent with the order in the code,
    // this array reflects the code order and is used to generate the load command in the correct order
    public static final int[] definedColumnOrder = { GET_REQUEST, LOAD_ID, DM_LOAD_ID, ASSOCIATED_HITS,
            TEMP_SESSION_ID, ACTIVITY_DT, PAGE_SEQUENCE, STATUS, SERVER_NAME, BYTES_SENT, TIME_TAKEN_TO_SERV_REQUEST,
            CANONICAL_SERVER_PORT, REFERRER_URL, CLEANSED, CLEANSED_ID };

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
        // put substr into oracle statement as extra protection
        if (notBuilt) {
            this.resolveHitColumnMaps();

            this.buildStatement();

            notBuilt = false;
        }

        if ((resultRecord != null) && (resultRecord.Type == PageView.PAGEVIEW)) {
            PageView currentPageView = (PageView) resultRecord;

            boolean performInsert = true;

            // do the get request first as it will exclude hits if need be
            if (maHitParameterPosition[GET_REQUEST] != 0) {
                if ((currentPageView.getGetRequest() != null) && (currentPageView.getGetRequest().isNull() == false)) {
                    String str = currentPageView.getGetRequest().getString();

                    if (urlCleaner == null) {
                        urlCleaner = new URLCleaner();
                        urlCleaner.setPageParserDefinitions(this.PageParserDefinitions);
                    }

                    int errorCode = -1;

                    if ((currentPageView.getHTMLErrorCode() != null)
                            && (currentPageView.getHTMLErrorCode().isNull() == false)) {
                        errorCode = currentPageView.getHTMLErrorCode().getInteger();
                    }

                    try {
                        str = urlCleaner.cleanHTTPRequest(str, errorCode, 0, EngineConstants.MAX_REQUEST_LENGTH, true);
                    } catch (Exception e) {
                        ResourcePool.LogMessage(this, "URL Decoder exception: " + e);
                    }

                    if (urlCleaner.cleansed == true) {
                        currentPageView.Session.PageViews++;
                        currentPageView.setPageSequenceID(currentPageView.Session.PageViews);
                    }
                    else if (this.PagesOnly == true) {
                        performInsert = false;
                    }

                    // set field if insert allowed
                    if (performInsert) {
                        stmt.setString(maHitParameterPosition[GET_REQUEST], str);
                    }
                }
                else {
                    if (this.PagesOnly == true) {
                        performInsert = false;
                    }

                    // set field if insert allowed
                    if (performInsert) {
                        stmt.setNull(maHitParameterPosition[GET_REQUEST], java.sql.Types.VARCHAR);
                    }

                    urlCleaner.cleansed = false;
                }
            }

            if (performInsert) {
                if (maHitParameterPosition[LOAD_ID] != 0) {
                    stmt.setInt(maHitParameterPosition[LOAD_ID], (LoadID));
                }

                if (maHitParameterPosition[DM_LOAD_ID] != 0) {
                    stmt.setInt(maHitParameterPosition[DM_LOAD_ID], (DMLoadID));
                }

                if (maHitParameterPosition[ASSOCIATED_HITS] != 0) {
                    stmt.setInt(maHitParameterPosition[ASSOCIATED_HITS], 0);
                }

                if (maHitParameterPosition[TEMP_SESSION_ID] != 0) {
                    stmt.setLong(maHitParameterPosition[TEMP_SESSION_ID], currentPageView.getSessionID());
                }

                if (maHitParameterPosition[ACTIVITY_DT] != 0) {
                    java.util.Date dt = currentPageView.getHitDateTime().getDate();

                    if (dt != null) {
                        stmt.setTimestamp(maHitParameterPosition[ACTIVITY_DT], dt);
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[ACTIVITY_DT], java.sql.Types.TIMESTAMP);
                    }
                }

                if (maHitParameterPosition[PAGE_SEQUENCE] != 0) {
                    if (currentPageView.getPageSequence() == -1) {
                        stmt.setNull(maHitParameterPosition[PAGE_SEQUENCE], Types.NUMERIC);
                    }
                    else {
                        stmt.setLong(maHitParameterPosition[PAGE_SEQUENCE], currentPageView.getPageSequence());
                    }
                }

                if (maHitParameterPosition[STATUS] != 0) {
                    if ((currentPageView.getHTMLErrorCode() != null)
                            && (currentPageView.getHTMLErrorCode().isNull() == false)) {
                        stmt.setInt(maHitParameterPosition[STATUS], (currentPageView.getHTMLErrorCode().getInteger()));
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[STATUS], java.sql.Types.INTEGER);
                    }
                }

                if (maHitParameterPosition[SERVER_NAME] != 0) {
                    if ((currentPageView.getServerName() != null)
                            && (currentPageView.getServerName().isNull() == false)) {
                        stmt.setString(maHitParameterPosition[SERVER_NAME], (currentPageView.getServerName()
                                .getString()));
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[SERVER_NAME], java.sql.Types.VARCHAR);
                    }
                }

                if (maHitParameterPosition[BYTES_SENT] != 0) {
                    if ((currentPageView.getBytesSent() != null) && (currentPageView.getBytesSent().isNull() == false)) {
                        stmt.setDouble(maHitParameterPosition[BYTES_SENT],
                                (currentPageView.getBytesSent().getDouble()), 0);
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[BYTES_SENT], java.sql.Types.DOUBLE);
                    }
                }

                if (maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST] != 0) {
                    if ((currentPageView.getTimeTakenToServeRequest() != null)
                            && (currentPageView.getTimeTakenToServeRequest().isNull() == false)) {
                        stmt.setDouble(maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST], (currentPageView
                                .getTimeTakenToServeRequest().getDouble()), 0);
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST], java.sql.Types.DOUBLE);
                    }
                }

                if (maHitParameterPosition[CANONICAL_SERVER_PORT] != 0) {
                    if ((currentPageView.getCanonicalServerPort() != null)
                            && (currentPageView.getCanonicalServerPort().isNull() == false)) {
                        stmt.setInt(maHitParameterPosition[CANONICAL_SERVER_PORT], (currentPageView
                                .getCanonicalServerPort().getInteger()));
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[CANONICAL_SERVER_PORT], java.sql.Types.INTEGER);
                    }
                }

                if (maHitParameterPosition[REFERRER_URL] != 0) {
                    if ((currentPageView.getReferrerURL() != null)
                            && (currentPageView.getReferrerURL().isNull() == false)) {
                        String str = currentPageView.getReferrerURL().getString();

                        if (referrerURLCleaner == null) {
                            referrerURLCleaner = new URLCleaner();
                            referrerURLCleaner.setPageParserDefinitions(this.PageParserDefinitions);
                        }

                        try {
                            // str = referrerURLCleaner.decode(str, this.MAX_REFERRER_LENGTH);
                            str = referrerURLCleaner.cleanURL(str, -1, 0, EngineConstants.MAX_REQUEST_LENGTH, true);

                            if (urlCleaner.cleansed && (maHitParameterPosition[GET_REQUEST] != 0)
                                    && (maHitParameterPosition[ACTIVITY_DT] != 0)) {
                                currentPageView.Session.syncReferrer(str, currentPageView.getHitDateTime().getDate());
                            }
                        } catch (Exception e) {
                            ResourcePool.LogMessage(this, "URL Cleaner exception: " + e);
                        }

                        stmt.setString(maHitParameterPosition[REFERRER_URL], str);
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[REFERRER_URL], java.sql.Types.VARCHAR);
                    }
                }

                if (maHitParameterPosition[CLEANSED] != 0) {
                    if (urlCleaner.cleansed == true) {
                        stmt.setInt(maHitParameterPosition[CLEANSED], 1);
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[CLEANSED], java.sql.Types.INTEGER);
                    }
                }

                if (maHitParameterPosition[CLEANSED_ID] != 0) {
                    if (urlCleaner.cleansed == true) {
                        stmt.setInt(maHitParameterPosition[CLEANSED_ID], urlCleaner.cleansedWithID);
                    }
                    else {
                        stmt.setNull(maHitParameterPosition[CLEANSED_ID], java.sql.Types.INTEGER);
                    }
                }

                if (currentPageView.Session.lastHit == currentPageView) {
                    currentPageView.Session.lastHitStored = true;
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
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#numberOfTimestampsPerStatement()
     */
    int numberOfTimestampsPerStatement() {
        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#setDatabase(java.lang.String[])
     */
    public boolean setDatabase(String[] pFields) {
        // TODO Auto-generated method stub
        return false;
    }

    void sortColumnsForStatementCreation() {
        this.sortColumns(definedColumnOrder);
    }
}
