/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;
import com.kni.etl.urltools.URLCleaner;


public class HitDatabaseWriter extends JDBCSessionizationWriterRoot
{
    /**
     * @param esJobStatus
     */
    void writeRecord(ResultRecord resultRecord) throws SQLException, Exception
    {
        if ((resultRecord != null) && (resultRecord.Type == PageView.PAGEVIEW))
        {
            PageView currentPageView = (PageView) resultRecord;

            boolean performInsert = true;

            // put substr into oracle statement as extra protection
            if (stmt == null)
            {
                this.resolveHitColumnMaps();
                stmt = ((Connection) dbConnection).prepareStatement(buildStatement());

                // supply batch size and the number of timestamp in the statement
                buildTimestampCache();
            }

            // do the get request first as it will exclude hits if need be
            if (maHitParameterPosition[GET_REQUEST] != 0)
            {
                if ((currentPageView.getGetRequest() != null) && (currentPageView.getGetRequest().isNull() == false))
                {
                    String str = currentPageView.getGetRequest().getString();

                    if (urlCleaner == null)
                    {
                        urlCleaner = new URLCleaner();
                        urlCleaner.setPageParserDefinitions(this.PageParserDefinitions);
                    }

                    int errorCode = -1;

                    if ((currentPageView.getHTMLErrorCode() != null) &&
                            (currentPageView.getHTMLErrorCode().isNull() == false))
                    {
                        errorCode = currentPageView.getHTMLErrorCode().getInteger();
                    }

                    try
                    {
                        str = urlCleaner.cleanHTTPRequest(str, errorCode, 0, EngineConstants.MAX_REQUEST_LENGTH, true);
                    }
                    catch (Exception e)
                    {
                        ResourcePool.LogMessage(this, "URL Decoder exception: " + e);
                    }

                    if (urlCleaner.cleansed == true)
                    {
                        currentPageView.Session.PageViews++;
                        currentPageView.setPageSequenceID(currentPageView.Session.PageViews);
                    }
                    else if (this.PagesOnly == true)
                    {
                        performInsert = false;
                    }

                    stmt.setString(maHitParameterPosition[GET_REQUEST],
                        fieldTrim(str, this.maHitParameterSize[GET_REQUEST]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[GET_REQUEST], java.sql.Types.VARCHAR);
                    urlCleaner.cleansed = false;

                    if (this.PagesOnly == true)
                    {
                        performInsert = false;
                    }
                }
            }

            if (performInsert)
            {
                if (maHitParameterPosition[LOAD_ID] != 0)
                {
                    stmt.setInt(maHitParameterPosition[LOAD_ID], (LoadID));
                }

                if (maHitParameterPosition[DM_LOAD_ID] != 0)
                {
                    stmt.setInt(maHitParameterPosition[DM_LOAD_ID], (DMLoadID));
                }

                if (maHitParameterPosition[ASSOCIATED_HITS] != 0)
                {
                    stmt.setInt(maHitParameterPosition[ASSOCIATED_HITS], 0);
                }

                if (maHitParameterPosition[TEMP_SESSION_ID] != 0)
                {
                    stmt.setLong(maHitParameterPosition[TEMP_SESSION_ID], currentPageView.getSessionID());
                }

                if (maHitParameterPosition[ACTIVITY_DT] != 0)
                {
                    if ((currentPageView.getHitDateTime() != null) &&
                            (currentPageView.getHitDateTime().getDate() != null))
                    {
                        Timestamp tms = getCachedTimeStamp(insertCnt);
                        tms.setTime(currentPageView.getHitDateTime().getDate().getTime());
                        stmt.setTimestamp(maHitParameterPosition[ACTIVITY_DT], tms);
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[ACTIVITY_DT], java.sql.Types.TIMESTAMP);
                    }
                }

                if (maHitParameterPosition[PAGE_SEQUENCE] != 0)
                {
                    if (currentPageView.getPageSequence() == -1)
                    {
                        stmt.setNull(maHitParameterPosition[PAGE_SEQUENCE], Types.NUMERIC);
                    }
                    else
                    {
                        stmt.setLong(maHitParameterPosition[PAGE_SEQUENCE], currentPageView.getPageSequence());
                    }
                }

                if (maHitParameterPosition[STATUS] != 0)
                {
                    if ((currentPageView.getHTMLErrorCode() != null) &&
                            (currentPageView.getHTMLErrorCode().isNull() == false))
                    {
                        stmt.setInt(maHitParameterPosition[STATUS], (currentPageView.getHTMLErrorCode().getInteger()));
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[STATUS], java.sql.Types.INTEGER);
                    }
                }

                if (maHitParameterPosition[SERVER_NAME] != 0)
                {
                    if ((currentPageView.getServerName() != null) &&
                            (currentPageView.getServerName().isNull() == false))
                    {
                        stmt.setString(maHitParameterPosition[SERVER_NAME],
                            (currentPageView.getServerName().getString()));
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[SERVER_NAME], java.sql.Types.VARCHAR);
                    }
                }

                if (maHitParameterPosition[BYTES_SENT] != 0)
                {
                    if ((currentPageView.getBytesSent() != null) && (currentPageView.getBytesSent().isNull() == false))
                    {
                        stmt.setDouble(maHitParameterPosition[BYTES_SENT], (currentPageView.getBytesSent().getDouble()));
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[BYTES_SENT], java.sql.Types.DOUBLE);
                    }
                }

                if (maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST] != 0)
                {
                    if ((currentPageView.getTimeTakenToServeRequest() != null) &&
                            (currentPageView.getTimeTakenToServeRequest().isNull() == false))
                    {
                        stmt.setDouble(maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST],
                            (currentPageView.getTimeTakenToServeRequest().getDouble()));
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST], java.sql.Types.DOUBLE);
                    }
                }

                if (maHitParameterPosition[CANONICAL_SERVER_PORT] != 0)
                {
                    if ((currentPageView.getCanonicalServerPort() != null) &&
                            (currentPageView.getCanonicalServerPort().isNull() == false))
                    {
                        stmt.setInt(maHitParameterPosition[CANONICAL_SERVER_PORT],
                            (currentPageView.getCanonicalServerPort().getInteger()));
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[CANONICAL_SERVER_PORT], java.sql.Types.INTEGER);
                    }
                }

                if (maHitParameterPosition[REFERRER_URL] != 0)
                {
                    if ((currentPageView.getReferrerURL() != null) &&
                            (currentPageView.getReferrerURL().isNull() == false))
                    {
                        String str = currentPageView.getReferrerURL().getString();

                        if (referrerURLCleaner == null)
                        {
                            referrerURLCleaner = new URLCleaner();
                            referrerURLCleaner.setPageParserDefinitions(this.PageParserDefinitions);
                        }

                        try
                        {
                            //str = referrerURLCleaner.decode(str, this.MAX_REFERRER_LENGTH);
                            str = referrerURLCleaner.cleanURL(str, -1, 0, EngineConstants.MAX_REQUEST_LENGTH, true);

                            if (urlCleaner.cleansed && (maHitParameterPosition[GET_REQUEST] != 0) &&
                                    (maHitParameterPosition[ACTIVITY_DT] != 0))
                            {
                                currentPageView.Session.syncReferrer(str, currentPageView.getHitDateTime().getDate());
                            }
                        }
                        catch (Exception e)
                        {
                            ResourcePool.LogMessage(this, "URL Cleaner exception: " + e);
                        }

                        stmt.setString(maHitParameterPosition[REFERRER_URL],
                            fieldTrim(str, this.maHitParameterSize[REFERRER_URL]));
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[REFERRER_URL], java.sql.Types.VARCHAR);
                    }
                }

                if (maHitParameterPosition[CLEANSED] != 0)
                {
                    if (urlCleaner.cleansed == true)
                    {
                        stmt.setInt(maHitParameterPosition[CLEANSED], 1);
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[CLEANSED], java.sql.Types.INTEGER);
                    }
                }

                if (maHitParameterPosition[CLEANSED_ID] != 0)
                {
                    if (urlCleaner.cleansed == true)
                    {
                        stmt.setInt(maHitParameterPosition[CLEANSED_ID], urlCleaner.cleansedWithID);
                    }
                    else
                    {
                        stmt.setNull(maHitParameterPosition[CLEANSED_ID], java.sql.Types.INTEGER);
                    }
                }

                if (currentPageView.Session.lastHit == currentPageView)
                {
                    currentPageView.Session.lastHitStored = true;
                }

                // execute statement
                addBatch();
            }
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#numberOfTimestampsPerStatement()
     */
    int numberOfTimestampsPerStatement()
    {
        return 1;
    }
}
