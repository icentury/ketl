/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.sql.SQLException;
import java.sql.Timestamp;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;
import com.kni.etl.urltools.URLCleaner;


public class HitTestWriter extends TestSessionizationWriterRoot
{
    /**
     * @param esJobStatus
     */
    Timestamp tms = new Timestamp(0);

    void writeRecord(ResultRecord resultRecord) throws SQLException, Exception
    {
        if ((resultRecord != null) && (resultRecord.Type == PageView.PAGEVIEW))
        {
            PageView currentPageView = (PageView) resultRecord;

            boolean performInsert = true;

            // put substr into oracle statement as extra protection
            if (stmt == null)
            {
                this.buildStatement();

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
                }
                else
                {
                    urlCleaner.cleansed = false;

                    if (this.PagesOnly == true)
                    {
                        performInsert = false;
                    }
                }
            }

            if (performInsert)
            {
                if (maHitParameterPosition[TEMP_SESSION_ID] != 0)
                {
                    currentPageView.getSessionID();
                }

                if (maHitParameterPosition[ACTIVITY_DT] != 0)
                {
                    if ((currentPageView.getHitDateTime() != null) &&
                            (currentPageView.getHitDateTime().getDate() != null))
                    {
                        tms.setTime(currentPageView.getHitDateTime().getDate().getTime());
                    }
                }

                if (maHitParameterPosition[PAGE_SEQUENCE] != 0)
                {
                    currentPageView.getPageSequence();
                }

                if (maHitParameterPosition[STATUS] != 0)
                {
                    if ((currentPageView.getHTMLErrorCode() != null) &&
                            (currentPageView.getHTMLErrorCode().isNull() == false))
                    {
                        currentPageView.getHTMLErrorCode().getInteger();
                    }
                }

                if (maHitParameterPosition[SERVER_NAME] != 0)
                {
                    if ((currentPageView.getServerName() != null) &&
                            (currentPageView.getServerName().isNull() == false))
                    {
                        currentPageView.getServerName().getString();
                    }
                }

                if (maHitParameterPosition[BYTES_SENT] != 0)
                {
                    if ((currentPageView.getBytesSent() != null) && (currentPageView.getBytesSent().isNull() == false))
                    {
                        currentPageView.getBytesSent().getDouble();
                    }
                }

                if (maHitParameterPosition[TIME_TAKEN_TO_SERV_REQUEST] != 0)
                {
                    if ((currentPageView.getTimeTakenToServeRequest() != null) &&
                            (currentPageView.getTimeTakenToServeRequest().isNull() == false))
                    {
                        currentPageView.getTimeTakenToServeRequest().getDouble();
                    }
                }

                if (maHitParameterPosition[CANONICAL_SERVER_PORT] != 0)
                {
                    if ((currentPageView.getCanonicalServerPort() != null) &&
                            (currentPageView.getCanonicalServerPort().isNull() == false))
                    {
                        currentPageView.getCanonicalServerPort().getInteger();
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
                    }
                }

                if (maHitParameterPosition[CLEANSED] != 0)
                {
                }

                if (maHitParameterPosition[CLEANSED_ID] != 0)
                {
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
