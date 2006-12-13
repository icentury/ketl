/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.sql.SQLException;
import java.sql.Timestamp;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;


public class SessionTestWriter extends TestSessionizationWriterRoot
{
    /**
     * @param esJobStatus
     */
    StringBuffer sBuf = null;
    static final String IP_ADDRESS_NOT_TRANSLATED = null;
    static final String IP_ADDRESS_TRANSLATED = "T";
    static final String IP_ADDRESS_PARTIALLY_TRANSLATED = "P";
    static final String IP_ADDRESS_NOT_FOUND = "N";
    String sDNSTranslationLevel = IP_ADDRESS_NOT_TRANSLATED;
    Timestamp tms = new Timestamp(0);

    void writeRecord(ResultRecord resultRecord) throws SQLException, Exception
    {
        if ((resultRecord != null) && (resultRecord.Type == Session.SESSION))
        {
            Session sessionToStore = (Session) resultRecord;

            // put substr into oracle statement as extra protection
            if (stmt == null)
            {
                this.buildStatement();

                // supply batch size and the number of timestamp in the statement
                buildTimestampCache();
            }

            if (maHitParameterPosition[LOAD_ID] != 0)
            {
            }

            if (maHitParameterPosition[DM_LOAD_ID] != 0)
            {
            }

            if (maHitParameterPosition[TEMP_SESSION_ID] != 0)
            {
                sessionToStore.getID();
            }

            if (maHitParameterPosition[HITS] != 0)
            {
            }

            if (maHitParameterPosition[PAGEVIEWS] != 0)
            {
            }

            if (maHitParameterPosition[MAIN_SESSION_IDENTIFIER] != 0)
            {
            }

            if (maHitParameterPosition[CUSTOMFIELD1] != 0)
            {
            }

            if (maHitParameterPosition[CUSTOMFIELD2] != 0)
            {
            }

            if (maHitParameterPosition[CUSTOMFIELD3] != 0)
            {
            }

            if (maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER] != 0)
            {
            }

            if (maHitParameterPosition[PERSISTANT_IDENTIFIER] != 0)
            {
            }

            if (maHitParameterPosition[START_PERSISTANT_IDENTIFIER] != 0)
            {
            }

            if (maHitParameterPosition[FIRST_SESSION_ACTIVITY] != 0)
            {
                if (sessionToStore.FirstActivity != null)
                {
                    tms.setTime(sessionToStore.FirstActivity.getTime());
                }
            }

            if (maHitParameterPosition[LAST_SESSION_ACTIVITY] != 0)
            {
                if (sessionToStore.LastActivity != null)
                {
                    tms.setTime(sessionToStore.LastActivity.getTime());
                }
            }

            if (maHitParameterPosition[IP_ADDRESS] != 0)
            {
            }

            if (maHitParameterPosition[SOURCE_FILE] != 0)
            {
            }

            // reset to not translated
            sDNSTranslationLevel = IP_ADDRESS_NOT_TRANSLATED;

            if (maHitParameterPosition[DNS_ADDRESS] != 0)
            {
                if (sessionToStore.IPAddress != null)
                {
                    //InetAddress addr;
                    /*try
                    {
                        addr = InetAddress.getByName(sessionToStore.IPAddress);

                        sDNSTranslationLevel = IP_ADDRESS_TRANSLATED;

                        String sAddr = addr.getHostName();


                    }
                    catch (UnknownHostException e)*/
                    {
                        sDNSTranslationLevel = IP_ADDRESS_NOT_FOUND;
                    }
                }
            }

            if (maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL] != 0)
            {
            }

            if (maHitParameterPosition[BROWSER] != 0)
            {
            }

            if (maHitParameterPosition[KEEP_VARIABLES] != 0)
            {
                if ((sessionToStore.CookieKeepVariables != null) && (sessionToStore.CookieKeepVariables.length > 0))
                {
                    if (sBuf == null)
                    {
                        sBuf = new StringBuffer(EngineConstants.MAX_KEEP_VARIABLE_LENGTH);
                    }
                    else
                    {
                        sBuf.delete(0, EngineConstants.MAX_KEEP_VARIABLE_LENGTH - 1);
                    }

                    for (int i = sessionToStore.CookieKeepVariables.length - 1; i >= 0; i--)
                    {
                        sBuf.append(sessionToStore.CookieKeepVariables[i][0]).append('=')
                            .append(sessionToStore.CookieKeepVariables[i][1]).append(';');
                    }
                }
            }

            if (maHitParameterPosition[REPEAT_VISITOR] != 0)
            {
            }

            if (maHitParameterPosition[REFERRER] != 0)
            {
            }

            // date and time
            if (stmt == null)
            {
                ResourcePool.LogMessage(this, "Connection refreshing, insert:" + insertCnt);
                refreshWriterConnection();
            }

            // execute statement
            addBatch();
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.sessionizer.SessionizationWriterRoot#numberOfTimestampsPerStatement()
     */
    int numberOfTimestampsPerStatement()
    {
        return 3;
    }
}
