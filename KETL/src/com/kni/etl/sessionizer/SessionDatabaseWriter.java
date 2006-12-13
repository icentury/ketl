/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl_v1.ResultRecord;


public class SessionDatabaseWriter extends JDBCSessionizationWriterRoot
{
    StringBuffer sBuf = null;
    static final String IP_ADDRESS_NOT_TRANSLATED = null;
    static final String IP_ADDRESS_TRANSLATED = "T";
    static final String IP_ADDRESS_PARTIALLY_TRANSLATED = "P";
    static final String IP_ADDRESS_NOT_FOUND = "N";
    String sDNSTranslationLevel = IP_ADDRESS_NOT_TRANSLATED;

    void writeRecord(ResultRecord resultRecord) throws SQLException, Exception
    {
        if ((resultRecord != null) && (resultRecord.Type == Session.SESSION))
        {
            Session sessionToStore = (Session) resultRecord;

            // put substr into oracle statement as extra protection
            if (stmt == null)
            {
                this.resolveSessionColumnMaps();
                stmt = ((Connection) dbConnection).prepareStatement(buildStatement());

                // supply batch size and the number of timestamp in the statement
                buildTimestampCache();
            }

            if (maHitParameterPosition[LOAD_ID] != 0)
            {
                stmt.setInt(maHitParameterPosition[LOAD_ID], (LoadID));
            }

            if (maHitParameterPosition[DM_LOAD_ID] != 0)
            {
                stmt.setInt(maHitParameterPosition[DM_LOAD_ID], (DMLoadID));
            }

            if (maHitParameterPosition[TEMP_SESSION_ID] != 0)
            {
                stmt.setLong(maHitParameterPosition[TEMP_SESSION_ID], sessionToStore.getID());
            }

            if (maHitParameterPosition[HITS] != 0)
            {
                stmt.setLong(maHitParameterPosition[HITS], sessionToStore.Hit);
            }

            if (maHitParameterPosition[PAGEVIEWS] != 0)
            {
                stmt.setLong(maHitParameterPosition[PAGEVIEWS], sessionToStore.PageViews);
            }

            if (maHitParameterPosition[MAIN_SESSION_IDENTIFIER] != 0)
            {
                if (sessionToStore.MainSessionIdentifier != null)
                {
                    stmt.setString(maHitParameterPosition[MAIN_SESSION_IDENTIFIER],
                        fieldTrim(sessionToStore.MainSessionIdentifier, this.maHitParameterSize[MAIN_SESSION_IDENTIFIER]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[MAIN_SESSION_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[CUSTOMFIELD1] != 0)
            {
                if (sessionToStore.customField1 != null)
                {
                    stmt.setString(maHitParameterPosition[CUSTOMFIELD1],
                        fieldTrim(sessionToStore.customField1, this.maHitParameterSize[CUSTOMFIELD1]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[CUSTOMFIELD1], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[CUSTOMFIELD2] != 0)
            {
                if (sessionToStore.customField2 != null)
                {
                    stmt.setString(maHitParameterPosition[CUSTOMFIELD2],
                        fieldTrim(sessionToStore.customField2, this.maHitParameterSize[CUSTOMFIELD2]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[CUSTOMFIELD2], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[CUSTOMFIELD3] != 0)
            {
                if (sessionToStore.customField3 != null)
                {
                    stmt.setString(maHitParameterPosition[CUSTOMFIELD3],
                        fieldTrim(sessionToStore.customField3, this.maHitParameterSize[CUSTOMFIELD3]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[CUSTOMFIELD3], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER] != 0)
            {
                if (sessionToStore.FirstClickSessionIdentifier != null)
                {
                    stmt.setString(maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER],
                        fieldTrim(sessionToStore.FirstClickSessionIdentifier,
                            this.maHitParameterSize[FIRST_CLICK_SESSION_IDENTIFIER]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[FIRST_CLICK_SESSION_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[PERSISTANT_IDENTIFIER] != 0)
            {
                if (sessionToStore.PersistantIdentifier != null)
                {
                    stmt.setString(maHitParameterPosition[PERSISTANT_IDENTIFIER],
                        fieldTrim(sessionToStore.PersistantIdentifier, this.maHitParameterSize[PERSISTANT_IDENTIFIER]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[PERSISTANT_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[START_PERSISTANT_IDENTIFIER] != 0)
            {
                if (sessionToStore.StartPersistantIdentifier != null)
                {
                    stmt.setString(maHitParameterPosition[START_PERSISTANT_IDENTIFIER],
                        fieldTrim(sessionToStore.StartPersistantIdentifier,
                            this.maHitParameterSize[START_PERSISTANT_IDENTIFIER]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[START_PERSISTANT_IDENTIFIER], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[FIRST_SESSION_ACTIVITY] != 0)
            {
                if (sessionToStore.FirstActivity != null)
                {
                    Timestamp tms = this.getCachedTimeStamp(insertCnt);
                    tms.setTime(sessionToStore.FirstActivity.getTime());
                    stmt.setTimestamp(maHitParameterPosition[FIRST_SESSION_ACTIVITY], tms);
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[FIRST_SESSION_ACTIVITY], java.sql.Types.TIMESTAMP);
                }
            }

            if (maHitParameterPosition[LAST_SESSION_ACTIVITY] != 0)
            {
                if (sessionToStore.LastActivity != null)
                {
                    Timestamp tms = this.getCachedTimeStamp(insertCnt);
                    tms.setTime(sessionToStore.LastActivity.getTime());
                    stmt.setTimestamp(maHitParameterPosition[LAST_SESSION_ACTIVITY], tms);
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[LAST_SESSION_ACTIVITY], java.sql.Types.TIMESTAMP);
                }
            }

            if (maHitParameterPosition[IP_ADDRESS] != 0)
            {
                if (sessionToStore.IPAddress != null)
                {
                    stmt.setString(maHitParameterPosition[IP_ADDRESS],
                        fieldTrim(sessionToStore.IPAddress, this.maHitParameterSize[IP_ADDRESS]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[IP_ADDRESS], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[SOURCE_FILE] != 0)
            {
                if (sessionToStore.SourceFile != null)
                {
                    stmt.setString(maHitParameterPosition[SOURCE_FILE],
                        fieldTrim(sessionToStore.SourceFile, this.maHitParameterSize[SOURCE_FILE]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[SOURCE_FILE], java.sql.Types.VARCHAR);
                }
            }

            // reset to not translated
            sDNSTranslationLevel = IP_ADDRESS_NOT_TRANSLATED;

            if (maHitParameterPosition[DNS_ADDRESS] != 0)
            {
                if (sessionToStore.IPAddress != null)
                {
                    InetAddress addr;

                    try
                    {
                        addr = InetAddress.getByName(sessionToStore.IPAddress);

                        sDNSTranslationLevel = IP_ADDRESS_TRANSLATED;

                        String sAddr = addr.getHostName();

                        stmt.setString(maHitParameterPosition[DNS_ADDRESS],
                            fieldTrim(sAddr, this.maHitParameterSize[DNS_ADDRESS]));
                    }
                    catch (UnknownHostException e)
                    {
                        sDNSTranslationLevel = IP_ADDRESS_NOT_FOUND;
                        stmt.setNull(maHitParameterPosition[DNS_ADDRESS], java.sql.Types.VARCHAR);
                    }
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[DNS_ADDRESS], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL] != 0)
            {
                if (sessionToStore.IPAddress != null)
                {
                    stmt.setString(maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL],
                        fieldTrim(sDNSTranslationLevel, this.maHitParameterSize[DNS_ADDRESS_TRANSLATION_LEVEL]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[DNS_ADDRESS_TRANSLATION_LEVEL], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[BROWSER] != 0)
            {
                if (sessionToStore.Browser != null)
                {
                    stmt.setString(maHitParameterPosition[BROWSER],
                        fieldTrim(sessionToStore.Browser, this.maHitParameterSize[BROWSER]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[BROWSER], java.sql.Types.VARCHAR);
                }
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

                    stmt.setString(maHitParameterPosition[KEEP_VARIABLES],
                        fieldTrim(sBuf.toString(), this.maHitParameterSize[KEEP_VARIABLES]));
                }
                else
                {
                    stmt.setNull(maHitParameterPosition[KEEP_VARIABLES], java.sql.Types.VARCHAR);
                }
            }

            if (maHitParameterPosition[REPEAT_VISITOR] != 0)
            {
                //			missing referrer
                if (sessionToStore.isRepeatVisitor() == true)
                {
                    stmt.setInt(maHitParameterPosition[REPEAT_VISITOR], (1));
                }
                else
                {
                    stmt.setInt(maHitParameterPosition[REPEAT_VISITOR], (0));
                }
            }

            if (maHitParameterPosition[REFERRER] != 0)
            {
                if (sessionToStore.Referrer == null)
                {
                    stmt.setNull(maHitParameterPosition[REFERRER], java.sql.Types.VARCHAR);
                }
                else
                {
                    stmt.setString(maHitParameterPosition[REFERRER],
                        fieldTrim(sessionToStore.Referrer, maHitParameterSize[REFERRER]));
                }
            }

            // date and time
            if (dbConnection == null)
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
