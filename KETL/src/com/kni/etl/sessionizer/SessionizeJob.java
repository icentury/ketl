/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.io.File;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.ETLJob;
import com.kni.etl.stringtools.StringMatcher;


public class SessionizeJob extends ETLJob
{
    private char[] comparisonBuffer;
    private char[] buffer;

    // Any single character
    public static final char ANY = '?';

    // Zero or more characters
    public static final char MORE = '*';

    // Relevant under Windows
    public static final String DOS = "*.*";

    /**
     * SessionizeJob constructor comment.
     */

    /* Parameters */
    public static final String SMP = "SMP";
    public static final String DATABASE_PASSWORD = "db_password";
    public static final String BATCH_COMMIT_SIZE = "batch_commit_size";
    public static final String DATABASE_DRIVER_CLASS = "db_driver";
    public static final String DATABASE_URL = "db_url";
    public static final String DATABASE_PREFIX = "db_preifx";
    public static final String DATABASE_USER = "db_user";
    public static final String FILE_DEFINITION_ID = "file_definition_id";
    public static final String METADATA_DRIVER_CLASS = "md_driver";
    public static final String METADATA_PASSWORD = "md_pwd";
    public static final String METADATA_URL = "md_url";
    public static final String METADATA_USER = "md_user";
    public static final String PAGE_DEFINITION_ID = "page_definition_id";
    public static final String SESSION_DEFINITION_ID = "session_definition_id";
    public static final String WEBLOG_SEARCH_STRING = "weblog_srch";
    protected int iUpdateCount = -1;

    /**
    * Insert the method's description here.
    * Creation date: (5/4/2002 4:47:29 PM)
    * @param strSQL java.lang.String
    */
    public SessionizeJob() throws Exception
    {
        super();
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 2:28:24 PM)
     */
    public void cleanup()
    {
        // If we still have a ResultSet open, we should close it...
    }

    protected Node setChildNodes(Node pParentNode)
    {
        Element e = pParentNode.getOwnerDocument().createElement("EMPTY");
        e.appendChild(pParentNode.getOwnerDocument().createTextNode("See configuration tables"));
        pParentNode.appendChild(e);

        return e;
    }

    Node getJobChildNodes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 12:06:44 PM)
     */
    protected void finalize() throws Throwable
    {
        cleanup();

        // It's good practice to call the superclass's finalize() method,
        // even if you know there is not one currently defined...
        super.finalize();
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:13:26 PM)
     * @return int
     */
    public String getBatchCommitSize()
    {
        return (String) getGlobalParameter(SessionizeJob.BATCH_COMMIT_SIZE);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 5:56:56 PM)
     * @return java.lang.String
     */
    public java.lang.String getDatabaseDriverClass()
    {
        return (String) getGlobalParameter(SessionizeJob.DATABASE_DRIVER_CLASS);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:02:58 PM)
     * @return java.lang.String
     */
    public java.lang.String getDatabasePassword()
    {
        return (String) getGlobalParameter(SessionizeJob.DATABASE_PASSWORD);
    }

    public java.lang.String getDatabasePrefix()
    {
        return (String) getGlobalParameter(SessionizeJob.DATABASE_PREFIX);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:00:10 PM)
     * @return java.lang.String
     */
    public java.lang.String getDatabaseURL()
    {
        return (String) getGlobalParameter(SessionizeJob.DATABASE_URL);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:02:34 PM)
     * @return java.lang.String
     */
    public java.lang.String getDatabaseUser()
    {
        return (String) getGlobalParameter(SessionizeJob.DATABASE_USER);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:09:22 PM)
     * @return java.lang.String
     */
    public String getFileDefinitionID()
    {
        return (String) getGlobalParameter(SessionizeJob.FILE_DEFINITION_ID);
    }

    public String getMetadataDriver()
    {
        return (String) getGlobalParameter(SessionizeJob.METADATA_DRIVER_CLASS);
    }

    public String getMetadataPassword()
    {
        return (String) getGlobalParameter(SessionizeJob.METADATA_PASSWORD);
    }

    public String getMetadataURL()
    {
        return (String) getGlobalParameter(SessionizeJob.METADATA_URL);
    }

    public String getMetadataUser()
    {
        return (String) getGlobalParameter(SessionizeJob.METADATA_USER);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:09:42 PM)
     * @return java.lang.String
     */
    public String getPageDefinitionID()
    {
        return (String) getGlobalParameter(SessionizeJob.PAGE_DEFINITION_ID);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:08:59 PM)
     * @return java.lang.String
     */
    public String getSessionDefinitionID()
    {
        return (String) getGlobalParameter(SessionizeJob.SESSION_DEFINITION_ID);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:53:09 PM)
     * @return int
     */
    public int getUpdateCount()
    {
        return iUpdateCount;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:07:13 PM)
     * @return java.lang.String[]
     */
    public String[] getWebLogFilenames(String pSearchPath)
    {
        String[] fileNames = null;

        if (pSearchPath == null)
        {
            return (null);
        }

        int lastPos = pSearchPath.lastIndexOf("/");

        if (lastPos == -1)
        {
            lastPos = pSearchPath.lastIndexOf("\\");
        }

        int fieldCnt = 0;

        if (lastPos > 0)
        {
            String dirStr = pSearchPath.substring(0, lastPos);
            String fileSearch = pSearchPath.substring(lastPos + 1);

            StringMatcher filePattern = null;

            if (fileSearch != null)
            {
                filePattern = new StringMatcher(fileSearch);
            }
            else
            {
                return null;
            }

            File dir = new File(dirStr);

            if (dir.exists() == false)
            {
                this.getStatus().setErrorMessage("Weblog search string doesn not exist" + dirStr);

                return null;
            }

            File[] list = dir.listFiles();

            for (int i = 0; i < list.length; i++)
            {
                if (list[i].isFile())
                {
                    if (filePattern.match(list[i].getName()))
                    {
                        if (fileNames == null)
                        {
                            fileNames = new String[fieldCnt + 1];
                        }
                        else
                        {
                            fieldCnt++;

                            String[] tmp = new String[fieldCnt + 1];
                            System.arraycopy(fileNames, 0, tmp, 0, fileNames.length);
                            fileNames = tmp;
                        }

                        fileNames[fieldCnt] = list[i].getPath();
                    }
                }
            }
        }

        return fileNames;
    }

    public String getWebLogSearchString()
    {
        return (String) getGlobalParameter(SessionizeJob.WEBLOG_SEARCH_STRING);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:02:34 PM)
     * @return java.lang.String
     */
    public boolean runSMP()
    {
        String smp = (String) getGlobalParameter(SessionizeJob.SMP);

        if (smp == null)
        {
            return false;
        }
        else if (smp.compareToIgnoreCase("TRUE") == 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:13:26 PM)
     * @return int
     */
    public void setBatchCommitSize(String pParam)
    {
        setGlobalParameter("batch_commit_size", pParam);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 5:56:56 PM)
     * @param newDriverClass java.lang.String
     */
    public void setDatabaseDriverClass(java.lang.String newDatabaseDriverClass)
    {
        setGlobalParameter("driver", newDatabaseDriverClass);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:02:58 PM)
     * @param newDatabasePassword java.lang.String
     */
    public void setDatabasePassword(java.lang.String newDatabasePassword)
    {
        setGlobalParameter("password", newDatabasePassword);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:00:10 PM)
     * @param newDatabaseURL java.lang.String
     */
    public void setDatabaseURL(java.lang.String newDatabaseURL)
    {
        setGlobalParameter("url", newDatabaseURL);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:02:34 PM)
     * @param newDatabaseUser java.lang.String
     */
    public void setDatabaseUser(java.lang.String newDatabaseUser)
    {
        setGlobalParameter("user", newDatabaseUser);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:09:22 PM)
     * @return java.lang.String
     */
    public void setFileDefinitionID(String pParam)
    {
        setGlobalParameter("file_definition_id", pParam);
    }

    public void setMetadataDriver(String newMD)
    {
        setGlobalParameter("md_driver", newMD);
    }

    public void setMetadataPassword(String newMD)
    {
        setGlobalParameter("md_pwd", newMD);
    }

    public void setMetadataURL(String newMD)
    {
        setGlobalParameter("md_url", newMD);
    }

    public void setMetadataUser(String newMD)
    {
        setGlobalParameter("md_user", newMD);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:09:42 PM)
     * @return java.lang.String
     */
    public void setPageDefinitionID(String pParam)
    {
        setGlobalParameter("page_definition_id", pParam);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/13/2002 6:08:59 PM)
     * @return java.lang.String
     */
    public void setSessionDefinitionID(String pParam)
    {
        setGlobalParameter("session_definition_id", pParam);
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:53:09 PM)
     * @param newUpdateCount int
     */
    public void setUpdateCount(int newUpdateCount)
    {
        iUpdateCount = newUpdateCount;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/4/2002 6:00:10 PM)
     * @param newDatabaseURL java.lang.String
     */
    public void setWebLogSearchString(java.lang.String newParam)
    {
        setGlobalParameter("weblog_srch", newParam);
    }
}
