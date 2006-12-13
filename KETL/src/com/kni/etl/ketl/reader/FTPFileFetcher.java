/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Mar 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.reader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;
import com.kni.util.net.ftp.DefaultFTPFileListParser;
import com.kni.util.net.ftp.FTP;
import com.kni.util.net.ftp.FTPClient;
import com.kni.util.net.ftp.FTPConnectionClosedException;
import com.kni.util.net.ftp.FTPFile;
import com.kni.util.net.ftp.FTPReply;

/**
 * @author nwakefield Creation Date: Jun 12, 2003
 */
/**
 * @author nwakefield Creation Date: Mar 17, 2003
 */
public class FTPFileFetcher extends ETLReader implements DefaultReaderCore {

    public FTPFileFetcher(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    class FTPWorkerThread extends Thread {

        public static final String ASCII = "ASCII";
        public static final String FALSE = "FALSE";
        protected boolean bShutdown = false;
        protected LinkedList llPendingQueue = null;
        protected int iSleepPeriod = 100;
        protected boolean bFileDownloaded = false;
        public String fileName;
        public String user;
        public String server;
        private String password;
        public String transferType;
        public long fileSize;
        public long downloadTime;
        public String destFileName;
        String passiveMode;
        Long lastModifiedDate;

        /**
         * ETLJobExecutorThread constructor comment.
         */
        public FTPWorkerThread() {
            super();
        }

        /**
         * ETLJobExecutorThread constructor comment.
         * 
         * @param target java.lang.Runnable
         */
        public FTPWorkerThread(Runnable target) {
            super(target);
        }

        /**
         * ETLJobExecutorThread constructor comment.
         * 
         * @param target java.lang.Runnable
         * @param name java.lang.String
         */
        public FTPWorkerThread(Runnable target, String name) {
            super(target, name);
        }

        /**
         * ETLJobExecutorThread constructor comment.
         * 
         * @param name java.lang.String
         */
        public FTPWorkerThread(String name) {
            super(name);
        }

        /**
         * ETLJobExecutorThread constructor comment.
         * 
         * @param group java.lang.ThreadGroup
         * @param target java.lang.Runnable
         */
        public FTPWorkerThread(ThreadGroup group, Runnable target) {
            super(group, target);
        }

        /**
         * ETLJobExecutorThread constructor comment.
         * 
         * @param group java.lang.ThreadGroup
         * @param target java.lang.Runnable
         * @param name java.lang.String
         */
        public FTPWorkerThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        /**
         * ETLJobExecutorThread constructor comment.
         * 
         * @param group java.lang.ThreadGroup
         * @param name java.lang.String
         */
        public FTPWorkerThread(ThreadGroup group, String name) {
            super(group, name);
        }

        /**
         * Insert the method's description here. Creation date: (5/3/2002 6:49:24 PM)
         * 
         * @return boolean
         * @param param com.kni.etl.ETLJob
         */
        public boolean getFile(String strUser, String strPassword, String strServer, String strTransferType,
                String strFileName, String strDestFileName, String strPassiveMode, Long lExpectedSize,
                Long lModifiedDate) {
            this.server = strServer;
            this.user = strUser;
            this.password = strPassword;
            this.fileName = strFileName;
            this.transferType = strTransferType;
            this.fileSize = lExpectedSize.longValue();
            this.passiveMode = strPassiveMode;

            char pathSeperator = '\\';
            this.lastModifiedDate = lModifiedDate;

            String destFileName = strFileName;

            if (strFileName != null) {
                int endOfPath = strFileName.lastIndexOf(pathSeperator);

                if (endOfPath == -1) {
                    pathSeperator = '/';
                    endOfPath = strFileName.lastIndexOf(pathSeperator);
                }

                if (endOfPath != -1) {
                    destFileName = strFileName.substring(endOfPath + 1);
                }
            }

            pathSeperator = '\\';

            if (strDestFileName != null) {
                String pathName = null;
                int endOfPath = strDestFileName.lastIndexOf(pathSeperator);

                if (endOfPath == -1) {
                    pathSeperator = '/';
                    endOfPath = strDestFileName.lastIndexOf(pathSeperator);
                }

                if (endOfPath != -1) {
                    pathName = strDestFileName.substring(0, endOfPath);
                }

                if (pathName != null) {
                    if (endOfPath == (pathName.length() - 1)) {
                        this.destFileName = pathName + destFileName;
                    }
                    else {
                        this.destFileName = pathName + pathSeperator + destFileName;
                    }
                }
                else {
                    this.destFileName = destFileName;
                }
            }
            else {
                this.destFileName = strFileName;
            }

            return true;
        }

        /**
         * Insert the method's description here. Creation date: (5/7/2002 11:23:02 AM)
         * 
         * @return com.kni.etl.ETLJobExecutorStatus
         */
        public boolean fileDownloaded() {
            return bFileDownloaded;
        }

        /**
         * Insert the method's description here. Creation date: (5/7/2002 11:52:15 AM)
         */
        protected boolean initialize() {
            return true;
        }

        /**
         * Loops on the job queue, taking each job and running with it. Creation date: (5/3/2002 5:43:04 PM)
         */
        public void run() {
            boolean bSuccess;

            // Run any initialization code that the subclasses will need...
            if (initialize() == false) {
                bFileDownloaded = false;

                return;
            }

            bSuccess = downloadFile();

            if (bSuccess) {
                bFileDownloaded = true;
            }

            terminate();
        }

        private boolean downloadFile() {
            FTPClient ftp = new FTPClient();

            try {
                int reply;
                ftp.connect(this.server);
                ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Connected to " + this.server);

                // After connection attempt, you should check the reply code to verify
                // success.
                reply = ftp.getReplyCode();

                if (!FTPReply.isPositiveCompletion(reply)) {
                    ftp.disconnect();
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP server refused connection.");

                    return false;
                }
            } catch (IOException e) {
                if (ftp.isConnected()) {
                    try {
                        ftp.disconnect();
                    } catch (IOException f) {
                        return false;
                    }
                }

                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP Could not connect to server.");
                ResourcePool.LogException(e, this);

                return false;
            }

            try {
                if (!ftp.login(this.user, this.password)) {
                    ftp.logout();
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP login failed.");

                    return false;
                }

                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Remote system is " + ftp.getSystemName());

                if ((transferType != null) && (transferType.compareTo(ASCII) == 0)) {
                    ftp.setFileType(FTP.ASCII_FILE_TYPE);
                }
                else {
                    ftp.setFileType(FTP.BINARY_FILE_TYPE);
                }

                // Use passive mode as default because most of us are
                // behind firewalls these days.
                if ((passiveMode != null) && passiveMode.equalsIgnoreCase(FALSE)) {
                    ftp.enterLocalActiveMode();
                }
                else {
                    ftp.enterLocalPassiveMode();
                }
            } catch (FTPConnectionClosedException e) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Server closed connection.");
                ResourcePool.LogException(e, this);

                return false;
            } catch (IOException e) {
                ResourcePool.LogException(e, this);

                return false;
            }

            OutputStream output;

            try {
                java.util.Date startDate = new java.util.Date();

                output = new FileOutputStream(this.destFileName);
                ftp.retrieveFile(this.fileName, output);

                File f = new File(this.destFileName);

                // set date of file to match source
                if (f.exists() && (lastModifiedDate != null)) {
                    f.setLastModified(lastModifiedDate.longValue());
                }

                java.util.Date endDate = new java.util.Date();

                downloadTime = endDate.getTime() - startDate.getTime();

                double iDownLoadTime = ((downloadTime + 1) / 1000) + 1;

                ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Download Complete, Rate = " + (this.fileSize / (iDownLoadTime * 1024))
                        + " Kb/s, Seconds = " + iDownLoadTime);

                downloadTime = (downloadTime + 1) / 1000;

                if (ftp.isConnected()) {
                    ftp.disconnect();
                }
            } catch (FTPConnectionClosedException e) {
                ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, e.getMessage());
                ResourcePool.LogException(e, this);

                return false;
            } catch (IOException e) {
                ResourcePool.LogException(e, this);

                return false;
            }

            return true;
        }

        /**
         * This is the publicly accessible function to set the "shutdown" flag for the thread. It will no longer process
         * any new jobs, but finish what it's working on. It will then call the internal terminate() function to close
         * down anything it needs to. BRIAN: should we make this final? Creation date: (5/3/2002 6:50:09 PM)
         */
        public void shutdown() {
            bShutdown = true;
        }

        protected boolean terminate() {
            return true;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        public String toString() {
            return "FTPWorkerThread: Server->" + this.server + ", File->" + this.fileName;
        }
    }

    public static final String ASCII = "ASCII";
    public static final String BINARY = "BINARY";
    public static final String DESTINATIONFILE = "DESTINATIONFILE";
    public static final String DESTINATIONPATH = "DESTINATIONPATH";
    public static final String DOWNLOAD_TIME = "DOWNLOADTIME";
    public static final String FALSE = "FALSE";
    public static final String FILENAME = "FILENAME";
    public static final String FILESIZE = "FILESIZE";
    public static final String REQUIREDFILECOUNT = "REQUIREDFILECOUNT";
    public static final String MAX_PARALLEL_CONNECTIONS = "MAXPARALLELCONNECTIONS";
    public static final String PASSIVEMODE = "PASSIVEMODE";
    public static final String PASSWORD = "PASSWORD";
    public static final String PATH = "PATH";
    public static final String SERVER = "SERVER";
    public static final String SOURCEPATH = "SOURCEPATH";
    public static final String TRANSFER_TYPE = "TRANSFER_TYPE";
    public static final String SYNC = "SYNC";
    public static final String USER = "USER";
    static int FILESIZE_POS = 1;
    static int FILENAME_POS = 0;
    static int IGNOREFILE_POS = 2;
    static int FILEDATE_POS = 3;
    static int PASSWORD_POS = 8;
    static int PASSIVEMODE_POS = 7;
    static int SERVER_POS = 9;
    static int TRANSFER_TYPE_POS = 4;
    static int DOWNLOAD_ELEMENTS = 10;
    static int DEFAULT_MAX_PARALLEL_CONNECTIONS = 20;
    static int DESTINATION_POS = 6;
    static int USER_POS = 5;
    String[] msRequiredTags = { USER, PASSWORD, SERVER, SOURCEPATH, DESTINATIONPATH };
    int connectionCnt = 1;
    int fileCnt = 0;
    boolean filesFound = false;
    ArrayList filesToDownload = null;
    boolean synchronizeFiles = true;

    // private Object[] ftpClients = null;

    /**
     * Insert the method's description here. Creation date: (3/26/2002 1:48:12 PM)
     * 
     * @return int
     * @param LineFields java.lang.Object[]
     */

    // ArrayList tmpFTPClients = new ArrayList();
    ArrayList ftpThreadPool = null;
    int iMaxParallelConnections;

    int getFilenamesForEachCompleteParameterList(int iParamList) {
        String strPath;
        String strUser;
        String strPassword;
        String strServer;
        String strTransferType;
        String strDestinationPath;
        String strPassiveMode;
        String strRequiredFileCount;

        // Pull the parameters from the list...
        strPath = this.getParameterValue(iParamList, SOURCEPATH);
        strUser = this.getParameterValue(iParamList, USER);
        strPassword = this.getParameterValue(iParamList, PASSWORD);
        strServer = this.getParameterValue(iParamList, SERVER);
        strTransferType = this.getParameterValue(iParamList, TRANSFER_TYPE);
        strDestinationPath = this.getParameterValue(iParamList, DESTINATIONPATH);
        strPassiveMode = this.getParameterValue(iParamList, PASSIVEMODE);
        strRequiredFileCount = this.getParameterValue(iParamList, REQUIREDFILECOUNT);

        boolean binaryTransfer = true;

        if ((strTransferType != null) && (strTransferType.compareTo(ASCII) == 0)) {
            binaryTransfer = false;
        }

        boolean passiveMode = true;

        if ((strPassiveMode != null) && (strPassiveMode.equalsIgnoreCase(FALSE))) {
            passiveMode = false;
        }

        int iRequiredFileCount = -1;

        if (strRequiredFileCount != null) {
            try {
                iRequiredFileCount = Integer.parseInt(strRequiredFileCount);
            } catch (NumberFormatException e) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                        "Required file count parameters is an invalid number \"" + strRequiredFileCount + "\"");

                return -1;
            }
        }

        // See if we can match the name of the datasource...
        if ((strUser != null) && (strPassword != null) && (strServer != null) && (strPath != null)
                && (strDestinationPath != null)) {
            Object[] files = null;
            files = getFilenamesFromFTPServer(strUser, strPassword, strServer, strDestinationPath, binaryTransfer,
                    strPath, passiveMode);

            if ((files == null) || (files.length == 0)) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "No files found on server " + strServer
                        + " for search string " + strPath);

                return 0;
            }
            else if ((files != null) && (iRequiredFileCount != -1) && (iRequiredFileCount != files.length)) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                        "Number of files does not match required file count of " + iRequiredFileCount
                                + ", actual files found " + files.length);

                return -1;
            }

            if (filesToDownload == null) {
                filesToDownload = new ArrayList();
            }

            // record each file to download, list will be sorted later for uniqueness
            for (int i = 0; i < files.length; i++) {
                Object[] tmp = new Object[DOWNLOAD_ELEMENTS];

                if (((Object[]) files[i])[IGNOREFILE_POS] == null) {
                    tmp[FILENAME_POS] = ((Object[]) files[i])[FILENAME_POS];
                    tmp[FILESIZE_POS] = ((Object[]) files[i])[FILESIZE_POS];
                    tmp[FILEDATE_POS] = ((Object[]) files[i])[FILEDATE_POS];
                    tmp[USER_POS] = strUser;
                    tmp[SERVER_POS] = strServer;
                    tmp[PASSWORD_POS] = strPassword;
                    tmp[TRANSFER_TYPE_POS] = strTransferType;
                    tmp[DESTINATION_POS] = strDestinationPath;
                    tmp[PASSIVEMODE_POS] = strPassiveMode;

                    filesToDownload.add(tmp);
                }
                else {
                    ResourcePool.LogMessage("Ignoring file " + ((Object[]) files[i])[FILENAME_POS]
                            + " as it matches destination");
                }
            }

            return files.length;
        }

        return 0;
    }

    public Object[] getFilenamesFromFTPServer(String strUser, String strPassword, String strServer,
            String strDestinationPath, boolean bBinaryTransfer, String searchString, boolean bPassiveMode) {
        Object[] result = null;
        FTPClient ftp = getFTPConnection(strUser, strPassword, strServer, bBinaryTransfer,
                "Directory listing connection.", bPassiveMode);

        if (ftp == null) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP: Could not connect to server.");

            return null;
        }

        FTPFile[] fList;

        try {
            fList = ftp.listFiles(new DefaultFTPFileListParser(), searchString);

            char pathSeperator = '\\';
            String pathName = null;
            int endOfPath = searchString.lastIndexOf(pathSeperator);

            if (endOfPath == -1) {
                pathSeperator = '/';
                endOfPath = searchString.lastIndexOf(pathSeperator);
            }

            if (endOfPath != -1) {
                pathName = searchString.substring(0, endOfPath);
            }

            if (fList != null) {
                ArrayList tmpFiles = new ArrayList();

                for (int i = 0; i < fList.length; i++) {
                    Object[] tmp = new Object[4];
                    String fileName;

                    if (pathName != null) {
                        pathSeperator = '\\';
                        endOfPath = fList[i].getName().lastIndexOf(pathSeperator);

                        String fn = fList[i].getName();

                        if (endOfPath == -1) {
                            pathSeperator = '/';
                            endOfPath = fn.lastIndexOf(pathSeperator);
                        }

                        if (endOfPath != -1) {
                            tmp[FILENAME_POS] = pathName + pathSeperator + fList[i].getName().substring(endOfPath + 1);
                            fileName = fList[i].getName().substring(endOfPath + 1);
                        }
                        else {
                            tmp[FILENAME_POS] = pathName + pathSeperator + fList[i].getName();
                            fileName = fList[i].getName();
                        }
                    }
                    else {
                        tmp[FILENAME_POS] = fList[i].getName();
                        fileName = fList[i].getName();
                    }

                    tmp[FILESIZE_POS] = new Long(fList[i].getSize());
                    tmp[FILEDATE_POS] = new Long(fList[i].getTimestamp().getTimeInMillis());

                    // check to see if file exists, if does create an object to mark true
                    try {
                        long mCreationDate = fList[i].getTimestamp().getTimeInMillis();

                        if (strDestinationPath != null) {
                            fileName = strDestinationPath + fileName;
                        }

                        File f = new File(fileName);

                        if (f.exists() && (f.lastModified() == mCreationDate) && (f.length() == fList[i].getSize())) {
                            tmp[IGNOREFILE_POS] = "";
                        }
                    } catch (Exception e) {
                        if (this.synchronizeFiles) {
                            ResourcePool.LogMessage("Warning: FTP server does not support file synchronization.");
                        }
                    }

                    tmpFiles.add(tmp);
                }

                if (tmpFiles.size() > 0) {
                    result = new Object[tmpFiles.size()];
                    tmpFiles.toArray(result);
                }
            }
        } catch (IOException e1) {
            ResourcePool.LogException(e1, this);
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, searchString
                    + " caused IO Exception, file will be ignored");
        }

        if (ftp.isConnected()) {
            try {
                ftp.disconnect();
            } catch (IOException f) {
                // do nothing
            }
        }

        return result;
    }

    private FTPClient getFTPConnection(String strUser, String strPassword, String strServer, boolean binaryTransfer,
            String connectionNote, boolean passiveMode) {
        FTPClient ftp = new FTPClient();

        try {
            int reply;
            ftp.connect(strServer);
            ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Connected to " + strServer + ", " + connectionNote);

            // After connection attempt, you should check the reply code to verify
            // success.
            reply = ftp.getReplyCode();

            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP server refused connection.");

                return null;
            }
        } catch (IOException e) {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException f) {
                    return null;
                }
            }

            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP Could not connect to server.");
            ResourcePool.LogException(e, this);

            return null;
        }

        try {
            if (!ftp.login(strUser, strPassword)) {
                ftp.logout();
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP login failed.");

                return null;
            }

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Remote system is " + ftp.getSystemName() + ", " + connectionNote);

            if (binaryTransfer) {
                ftp.setFileType(FTP.BINARY_FILE_TYPE);
            }
            else {
                ftp.setFileType(FTP.ASCII_FILE_TYPE);
            }

            // Use passive mode as default because most of us are
            // behind firewalls these days.
            if (passiveMode) {
                ftp.enterLocalPassiveMode();
            }
            else {
                ftp.enterLocalActiveMode();
            }
        } catch (FTPConnectionClosedException e) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Server closed connection.");
            ResourcePool.LogException(e, this);

            return null;
        } catch (IOException e) {
            ResourcePool.LogException(e, this);

            return null;
        }

        return ftp;
    }

    public int initialize(Node xmlSourceNode) throws KETLThreadException {
        int res = super.initialize(xmlSourceNode);
        if (res != 0) {
            return res;
        }
        NamedNodeMap nmAttrs = xmlSourceNode.getAttributes();

        iMaxParallelConnections = XMLHelper.getAttributeAsInt(nmAttrs, MAX_PARALLEL_CONNECTIONS, 0);

        if (this.iMaxParallelConnections == 0) {
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                    "Defaulting to max parallel ftp connections of " + this.iMaxParallelConnections);

            this.iMaxParallelConnections = DEFAULT_MAX_PARALLEL_CONNECTIONS;
        }

        synchronizeFiles = XMLHelper.getAttributeAsBoolean(nmAttrs, SYNC, true);

        int filesToGet = 0;

        if (this.maParameters != null) {
            for (int i = 0; i < this.maParameters.size(); i++) {
                filesToGet = filesToGet + this.getFilenamesForEachCompleteParameterList(i);
            }
        }

        if (filesToGet == 0) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                    "No files found on any server, check parameter lists");

            return 3;
        }
        else if (filesToGet == -1) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Failing job see previous error");

            return 3;
        }

        // take file list array and make it distinct
        ArrayList tmpArrayList = new ArrayList();

        for (int i = 0; i < this.filesToDownload.size(); i++) {
            Object[] fileDetails = (Object[]) this.filesToDownload.get(i);

            boolean alreadyListed = false;

            for (int p = 0; p < tmpArrayList.size(); p++) {
                Object[] cmpFileDetails = (Object[]) tmpArrayList.get(p);

                // compare all details except size, if files matched then we have duplicate
                // and we should not download it twice
                if ((((String) cmpFileDetails[SERVER_POS]).compareTo((String) fileDetails[SERVER_POS]) == 0)
                        && (((String) cmpFileDetails[USER_POS]).compareTo((String) fileDetails[USER_POS]) == 0)
                        && (((String) cmpFileDetails[PASSWORD_POS]).compareTo((String) fileDetails[PASSWORD_POS]) == 0)
                        && (((String) cmpFileDetails[FILENAME_POS]).compareTo((String) fileDetails[FILENAME_POS]) == 0)
                        && (((String) cmpFileDetails[DESTINATION_POS]).compareTo((String) fileDetails[DESTINATION_POS]) == 0)
                        && (((String) cmpFileDetails[SERVER_POS]).compareTo((String) fileDetails[SERVER_POS]) == 0)) {
                    ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                            "Parameter list results in duplicate file search,"
                                    + " duplicate file will be downloaded once only "
                                    + (String) fileDetails[FILENAME_POS]);
                    alreadyListed = true;
                }
            }

            if (alreadyListed == false) {
                tmpArrayList.add(fileDetails);
            }
        }

        // switch to distinct list
        filesToDownload = tmpArrayList;

        if (filesToDownload != null) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Total files to be downloaded = " + filesToDownload.size());
        }

        return 0;
    }

    public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException {
        // spin off threads with each download up to max parallel threads
        // while threads busy wait
        // if a thread has finished remove it from checklist
        // add next file to download thread pool
        // return download details.
        // if no files left return null

        if (ftpThreadPool == null) {
            ftpThreadPool = new ArrayList();
        }

        FTPWorkerThread ftpWorkerThread = null;

        // fill up any missing threads
        while (((this.filesToDownload != null) && (this.filesToDownload.size() != 0))
                && (ftpThreadPool.size() <= this.iMaxParallelConnections)) {
            FTPWorkerThread newFTPWorkerThread = new FTPWorkerThread();

            // get file to download
            Object[] filesToDownload = (Object[]) this.filesToDownload.remove(0);

            // set file to download
            newFTPWorkerThread.getFile((String) filesToDownload[USER_POS], (String) filesToDownload[PASSWORD_POS],
                    (String) filesToDownload[SERVER_POS], (String) filesToDownload[TRANSFER_TYPE_POS],
                    (String) filesToDownload[FILENAME_POS], (String) filesToDownload[DESTINATION_POS],
                    (String) filesToDownload[PASSIVEMODE_POS], (Long) filesToDownload[FILESIZE_POS],
                    (Long) filesToDownload[FILEDATE_POS]);

            // add thread to threadpool
            this.ftpThreadPool.add(newFTPWorkerThread);

            // start thread
            newFTPWorkerThread.start();
        }

        int finishedThread = -1;

        // get the first finished thread
        while ((this.ftpThreadPool.size() > 0) && (finishedThread == -1)) {
            for (int i = 0; i < ftpThreadPool.size(); i++) {
                ftpWorkerThread = (FTPWorkerThread) ftpThreadPool.get(i);

                if (ftpWorkerThread.bFileDownloaded) {
                    finishedThread = i;
                    i = ftpThreadPool.size();
                }
            }

            // sleep a little whilst waiting for downloads
            if (finishedThread == -1) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException ie) {
                    throw new KETLReadException(ie);
                }
            }
        }

        if (finishedThread != -1) {
            ftpWorkerThread = (FTPWorkerThread) ftpThreadPool.remove(finishedThread);

            int pos = 0;
            // Need to copy the data array, since other steps are pointing to these objects...
            for (int i = 0; i < this.mOutPorts.length; i++) {

                if (this.mOutPorts[i].isUsed()) {
                    if (this.mOutPorts[i].isConstant())
                        pResultArray[pos++] = this.mOutPorts[i].getConstantValue();
                    else if (this.mOutPorts[i].mstrName.compareTo(FILENAME) == 0) {
                        pResultArray[pos++] = ftpWorkerThread.fileName;
                    }
                    else if (this.mOutPorts[i].mstrName.compareTo(SERVER) == 0) {
                        pResultArray[pos++] = (ftpWorkerThread.server);
                    }
                    else if (this.mOutPorts[i].mstrName.compareTo(USER) == 0) {
                        pResultArray[pos++] = (ftpWorkerThread.user);
                    }
                    else if (this.mOutPorts[i].mstrName.compareTo(TRANSFER_TYPE) == 0) {
                        pResultArray[pos++] = (ftpWorkerThread.transferType);
                    }
                    else if (this.mOutPorts[i].mstrName.compareTo(FILESIZE) == 0) {
                        pResultArray[pos++] = (ftpWorkerThread.fileSize);
                    }
                    else if (this.mOutPorts[i].mstrName.compareTo(DOWNLOAD_TIME) == 0) {
                        pResultArray[pos++] = (ftpWorkerThread.downloadTime);
                    }
                    else if (this.mOutPorts[i].mstrName.compareTo(DESTINATIONFILE) == 0) {
                        pResultArray[pos++] = (ftpWorkerThread.destFileName);
                    }
                }
            }
        }
        else {
            return COMPLETE;
        }
        return 1;
    }

    class FTPFetchOutPort extends ETLOutPort {



        public FTPFetchOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        @Override
        protected void setPortClass() throws ClassNotFoundException {
            String type = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "NAME", null);
            String dtype = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "DATATYPE", null);
            if (dtype != null && type != null) {
                if (type.equalsIgnoreCase(DOWNLOAD_TIME) || type.equalsIgnoreCase(FILESIZE)) {
                    type = "LONG";
                }
                else {
                    type = "STRING";
                }
                this.getXMLConfig().setAttribute("DATATYPE", type);
            }

            super.getPortClass();
        }
    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new FTPFetchOutPort(this, srcStep);
    }

    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub
        
    }
}
