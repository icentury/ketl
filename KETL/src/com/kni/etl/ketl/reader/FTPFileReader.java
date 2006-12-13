package com.kni.etl.ketl.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.ManagedFastInputChannel;
import com.kni.util.net.ftp.DefaultFTPFileListParser;
import com.kni.util.net.ftp.FTP;
import com.kni.util.net.ftp.FTPClient;
import com.kni.util.net.ftp.FTPConnectionClosedException;
import com.kni.util.net.ftp.FTPFile;
import com.kni.util.net.ftp.FTPReply;

public class FTPFileReader extends FileReader {

    private void closeFTPConnections() {
        if (this.ftpClients == null) {
            return;
        }

        for (int i = 0; i < this.ftpClients.length; i++) {
            if (((FTPClient) ftpClients[i]).isConnected()) {
                try {
                    ((FTPClient) ftpClients[i]).disconnect();
                } catch (IOException f) {
                    // do nothing
                }
            }
        }
    }

    static String USER = "USER";
    static String PASSWORD = "PASSWORD";
    static String TRANSFER_TYPE = "TRANSFER_TYPE";
    static String BINARY = "BINARY";
    static String ASCII = "ASCII";
    static String SERVER = "SERVER";
    private Object[] ftpClients = null;
    static int FILENAME_POS = 0;
    static int PARAMLIST_ID_POS = 1;

    public FTPFileReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    public Object[][] getFTPFilenames(int iParamList) {
        Object[][] result = null;

        boolean binaryTransfer = true;

        String searchString = this.getParameterValue(iParamList, SEARCHPATH);

        if (searchString == null) {
            return null;
        }

        String tmp = this.getParameterValue(iParamList, TRANSFER_TYPE);

        if ((tmp != null) && tmp.equalsIgnoreCase(ASCII)) {
            binaryTransfer = false;
        }

        FTPClient ftp = getFTPConnection(this.getParameterValue(iParamList, USER), this.getParameterValue(iParamList,
                PASSWORD), this.getParameterValue(iParamList, SERVER), binaryTransfer, "Directory listing connection.");

        if (ftp == null) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not connect to server.");

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
                ArrayList res = new ArrayList();

                for (int i = 0; i < fList.length; i++) {
                    Object[] o = new Object[2];
                    o[PARAMLIST_ID_POS] = new Integer(iParamList);

                    if (pathName != null) {
                        o[FILENAME_POS] = pathName + pathSeperator + fList[i].getName();

                        res.add(o);
                    }
                    else {
                        o[FILENAME_POS] = fList[i].getName();
                        res.add(o);
                    }
                }

                if (res.size() > 0) {
                    result = new Object[res.size()][];
                    res.toArray(result);
                }
            }
        } catch (IOException e1) {
            ResourcePool.LogException(e1, this);
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, searchString + " caused IO Exception, file will be ignored");
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

    ArrayList tmpFTPClients = new ArrayList();

    int connectionCnt = 1;

    // Returns the number of actually opened paths...
    int getFileChannels(FileToRead[] astrPaths) throws Exception {
        int iNumPaths = 0;
        FTPClient ftp = null;
        boolean binaryTransfer = true;

        if (astrPaths == null) {
            return 0;
        }

        if (this.mAllowDuplicates == false) {
            this.maFiles = dedupFileList(this.maFiles);
        }

        // add file streams to channel reader
        for (int pos = 0; pos < astrPaths.length; pos++) {

            InputStream tmpStream;

            try {
                if (ftp == null) {
                    String tmp = this.getParameterValue(astrPaths[pos].paramListID, TRANSFER_TYPE);

                    if ((tmp != null) && tmp.equalsIgnoreCase(ASCII)) {
                        binaryTransfer = false;
                    }

                    ftp = getFTPConnection(this.getParameterValue(astrPaths[pos].paramListID, USER), this
                            .getParameterValue(astrPaths[pos].paramListID, PASSWORD), this.getParameterValue(
                            astrPaths[pos].paramListID, SERVER), binaryTransfer, "Parallel connection "
                            + ++connectionCnt);
                }

                if (ftp == null) {
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP: Could not connect to server.");

                    if (tmpFTPClients.size() > 0) {
                        this.ftpClients = tmpFTPClients.toArray();
                        this.closeFTPConnections();
                    }

                    return -1;
                }

                tmpStream = ftp.retrieveFileStream(astrPaths[pos].filePath);

                openChannels++;

                ManagedFastInputChannel rf = new ManagedFastInputChannel();
                rf.mfChannel = java.nio.channels.Channels.newChannel(tmpStream);
                rf.mPath = astrPaths[pos].filePath;
                this.mvReadyFiles.add(rf);
                this.maFiles.add(astrPaths[pos]);
                iNumPaths++;
            } catch (Exception e) {
                while (this.mvReadyFiles.size() > 0) {
                    ManagedFastInputChannel fs = (ManagedFastInputChannel) this.mvReadyFiles.remove(0);
                    this.close(fs, OK_RECORD);
                }
                throw new Exception("Failed to open file: " + e.toString());
            }

        }

        return iNumPaths;
    }

    private FTPClient getFTPConnection(String strUser, String strPassword, String strServer, boolean binaryTransfer,
            String connectionNote) {
        FTPClient ftp = new FTPClient();

        try {
            int reply;
            ftp.connect(strServer);
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Connected to " + strServer + ", " + connectionNote);

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
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,"FTP login failed.");

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
            ftp.enterLocalPassiveMode();
        } catch (FTPConnectionClosedException e) {
            ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, "Server closed connection.");
            ResourcePool.LogException(e, this);

            return null;
        } catch (IOException e) {
            ResourcePool.LogException(e, this);

            return null;
        }

        return ftp;
    }
}
