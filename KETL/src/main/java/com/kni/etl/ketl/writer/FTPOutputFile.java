package com.kni.etl.ketl.writer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.util.net.ftp.FTP;
import com.kni.util.net.ftp.FTPClient;
import com.kni.util.net.ftp.FTPConnectionClosedException;
import com.kni.util.net.ftp.FTPFile;
import com.kni.util.net.ftp.FTPReply;

public class FTPOutputFile extends OutputFile {

  private String targetDirectory, targetFile, user, password, server;
  private boolean deleted = false, binary;
  private boolean overWrite;

  public FTPOutputFile(String user, String password, String server, boolean binary,
      String filename, String directory, String charSet, boolean overWrite, boolean zip,
      int miOutputBufferSize) {
    super(charSet, zip, miOutputBufferSize);

    this.user = user;
    this.overWrite = overWrite;
    this.password = password;
    this.server = server;
    this.binary = binary;
    this.targetDirectory = directory;
    this.targetFile = filename;
  }

  @Override
  public void close() throws IOException {
    super.close();
    int attempts = 0;
    while (attempts++ < 3) {
      FTPClient ftpClient = this.getFTPConnection(user, password, server, binary, "Upload");
      InputStream inputStream = new FileInputStream(this.getFile());
      try {
        ftpClient.changeWorkingDirectory(this.targetDirectory);
        FTPFile[] res = ftpClient.listFiles(this.targetFile);
        if (res != null) {
          if (overWrite == false)
            throw new IOException("Target file exists already: " + this.targetDirectory
                + File.separator + this.targetFile);
          ftpClient.deleteFile(this.targetFile);
        }


        String targetTmpFile = this.targetFile + ".tmp";
        boolean done = ftpClient.storeFile(targetTmpFile, inputStream);
        ftpClient.rename(targetTmpFile, this.targetFile);

        if (done) {
          ftpClient.disconnect();
          ResourcePool.logMessage("Uploaded " + this.getFTPLocation());
          this.deleted = this.getFile().delete();
          return;
        }
      } finally {
        inputStream.close();
      }
    }
  }

  public String getFTPLocation() {
    return "fttp://" + server + File.separator + this.targetDirectory + File.separator
        + this.targetFile;

  }

  public void delete() throws IOException {
    super.close();
    if (!deleted) {
      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deleting file "
          + this.getFile().getAbsolutePath());
      this.getFile().delete();
    }
  }


  private FTPClient getFTPConnection(String strUser, String strPassword, String strServer,
      boolean binaryTransfer, String connectionNote) throws SocketException, IOException {
    FTPClient ftp = new FTPClient();

    int reply;
    ftp.connect(strServer);
    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Connected to " + strServer + ", "
        + connectionNote);

    // After connection attempt, you should check the reply code to
    // verify
    // success.
    reply = ftp.getReplyCode();

    if (!FTPReply.isPositiveCompletion(reply)) {
      ftp.disconnect();
      ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP server refused connection.");

      return null;
    }


    try {
      if (!ftp.login(strUser, strPassword)) {
        ftp.logout();
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "FTP login failed.");

        return null;
      }

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
          "Remote system is " + ftp.getSystemName() + ", " + connectionNote);

      if (binaryTransfer) {
        ftp.setFileType(FTP.BINARY_FILE_TYPE);
      } else {
        ftp.setFileType(FTP.ASCII_FILE_TYPE);
      }

      // Use passive mode as default because most of us are
      // behind firewalls these days.
      ftp.enterLocalPassiveMode();
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

}
