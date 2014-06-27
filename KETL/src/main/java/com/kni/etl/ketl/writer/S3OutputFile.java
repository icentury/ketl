package com.kni.etl.ketl.writer;

import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.kni.etl.dbutils.ResourcePool;

public class S3OutputFile extends OutputFile {

  private BasicAWSCredentials credentials;
  private String bucketName;
  private String directory;
  private Integer mS3Expire;

  public S3OutputFile(String accessKey, String secretKey, String bucketName, String directory,
      String charSet, boolean zip, int miOutputBufferSize) {
    super(charSet, zip, miOutputBufferSize);

    this.credentials = new BasicAWSCredentials(accessKey, secretKey);
    this.bucketName = bucketName;
    this.directory = directory;
  }

  @Override
  public void close() throws IOException {
    super.close();
    int attempts = 0;
    while (attempts++ < 3) {
      try {
        AmazonS3 conn = new AmazonS3Client(credentials);
        PutObjectResult putResult =
            conn.putObject(bucketName, directory + File.separator + this.getFile().getName(),
                this.getFile());

        ResourcePool.logMessage("Uploaded " + this.getS3Location());
        this.getFile().delete();
        return;
      } catch (AmazonClientException e) {
        try {
          ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
              "Upload failed, retrying.." + e.toString());
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
          throw new IOException(e1);
        }
      }
    }
  }

  public String getS3Location() {
    return "s3://" + bucketName + File.separator + directory + File.separator
        + this.getFile().getName();

  }

  public void delete() throws IOException {
    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deleting file "
        + this.getFile().getAbsolutePath());
    super.close();
    this.getFile().delete();
  }

}
