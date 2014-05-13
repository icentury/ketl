package com.kni.etl.ketl.writer;

import java.io.File;
import java.io.IOException;

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

    AmazonS3 conn = new AmazonS3Client(credentials);
    PutObjectResult putResult =
        conn.putObject(bucketName, directory + File.separator + this.getFile().getName(),
            this.getFile());

    ResourcePool.logMessage("Uploaded " + this.getS3Location());
    this.getFile().delete();
  }

  public String getS3Location() {
    return "s3://" + bucketName + File.separator + directory + File.separator
        + this.getFile().getName();

  }

}
