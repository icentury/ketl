package com.kni.etl.util;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.kni.etl.FieldLevelFastInputChannel;
import com.kni.etl.dbutils.ResourcePool;

public class S3ManagedFastInputChannel implements ManagedInputChannel {
  /** The reader. */
  private FieldLevelFastInputChannel mReader;

  private String filePath;
  private S3Object o;
  private ReadableByteChannel ch;

  private AmazonS3Client s3Client;
  private String id;
  private AmazonS3URI s3URI;

  public S3ManagedFastInputChannel(String filePath, String id, AmazonS3Client s3Client) {

    this.filePath = filePath;
    this.s3URI = new AmazonS3URI(this.filePath);
    this.s3Client = s3Client;
    this.id = id;
  }

  @Override
  public ReadableByteChannel getChannel() {
    if (this.ch == null) {
      this.o = this.s3Client.getObject(s3URI.getBucket(), s3URI.getKey());
      this.ch = java.nio.channels.Channels.newChannel(o.getObjectContent());
    }
    return ch;
  }

  @Override
  public void close() throws IOException {
    if (ch != null) {
      ch.close();
    }
  }


  @Override
  public String getAbsolutePath() {
    return this.filePath;
  }

  @Override
  public String getName() {
    return this.filePath;
  }

  @Override
  public FieldLevelFastInputChannel getReader() {
    return this.mReader;
  }

  @Override
  public void setReader(FieldLevelFastInputChannel fieldLevelFastInputChannel) {
    this.mReader = fieldLevelFastInputChannel;

  }

  @Override
  public boolean fileExists() {
    try {
      this.s3Client.getObjectMetadata(s3URI.getBucket(), s3URI.getKey()).getVersionId();
      return true;
    } catch (AmazonClientException e) {
      ResourcePool.LogException(e, this);
    }

    return false;
  }

  @Override
  public String getId() {
    return id;
  }


}
