package com.kni.etl.util;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import com.kni.etl.FieldLevelFastInputChannel;



public interface ManagedInputChannel {

  public abstract ReadableByteChannel getChannel();

  /**
   * Close.
   * 
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void close() throws IOException;


  public abstract boolean fileExists();

  public abstract String getAbsolutePath();

  public abstract String getName();

  public abstract FieldLevelFastInputChannel getReader();

  public abstract void setReader(FieldLevelFastInputChannel fieldLevelFastInputChannel);

}
