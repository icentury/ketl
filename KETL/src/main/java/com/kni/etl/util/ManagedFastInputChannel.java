/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;

import com.kni.etl.FieldLevelFastInputChannel;

// TODO: Auto-generated Javadoc
/**
 * The Class ManagedFastInputChannel.
 */
public class ManagedFastInputChannel implements ManagedInputChannel {

  /** The mf channel. */
  private ReadableByteChannel mfChannel;

  /** The mi current record. */
  public int miCurrentRecord;

  /** The path. */
  public String mPath;

  /** The reader. */
  private FieldLevelFastInputChannel mReader;

  public File file;

  private File f;

  private FileInputStream fi;

  private long len;

  public ManagedFastInputChannel(String path) throws FileNotFoundException {
    this.f = new File(path);
    this.fi = new FileInputStream(f);
    this.mfChannel = fi.getChannel();
    this.mPath = path;
    this.file = f;
    this.len = this.f.length();
  }

  public ManagedFastInputChannel(String path, InputStream tmpStream, long len) {
    this.mfChannel = java.nio.channels.Channels.newChannel(tmpStream);
    this.mPath = path;
    this.len = len;
  }



  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.util.ManagedInputChannel#getChannel()
   */
  @Override
  public ReadableByteChannel getChannel() {
    return mfChannel;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.util.ManagedInputChannel#close()
   */
  @Override
  public void close() throws IOException {
    this.mfChannel.close();
    if (this.mReader != null)
      this.mReader.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.util.ManagedInputChannel#length()
   */

  @Override
  public String getAbsolutePath() {
    return this.mPath;
  }

  @Override
  public String getName() {
    return this.mPath;
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
    return this.f.exists();
  }
}
