package com.kni.etl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * The Class NetworkClassLoader.
 */
public class NetworkClassLoader extends ClassLoader {

  /**
   * Gets the class.
   * 
   * @param pFileName the file name
   * @param pClassName the class name
   * 
   * @return the class
   * 
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public Class<?> getClass(String pFileName, String pClassName) throws IOException {
    byte[] b = this.loadClassData(pFileName);

    return this.defineClass(pClassName, b, 0, b.length);
  }

  /**
   * Load class data.
   * 
   * @param pFileName the file name
   * 
   * @return the byte[]
   * 
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private byte[] loadClassData(String pFileName) throws IOException {
    // load the class data from the connection
    File javafile = new File(pFileName);

    // Create the byte array to hold the data
    byte[] buf = new byte[(int) javafile.length()]; // file is object of java.io.File for which you
                                                    // want the
    // byte array

    FileInputStream is = new FileInputStream(javafile);

    // Read in the bytes
    int offset = 0;
    int numRead = 0;

    while ((offset < buf.length) && ((numRead = is.read(buf, offset, buf.length - offset)) >= 0)) { // is
      // fileinputstream
      offset += numRead;
    }

    is.close();

    return buf;
  }
}
