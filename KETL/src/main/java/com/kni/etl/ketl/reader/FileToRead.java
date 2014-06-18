package com.kni.etl.ketl.reader;

/**
 * The Class FileToRead.
 */
public class FileToRead implements Comparable<String> {

  public String getFilePath() {
    return filePath;
  }

  /** The file path. */
  String filePath;

  /** The param list ID. */
  int paramListID;

  /**
   * Instantiates a new file to read.
   * 
   * @param name the name
   * @param paramListID the param list ID
   */
  public FileToRead(String name, int paramListID) {
    super();
    this.filePath = name;
    this.paramListID = paramListID;
  }

  @Override
  public int compareTo(String o) {
    return o.compareTo(this.filePath);
  }

}
