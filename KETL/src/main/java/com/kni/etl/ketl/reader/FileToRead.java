package com.kni.etl.ketl.reader;

/**
 * The Class FileToRead.
 */
public class FileToRead implements Comparable<FileToRead> {

  public String getFilePath() {
    return filePath;
  }

  /** The file path. */
  String filePath;

  /** The param list ID. */
  int paramListID;
  String id;

  /**
   * Instantiates a new file to read.
   * 
   * @param name the name
   * @param paramListID the param list ID
   */
  public FileToRead(String id, String name, int paramListID) {
    super();
    this.filePath = name;
    this.paramListID = paramListID;
    this.id = id;
  }

  @Override
  public int compareTo(FileToRead o) {
    return o.filePath.compareTo(this.filePath);
  }

}
