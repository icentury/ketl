package com.kni.etl.ketl.writer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.dbutils.postgresql.redshift.FTPTempFileWriter;
import com.kni.etl.ketl.dbutils.postgresql.redshift.RedshiftCopyFileWriter;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.reader.FTPFileReader;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;


public class FTPFileWriter extends ETLWriter implements DefaultWriterCore, WriterBatchManager {

  /**
   * The Class PGBulkETLInPort.
   */
  public class FTPETLInPort extends ETLInPort {


    public Class<?> targetClass;

    /**
     * Instantiates a new PG bulk ETL in port.
     * 
     * @param esOwningStep the es owning step
     * @param esSrcStep the es src step
     */
    public FTPETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
      super(esOwningStep, esSrcStep);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
      int res = super.initialize(xmlConfig);

      if (res != 0)
        return res;



      if (XMLHelper
          .getAttributeAsBoolean(xmlConfig.getAttributes(), NIOFileWriter.FILE_NAME, false)) {
        FTPFileWriter.this.fileNameInPort = true;

        FTPFileWriter.this.fileNameFormat =
            XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                NIOFileWriter.FILENAME_FORMAT, "{FILENAME}.{PARTITION}{SUBPARTITION}");
        FTPFileWriter.this.filePathFormat =
            XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                NIOFileWriter.FILEPATH_FORMAT, targetFilePath);
        FTPFileWriter.this.fileNamePort = this;

      }

      if (XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), NIOFileWriter.SUB_PARTITION,
          false)) {
        FTPFileWriter.this.subPartitionPort = this;
      }

      return 0;
    }

  }



  private int fileMaxRows;

  private String fileNameFormat;

  private boolean fileNameInPort = false;

  private FTPETLInPort fileNamePort;

  private String filePathFormat;

  private String mCharset;

  private int mIOBufferSize;

  /** The writer list. */
  private final List<FTPTempFileWriter> mWriterList = new ArrayList<FTPTempFileWriter>();

  private final Map<String, FTPTempFileWriter> mWriterMap =
      new HashMap<String, FTPTempFileWriter>();

  /** The writers. */
  private FTPTempFileWriter[] mWriters;

  private boolean mZip = true;

  /** The records in batch. */
  private int recordsInBatch = 0;

  public FTPETLInPort subPartitionPort;

  private String targetFilePath;

  private String mUser;

  private String mPassword;

  private String mServer;

  private boolean mBinary;

  private String targetDir;

  private boolean mOverWrite;

  private boolean mIncludeHeader;

  private String mDatetimeFormat;

  private String mDateFormat;

  private char mDelimiter = '\001';

  private String mFilePrefix;


  public FTPFileWriter(Node pXMLConfig, int pPartitionID, int pPartition,
      ETLThreadManager pThreadManager) throws KETLThreadException {
    super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
  }

  @Override
  protected void close(boolean success, boolean jobFailed) {
    try {

      if (this.mWriterList != null) {
        for (FTPTempFileWriter wr : this.mWriterList) {
          try {
            wr.rollback();
          } catch (IOException e) {
            ResourcePool.LogException(e, this);
          }
        }
      }


    } catch (Throwable e) {
      ResourcePool.LogException(e, this);
    }

  }

  @Override
  public int complete() throws KETLThreadException {
    int res = super.complete();
    if (res != 0)
      return res;
    try {

      if (this.recordsInBatch > 0) {
        for (FTPTempFileWriter wr : this.mWriterList) {
          try {
            wr.executeBatch();
          } catch (IOException e) {
            ResourcePool.LogException(e, this);
          }
        }
        this.recordsInBatch = 0;
      }
    } catch (Exception e) {
      throw new KETLThreadException(e, this);
    } finally {

      for (FTPTempFileWriter wr : this.mWriterList) {
        try {
          this.setWaiting("upload to S3");
          wr.close();
          this.setWaiting(null);
        } catch (Exception e) {
          ResourcePool.LogException(e, this);
        }
      }

      this.mWriterList.clear();



    }

    return res;
  }



  private FTPTempFileWriter createNewWriterMap(String fileName, String subPartition)
      throws KETLWriteException, IOException {

    String path = "";
    if (this.targetFilePath != null)
      path = this.targetFilePath + File.separator;
    else
      path = "." + File.separator;

    if (this.filePathFormat != null) {
      path = this.filePathFormat;
      path =
          path.replace("{PARTITION}", (this.partitions > 1 ? Integer.toString(this.partitionID)
              : ""));
      path = path.replace("{SUBPARTITION}", subPartition == null ? "" : subPartition);
      File f = new File(path);
      if (f.exists() == false) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Creating "
            + path + " directory " + f.getAbsolutePath());
        f.mkdir();
      } else if (f.isDirectory() == false) {
        throw new KETLWriteException("File path is invalid " + f.getAbsolutePath());
      }
      path = path + File.separator;
    }
    String fn = this.fileNameFormat;
    // partition id has to be part of filename
    if (this.partitions > 1 && fn.contains("{PARTITION}") == false) {
      path = path + this.partitionID + File.separator;
    }

    fn = fn.replace("{PARTITION}", (this.partitions > 1 ? Integer.toString(this.partitionID) : ""));
    fn = fn.replace("{SUBPARTITION}", subPartition == null ? "" : subPartition);
    fn = fn.replace("{FILENAME}", fileName);

    return createOutputFile(this.mIncludeHeader);
  }

  private FTPTempFileWriter createOutputFile(boolean includeHeaders) throws IOException {

    List<String> cols = new ArrayList<String>();
    for (int i = 0; i < this.mInPorts.length; i++) {
      if (this.mInPorts[i].skip == false) {
        cols.add(this.mInPorts[i].getName());
      }
    }
    FTPTempFileWriter writer =
        new FTPTempFileWriter(cols.toArray(new String[0]), this.mFilePrefix + this.getTargetFile(),
            this.getTargetPath(), new File(EngineConstants.PARTITION_PATH + File.separator
                + this.getPartitionID()), mUser, mPassword, mServer, this.mOverWrite, this.mBinary,
            this.mDelimiter, this.mCharset, this.mZip, this.mIOBufferSize, this.fileMaxRows,
            this.mDateFormat, this.mDatetimeFormat);

    if (includeHeaders) {
      int i = 1;
      for (String col : cols) {
        writer.setString(i++, col);
      }
      writer.addBatch();
    }
    this.mWriterList.add(writer);
    return writer;
  }

  private String getTargetFile() {
    return Paths.get(this.targetFilePath).getFileName().toString() + ".p" + this.partitionID;
  }

  private String getTargetPath() {
    return Paths.get(this.targetFilePath).getParent().toString();
  }


  @Override
  public int finishBatch(int len) throws KETLWriteException {
    int result = 0;
    if (this.recordsInBatch == this.batchSize
        || (this.recordsInBatch > 0 && len == BatchManager.LASTBATCH)) {
      try {
        for (FTPTempFileWriter wr : this.mWriterList) {
          try {
            wr.executeBatch();
            result += this.recordsInBatch;
          } catch (IOException e) {
            ResourcePool.LogException(e, this);
          }
        }

      } catch (Exception e) {
        throw new KETLWriteException(e);
      }
      this.recordsInBatch = 0;
    }
    return result;
  }


  @Override
  protected ETLInPort getNewInPort(ETLStep srcStep) {
    return new FTPETLInPort(this, srcStep);
  }

  @Override
  protected String getVersion() {
    return "$LastChangedRevision: 526 $";
  }


  @Override
  public int initialize(Node nConfig) throws KETLThreadException {
    int res = super.initialize(nConfig);

    if (res != 0) {
      return res;
    }

    // Get the attributes
    NamedNodeMap nmAttrs = nConfig.getAttributes();

    if (res != 0)
      return res;

    this.mIOBufferSize = XMLHelper.getAttributeAsInt(nmAttrs, "IOBUFFER", 16384);
    this.fileMaxRows = XMLHelper.getAttributeAsInt(nmAttrs, "MAXROWSPERFILE", Integer.MAX_VALUE);
    this.mZip = XMLHelper.getAttributeAsBoolean(nmAttrs, "COMPRESS", true);
    this.mIncludeHeader = XMLHelper.getAttributeAsBoolean(nmAttrs, "HEADER", false);
    this.mOverWrite = XMLHelper.getAttributeAsBoolean(nmAttrs, "OVERWRITE", true);
    this.mCharset =
        XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.CHARACTERSET_ATTRIB, "UTF-8");

    this.mDelimiter =
        XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.DELIMITER, "\001").charAt(0);
    this.mDatetimeFormat = XMLHelper.getAttributeAsString(nmAttrs, "DATETIMEFORMAT", "e");
    this.mDateFormat = XMLHelper.getAttributeAsString(nmAttrs, "DATEFORMAT", "e");
    this.mFilePrefix = XMLHelper.getAttributeAsString(nmAttrs, "FILEPREFIX", "");
    this.mCharset =
        XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.CHARACTERSET_ATTRIB, "UTF-8");
    this.mUser = this.getParameterValue(0, FTPFileReader.USER);
    this.mPassword = this.getParameterValue(0, FTPFileReader.PASSWORD);
    this.mServer = this.getParameterValue(0, FTPFileReader.SERVER);
    this.mBinary = XMLHelper.getAttributeAsBoolean(nmAttrs, "BINARY", true);
    this.targetFilePath = this.getParameterValue(0, "FILEPATH");

    return 0;
  }



  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.WriterBatchManager#initializeBatch(java.lang.Object [][], int)
   */
  @Override
  public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {

    return data;
  }

  @Override
  public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
      throws KETLWriteException {

    String fileName = "NA";
    Object subPartition = null;
    try {

      if (this.subPartitionPort != null) {
        subPartition = pInputRecords[this.subPartitionPort.getSourcePortIndex()];
      }

      if (this.fileNamePort != null) {
        Object tmp = pInputRecords[this.fileNamePort.getSourcePortIndex()];
        if (tmp != null)
          fileName = tmp.toString();

        String key = subPartition == null ? fileName : fileName + subPartition;
        FTPTempFileWriter wr = this.mWriterMap.get(key);

        if (wr == null) {
          wr = createNewWriterMap(fileName, subPartition == null ? null : subPartition.toString());
          this.mWriterMap.put(key, wr);
        }
        writeRecord(wr, pInputRecords, pExpectedDataTypes, pRecordWidth);
      } else {

        if (this.mWriters == null) {
          this.createOutputFile(this.mIncludeHeader);

          this.mWriters = new FTPTempFileWriter[this.mWriterList.size()];
          this.mWriterList.toArray(this.mWriters);

        }
        // all done write to streams
        for (FTPTempFileWriter wr : this.mWriters) {
          writeRecord(wr, pInputRecords, pExpectedDataTypes, pRecordWidth);
        }
      }

      for (FTPTempFileWriter wr : this.mWriterList) {
        wr.addBatch();
      }
    } catch (Exception e) {
      throw new KETLWriteException(e);
    }
    this.recordsInBatch++;
    return 1;
  }


  /**
   * Write.
   * 
   * @param pPreparedBatch the prepared batch
   * 
   * @throws SQLException the SQL exception
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void write(RedshiftCopyFileWriter pPreparedBatch) throws SQLException, IOException {
    RedshiftCopyFileWriter currentStatement = pPreparedBatch;
    currentStatement.executeBatch();
    currentStatement.commit();

  }

  private void writeRecord(FTPTempFileWriter stmt, Object[] pInputRecords,
      Class<?>[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException, IOException {

    for (int i = 0; i < this.mInPorts.length; i++) {

      if (((FTPETLInPort) this.mInPorts[i]).skip == false) {

        Class<?> cl, targetClass = ((FTPETLInPort) this.mInPorts[i]).targetClass;
        Object data;
        if (this.mInPorts[i].isConstant()) {
          cl = targetClass;
          data = this.mInPorts[i].getConstantValue();
        } else {
          cl = pExpectedDataTypes[this.mInPorts[i].getSourcePortIndex()];
          data = pInputRecords[this.mInPorts[i].getSourcePortIndex()];
        }

        if (data != null && Number.class.isAssignableFrom(cl)) {
          if (targetClass == Integer.class || targetClass == Long.class) {
            data = ((Number) data).longValue();
            cl = Long.class;
          } else {
            data = ((Number) data).doubleValue();
            cl = Double.class;
          }
        }

        if (data == null)
          stmt.setNull(i + 1, -1);
        else if (cl == String.class)
          stmt.setString(i + 1, (String) data);
        else if (cl == Integer.class || cl == int.class)
          stmt.setInt(i + 1, (Integer) data);
        else if (cl == Double.class || cl == double.class)
          stmt.setDouble(i + 1, (Double) data, 20);
        else if (cl == Long.class || cl == long.class)
          stmt.setLong(i + 1, (Long) data);
        else if (cl == Float.class || cl == Float.class)
          stmt.setFloat(i + 1, (Float) data);
        else if (cl == java.util.Date.class || cl == java.sql.Date.class
            || cl == java.sql.Timestamp.class || cl == java.sql.Time.class)
          if (targetClass == java.sql.Date.class)
            stmt.setDate(i + 1, (java.util.Date) data);
          else
            stmt.setTimestamp(i + 1, (java.util.Date) data);
        else if (cl == Boolean.class || cl == boolean.class)
          stmt.setBoolean(i + 1, (Boolean) data);
        else if (cl == byte[].class)
          stmt.setByteArrayValue(i + 1, (byte[]) data);
        else
          throw new KETLWriteException("Unsupported class for bulk writer " + cl.getCanonicalName());
      }
    }
  }

}
