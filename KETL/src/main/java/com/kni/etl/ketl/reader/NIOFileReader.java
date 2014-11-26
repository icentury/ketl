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
package com.kni.etl.ketl.reader;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.CodingErrorAction;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.validator.routines.UrlValidator;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.kni.etl.EngineConstants;
import com.kni.etl.FieldLevelFastInputChannel;
import com.kni.etl.SharedCounter;
import com.kni.etl.SourceFieldDefinition;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.qa.QAEventGenerator;
import com.kni.etl.ketl.qa.QAForFileReader;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.ManagedFastInputChannel;
import com.kni.etl.util.ManagedInputChannel;
import com.kni.etl.util.S3ManagedFastInputChannel;
import com.kni.etl.util.XMLHelper;
import com.kni.util.Arrays;
import com.kni.util.FileTools;

// TODO: Auto-generated Javadoc
/**
 * The Class NIOFileReader.
 */
public class NIOFileReader extends ETLReader implements QAForFileReader {

  private static final String AWSCLIENT = "$AWSCLIENT";

  @Override
  protected String getVersion() {
    return "$LastChangedRevision$";
  }

  /**
   * The Class FileETLOutPort.
   */
  public class FileETLOutPort extends ETLOutPort {

    /** The sf. */
    SourceFieldDefinition sf;

    /**
     * Gets the source field definition.
     * 
     * @return the source field definition
     */
    public SourceFieldDefinition getSourceFieldDefinition() {
      if (this.sf == null)
        this.sf = this.getSourceFieldDefinitions(this);
      return this.sf;
    }

    /** The type map. */
    Class[] typeMap = {String.class, Double.class, Integer.class, Float.class, Long.class,
        Short.class, Date.class, Boolean.class, Byte.class, Byte[].class, Character.class,
        Character[].class};

    /** The type methods. */
    String[] typeMethods =
        {
            "FieldLevelFastInputChannel.toString(${chars}, ${length})",
            "FieldLevelFastInputChannel.toDouble(${chars}, ${length})",
            "FieldLevelFastInputChannel.toInteger(${chars}, ${length})",
            "FieldLevelFastInputChannel.toFloat(${chars}, ${length})",
            "FieldLevelFastInputChannel.toLong(${chars}, ${length})",
            "FieldLevelFastInputChannel.toShort(${chars}, ${length})",
            "FieldLevelFastInputChannel.toDate(${chars}, ${length}, ${dateformatter},${parseposition})",
            "FieldLevelFastInputChannel.toBoolean(${chars}, ${length})",
            "FieldLevelFastInputChannel.toByte(${chars}, ${length})",
            "FieldLevelFastInputChannel.toByteArray(${chars}, ${length})",
            "FieldLevelFastInputChannel.toChar(${chars}, ${length})",
            "FieldLevelFastInputChannel.toCharArray(${chars}, ${length})"};

    /**
     * Instantiates a new file ETL out port.
     * 
     * @param esOwningStep the es owning step
     * @param esSrcStep the es src step
     */
    public FileETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
      super(esOwningStep, esSrcStep);
    }

    /**
     * Gets the source field definitions.
     * 
     * @param port the port
     * 
     * @return the source field definitions
     */
    private final SourceFieldDefinition getSourceFieldDefinitions(FileETLOutPort port) {
      NamedNodeMap nmAttrs;
      String mstrDefaultFieldDelimeter = null;
      Element nlOut = port.getXMLConfig();

      SourceFieldDefinition srcFieldDefinition = new SourceFieldDefinition();
      nmAttrs = nlOut.getAttributes();
      nmAttrs.getNamedItem(NIOFileReader.NAME);

      if (mstrDefaultFieldDelimeter == null) {
        mstrDefaultFieldDelimeter =
            XMLHelper.getAttributeAsString(nlOut.getParentNode().getAttributes(),
                NIOFileReader.DELIMITER, null);
      }

      if (mstrDefaultFieldDelimeter == null) {
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
            "FileReader: No default delimiter has been specified, system default field delimiter '"
                + NIOFileReader.DEFAULT_FIELD_DELIMITER
                + "' will be used for fields without delimiters specified.");
        mstrDefaultFieldDelimeter = NIOFileReader.DEFAULT_FIELD_DELIMITER;
      }

      srcFieldDefinition.MaxLength =
          XMLHelper.getAttributeAsInt(nmAttrs, NIOFileReader.MAXIMUM_LENGTH,
              srcFieldDefinition.MaxLength);
      srcFieldDefinition.FixedLength =
          XMLHelper.getAttributeAsInt(nmAttrs, NIOFileReader.FIXED_LENGTH,
              srcFieldDefinition.FixedLength);
      srcFieldDefinition.ReadOrder =
          EngineConstants.resolveValueFromConstant(
              XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.READ_ORDER,
                  Integer.toString(srcFieldDefinition.ReadOrder)), srcFieldDefinition.ReadOrder);
      srcFieldDefinition.ReadOrderSequence =
          XMLHelper.getAttributeAsInt(nmAttrs, NIOFileReader.READ_ORDER_SEQUENCE,
              srcFieldDefinition.ReadOrderSequence);

      srcFieldDefinition.AutoTruncate =
          XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileReader.AUTOTRUNCATE,
              srcFieldDefinition.AutoTruncate);

      srcFieldDefinition.setDelimiter(XMLHelper.getAttributeAsString(nmAttrs,
          NIOFileReader.DELIMITER, mstrDefaultFieldDelimeter));

      srcFieldDefinition.setEscapeCharacter(XMLHelper.getAttributeAsString(nmAttrs,
          NIOFileReader.ESCAPE_CHAR, null));
      srcFieldDefinition.setEscapeDoubleQuotes(XMLHelper.getAttributeAsBoolean(nmAttrs,
          NIOFileReader.ESCAPE_DOUBLEQUOTES, false));

      srcFieldDefinition.setNullIf(XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.NULLIF,
          null));

      srcFieldDefinition.setQuoteStart(XMLHelper.getAttributeAsString(nmAttrs,
          NIOFileReader.QUOTESTART, srcFieldDefinition.getQuoteStart()));
      srcFieldDefinition.setQuoteEnd(XMLHelper.getAttributeAsString(nmAttrs,
          NIOFileReader.QUOTEEND, srcFieldDefinition.getQuoteEnd()));

      srcFieldDefinition.FormatString =
          XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.FORMAT_STRING,
              srcFieldDefinition.FormatString);
      srcFieldDefinition.DefaultValue =
          XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.DEFAULT_VALUE,
              srcFieldDefinition.DefaultValue);
      srcFieldDefinition.keepDelimiter =
          XMLHelper.getAttributeAsBoolean(nmAttrs, "KEEPDELIMITER", false);

      srcFieldDefinition.PartitionField =
          XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileReader.PARTITION_KEY, false);
      srcFieldDefinition.setInternal(XMLHelper.getAttributeAsString(nmAttrs, "INTERNAL", null));

      srcFieldDefinition.ObjectType =
          EngineConstants.resolveObjectNameToID(XMLHelper.getAttributeAsString(nmAttrs,
              "OBJECTTYPE", null));

      srcFieldDefinition.DataType = port.getPortClass();

      String trimStr = XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.TRIM, "FALSE");

      if ((trimStr != null) && trimStr.equalsIgnoreCase("TRUE")) {
        srcFieldDefinition.TrimValue = true;
      }

      return srcFieldDefinition;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLOutPort#generateCode(int)
     */
    @Override
    public String generateCode(int portReferenceIndex) throws KETLThreadException {

      this.getSourceFieldDefinition();

      String sfRef = this.mstrName + "FieldDef";
      NIOFileReader.this.getCodeField("SourceFieldDefinition",
          "((com.kni.etl.ketl.reader.NIOFileReader.FileETLOutPort)this.getOwner().getOutPort("
              + portReferenceIndex + ")).getSourceFieldDefinition()", false, true, sfRef);

      if (this.sf.hasInternal()) {
        switch (this.sf.internal) {
          case FILENAME:
            return this.getCodeGenerationReferenceObject() + "["
                + this.mesStep.getUsedPortIndex(this)
                + "] = ((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getFileName();";
          case FILEPATH:
            return this.getCodeGenerationReferenceObject() + "["
                + this.mesStep.getUsedPortIndex(this)
                + "] = ((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getFilePath();";
          case FILE_ID:
            return this.getCodeGenerationReferenceObject() + "["
                + this.mesStep.getUsedPortIndex(this)
                + "] = ((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getFileId();";
          default:
            throw new KETLThreadException("No method has been defined to handle internal "
                + this.sf.internal + ", contact support", this);
        }
      }

      StringBuilder code =
          new StringBuilder("\n// handle negative codes and keep trying to resolve\ndo { res = ");
      // if fixed length then grab fixed length field else use delimiter,
      // ////TODO: should resolve sf values to
      // constants

      if (this.sf.FixedLength > 0)
        code.append(" this.mReader.readFixedLengthField( " + sfRef + ".FixedLength," + sfRef
            + ".getQuoteStartAsChars(), " + sfRef + ".getQuoteEndAsChars(), buf);");
      else
        code.append("this.mReader.readDelimitedField(" + sfRef + ".getDelimiterAsChars(), " + sfRef
            + ".getQuoteStartAsChars(), " + sfRef + ".getQuoteEndAsChars(), " + sfRef
            + ".mEscapeDoubleQuotes, " + sfRef + ".escapeChar, " + sfRef + ".MaxLength, " + sfRef
            + ".AverageLength, buf, " + sfRef + ".AutoTruncate," + sfRef + ".keepDelimiter);");

      // handle
      code.append(" if(res <0) {char[] tmp = (char[]) this.getOwner().handlePortEventCode(res,"
          + Arrays.searchArray(NIOFileReader.this.mOutPorts, this)
          + "); if(tmp != null) buf=tmp;}} while(res < 0);");

      // if not used handle
      if (this.isUsed()) {
        // max length check required
        if (this.sf.MaxLength > -1)
          code.append("res = res > " + this.sf.MaxLength + "?" + this.sf.MaxLength + ":res;");

        // null check required
        if (this.sf.NullIf != null)
          code.append("res =  com.kni.etl.ketl.reader.NIOFileReader.charArrayEquals(buf, res, "
              + NIOFileReader.this.getCodeField("char[]", "\""
                  + new String(this.sf.NullIfCharArray) + "\".toCharArray()", true, true, null)
              + "," + this.sf.NullIfCharArray.length + ")?0:res;");

        if (this.sf.DefaultValue != null) {
          if (this.sf.DataType == String.class) {
            code.append("if(res == 0) "
                + this.getCodeGenerationReferenceObject()
                + "["
                + this.mesStep.getUsedPortIndex(this)
                + "] = "
                + NIOFileReader.this.getCodeField("String", "\"" + this.sf.DefaultValue + "\"",
                    true, true, null) + ";");
          } else
            code.append("if(res == 0) "
                + NIOFileReader.this.getCodeField("String", "\"" + this.sf.DefaultValue + "\"",
                    true, true, null) + ".getChars(0," + this.sf.DefaultValue.length() + ",buf,0);");
        }

        code.append("try{" + (this.sf.position != null ? sfRef + ".position.setIndex(0);\n" : "")
            + this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this)
            + "] = (res == 0?null:");

        int res = Arrays.searchArray(this.typeMap, this.sf.DataType);
        String method;
        if (res < 0)
          method =
              NIOFileReader.this
                  .getMethodMapFromSystemXML(
                      "CREATEOBJECT",
                      NIOFileReader.class,
                      this.sf.DataType,
                      "The datatype "
                          + this.sf.DataType
                          + " is not directly supported, please add mapping of the form [Class name].[Static Method Supported parameter Char[] ${chars} and Char Length ${length}, returning required datatype]");
        else
          method = this.typeMethods[res];

        method = EngineConstants.replaceParameter(method, "chars", "buf");
        method = EngineConstants.replaceParameter(method, "length", "res");
        method = EngineConstants.replaceParameter(method, "parseposition", sfRef + ".position");
        method =
            EngineConstants.replaceParameter(method, "dateformatter", sfRef + ".DateFormatter");

        code.append(method + ");");

        code.append("} catch (Exception e) { this.getOwner().handlePortException(e,"
            + portReferenceIndex + "); }");

      }

      return code.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLPort#containsCode()
     */
    @Override
    public boolean containsCode() throws KETLThreadException {
      return true;
    }

  }

  /** The ALLO w_ DUPLICATE s_ ATTRIBUTE. */
  public static String ALLOW_DUPLICATES_ATTRIBUTE = "ALLOWDUPLICATES";

  /** The Constant AUTOTRUNCATE. */
  public static final String AUTOTRUNCATE = "AUTOTRUNCATE";

  /** The CHARACTERSE t_ ATTRIB. */
  public static String CHARACTERSET_ATTRIB = "CHARACTERSET";

  /** The CODINGERRORACTIO n_ ATTRIB. */
  public static String CODINGERRORACTION_ATTRIB = "CODINGERRORACTION";

  /** The Constant DATATYPE. */
  public static final String DATATYPE = "DATATYPE";

  /** The DEFAUL t_ FIEL d_ DELIMITER. */
  public static String DEFAULT_FIELD_DELIMITER = ",";

  /** The DEFAUL t_ ALLO w_ INVALI d_ LAS t_ RECORD. */
  public static boolean DEFAULT_ALLOW_INVALID_LAST_RECORD = false;

  /** The DEFAUL t_ RECOR d_ DELIMITER. */
  public static String DEFAULT_RECORD_DELIMITER = "\n";

  /** The DEFAUL t_ VALUE. */
  public static String DEFAULT_VALUE = "DEFAULTVALUE";

  /** The DELETESOURC e_ ATTRIB. */
  public static String DELETESOURCE_ATTRIB = "DELETESOURCE";

  /** The DELIMITER. */
  public static String DELIMITER = "DELIMITER";

  /** The Constant ESCAPE_CHAR. */
  public static final String ESCAPE_CHAR = "ESCAPECHARACTER";

  /** The Constant ESCAPE_DOUBLEQUOTES. */
  public static final String ESCAPE_DOUBLEQUOTES = "ESCAPEDOUBLEQUOTES";

  /** The Constant FIXED_LENGTH. */
  public static final String FIXED_LENGTH = "FIXEDLENGTH";

  /** The FORMA t_ STRING. */
  public static String FORMAT_STRING = "FORMATSTRING";

  /** The IGNOR e_ ACTION. */
  public static String IGNORE_ACTION = "IGNORE";

  /** The ALLO w_ INVALI d_ LAS t_ RECORD. */
  public static String ALLOW_INVALID_LAST_RECORD = "ALLOWINVALIDLASTRECORD";

  /** The MA x_ RECOR d_ DELIMITE r_ LENGTH. */
  public static int MAX_RECORD_DELIMITER_LENGTH = 1;

  /** The MAXIMU m_ LENGTH. */
  public static String MAXIMUM_LENGTH = "MAXIMUMLENGTH";

  /** The MOVESOURC e_ ATTRIB. */
  public static String MOVESOURCE_ATTRIB = "MOVESOURCE";

  /** The NAME. */
  public static String NAME = "NAME";

  /** The Constant NULLIF. */
  public static final String NULLIF = "NULLIF";

  /** The Constant OK_RECORD. */
  protected static final int OK_RECORD = 0;

  /** The Constant PARTIAL_RECORD. */
  private static final int PARTIAL_RECORD = -1;

  /** The Constant PARTITION_KEY. */
  public static final String PARTITION_KEY = "PARTITIONKEY";

  private static final String MAX_MISSING_FILES_ATTRIBUTE = "MAXMISSINGFILES";

  /** The PATH. */
  public static String PATH = "PATH";

  /** The QUOTEEND. */
  public static String QUOTEEND = "QUOTEEND";

  /** The QUOTESTART. */
  public static String QUOTESTART = "QUOTESTART";

  /** The REA d_ ORDER. */
  public static String READ_ORDER = "READORDER";

  /** The REA d_ ORDE r_ SEQUENCE. */
  public static String READ_ORDER_SEQUENCE = "READORDERSEQUENCE";

  /** The RECOR d_ DELIMITER. */
  public static String RECORD_DELIMITER = "RECORD_DELIMITER";

  /** The REPLAC e_ ACTION. */
  public static String REPLACE_ACTION = "REPLACE";

  /** The REPOR t_ ACTION. */
  public static String REPORT_ACTION = "REPORT";

  /** The SAMPL e_ EVER y_ ATTRIBUTE. */
  public static String SAMPLE_EVERY_ATTRIBUTE = "SAMPLEEVERY";

  /** The SEARCHPATH. */
  public static String SEARCHPATH = "SEARCHPATH";

  /** The SKI p_ LINES. */
  public static String SKIP_LINES = "SKIPLINES";

  /** The SOR t_ BUFFE r_ PE r_ FILE. */
  public static String SORT_BUFFER_PER_FILE = "SORTBUFFERPERFILE";

  /** The TRIM. */
  public static String TRIM = "TRIM";

  /** The ZIPPED. */
  public static String ZIPPED = "ZIPPED";

  /**
   * Char array equals.
   * 
   * @param a the a
   * @param len the len
   * @param a2 the a2
   * @param len2 the len2
   * 
   * @return true, if successful
   */
  final public static boolean charArrayEquals(char[] a, int len, char[] a2, int len2) {
    if (a == a2)
      return true;
    if (a == null || a2 == null)
      return false;

    if (len != len2)
      return false;

    for (int i = 0; i < len; i++)
      if (a[i] != a2[i])
        return false;

    return true;
  }

  /**
   * Dedup file list.
   * 
   * @param pSource the source
   * 
   * @return the array list
   */
  static public List<FileToRead> dedupFileList(List<FileToRead> pSource) {
    Set<FileToRead> nl = new HashSet<FileToRead>();
    for (FileToRead file : pSource) {
      if (nl.add(file) == false)
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
            "Duplicate file found in search will be ignored, use "
                + NIOFileReader.ALLOW_DUPLICATES_ATTRIBUTE
                + "=\"TRUE\" attribute to allow for duplicate files. File: " + file);
    }

    return new ArrayList<FileToRead>(nl);
  }

  /** The buf. */
  private char[] buf;


  /** The ma files. */
  protected List<FileToRead> maFiles = new ArrayList<FileToRead>();

  /** The allow duplicates. */
  protected boolean mAllowDuplicates = false;

  /** The mb allow invalid last record. */
  private boolean mbAllowInvalidLastRecord;

  /** The mc default record delimter. */
  private char mcDefaultRecordDelimter;

  /** The coding error action. */
  private String mCharacterSet, mCodingErrorAction;

  /** The current file channel. */
  private ManagedInputChannel mCurrentFileChannel = null;

  /** The delete source. */
  private boolean mDeleteSource = false;

  /** The IO buffer size. */
  private int mIOBufferSize;

  /** The mi skip lines. */
  private int miSkipLines;

  /** The max line length. */
  private int mMaxLineLength;

  /** The move source. */
  private String mMoveSource = null;

  /** The mstr default field delimeter. */
  private String mstrDefaultFieldDelimeter;

  /** The mstr default record delimter. */
  private String mstrDefaultRecordDelimter;

  /** The mv ready files. */
  protected Vector<ManagedInputChannel> mvReadyFiles = new Vector<ManagedInputChannel>();

  /** The open channels. */
  protected int openChannels = 0;

  /**
   * Instantiates a new NIO file reader.
   * 
   * @param pXMLConfig the XML config
   * @param pPartitionID the partition ID
   * @param pPartition the partition
   * @param pThreadManager the thread manager
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  public NIOFileReader(Node pXMLConfig, int pPartitionID, int pPartition,
      ETLThreadManager pThreadManager) throws KETLThreadException {
    super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
  }

  /**
   * Close.
   * 
   * @param file the file
   * @param pCause the cause
   * 
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected final void close(ManagedInputChannel file, int pCause) throws IOException {
    switch (pCause) {
      case PARTIAL_RECORD:
        ResourcePool
            .LogMessage(this, ResourcePool.WARNING_MESSAGE, "Partial record at end of file");
        break;
    }
    file.close();

    this.openChannels--;
  }

  /**
   * Delete files.
   */
  private void deleteFiles() {
    for (Object o : this.maFiles) {
      File fn = new File((String) o);

      if (fn.exists()) {
        if (fn.delete()) {
          ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
              "Deleted file: " + fn.getAbsolutePath());
        } else
          ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
              "Failed to delete file: " + fn.getAbsolutePath());
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLReader#generateCoreImports()
   */
  @Override
  protected String generateCoreImports() {
    return super.generateCoreImports() + "import com.kni.etl.util.ManagedInputChannel;\n"
        + "import com.kni.etl.FieldLevelFastInputChannel;\n"
        + "import com.kni.etl.SourceFieldDefinition;\n";
  }

  /**
   * Gets the open channels.
   * 
   * @return the open channels
   */
  public int getOpenChannels() {
    return this.openChannels;
  }

  /** The oc nm. */
  private String ocNm;

  /** The buffer length. */
  private int mBufferLength;

  /** The zipped. */
  private boolean mZipped;

  private SharedCounter iMissingFiles;

  /** The CURREN t_ FIL e_ CHANNEL. */
  private static String CURRENT_FILE_CHANNEL =
      "((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getCurrentFileChannel().getReader()";

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#generatePortMappingCode()
   */
  @Override
  protected String generatePortMappingCode() throws KETLThreadException {
    StringBuilder sb = new StringBuilder();
    // declare fields
    this.ocNm =
        this.getCodeField("int",
            "((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getOpenChannels()", false,
            false, "openChannel");
    this.getCodeField("FieldLevelFastInputChannel", this.ocNm + " > 0?"
        + NIOFileReader.CURRENT_FILE_CHANNEL + ":null", false, true, "mReader");
    this.getCodeField("char[]",
        "((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getBuffer()", false, true, "buf");
    this.getCodeField("boolean", "false", false, true, "partialRecord");

    // generate port maps

    // generate mapping method header;
    sb.append(this.getRecordExecuteMethodHeader() + "\n");
    sb.append("if (this." + this.ocNm + " == 0) return COMPLETE;");

    // outputs
    if (this.mOutPorts != null)
      for (int i = 0; i < this.mOutPorts.length; i++) {
        if (i == 0)
          sb.append("partialRecord = false;");
        sb.append(this.mOutPorts[i].generateCode(i) + "\n");
        if (i == 0)
          sb.append("partialRecord = true;");
      }

    // generate mapping method footer
    sb.append(this.getRecordExecuteMethodFooter() + "\n");

    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLReader#getRecordExecuteMethodFooter()
   */
  @Override
  protected String getRecordExecuteMethodFooter() {
    return " partialRecord = false;}catch(java.io.EOFException e) {if(partialRecord){ throw new KETLReadException(\"Partial record at end of file\");}try {Object res = this.getOwner().handleException(e); if(res == null){return COMPLETE;}if(res != null && res instanceof FieldLevelFastInputChannel) { this.mReader = (FieldLevelFastInputChannel) res;return SKIP_RECORD;} } catch(Exception e1){throw new KETLReadException(e1);}"
        + super.getRecordExecuteMethodFooter();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#getCharacterSet()
   */
  public String getCharacterSet() {
    return this.mCharacterSet;
  }

  /**
   * Gets the current file channel.
   * 
   * @return the current file channel
   * @throws KETLThreadException
   * @throws Exception
   */
  public ManagedInputChannel getCurrentFileChannel() {
    return this.mCurrentFileChannel;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#getDefaultFieldDelimeter()
   */
  public String getDefaultFieldDelimeter() {
    return this.mstrDefaultFieldDelimeter;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#getDefaultRecordDelimter()
   */
  public char getDefaultRecordDelimter() {
    return this.mcDefaultRecordDelimter;
  }

  // Returns the number of actually opened paths...
  /**
   * Gets the file channels.
   * 
   * @param astrPaths the astr paths
   * 
   * @return the file channels
   * 
   * @throws Exception the exception
   */
  int getFileChannels(FileToRead[] astrPaths) throws Exception {
    int iNumPaths = 0;

    if (astrPaths == null) {
      return 0;
    }

    if (this.mAllowDuplicates == false) {
      this.maFiles = dedupFileList(this.maFiles);
    }

    UrlValidator urlValidator = new UrlValidator();
    for (FileToRead element : astrPaths) {
      try {
        ManagedInputChannel rf;
        if (urlValidator.isValid(element.filePath)) {
          rf = getStreamFromURL(element);
        } else {
          rf = new ManagedFastInputChannel(element.filePath);
        }
        if (rf.fileExists() == false) {
          if (this.iMissingFiles.value() > this.mMaxMissingFiles)
            throw new KETLError("File " + rf.getName()
                + " could not be found, to many missing files");
          else {
            this.iMissingFiles.increment(1);
            ResourcePool.logMessage("Skipping missing file " + rf.getName());
          }
        } else {
          this.openChannels++;
          this.mvReadyFiles.add(rf);
          this.maFiles.add(element);
          iNumPaths++;
        }
      } catch (Exception e) {
        while (this.mvReadyFiles.size() > 0) {
          ManagedInputChannel fs = this.mvReadyFiles.remove(0);
          this.close(fs, NIOFileReader.OK_RECORD);
        }
        throw new Exception("Failed to open file: " + e.toString());
      }

    }

    return iNumPaths;
  }


  private ManagedInputChannel getStreamFromURL(FileToRead arg0) throws IOException {
    URL url = new URL(arg0.filePath);
    if (url.getHost().contains("amazonaws")) {
      // 1hr timeout by default

      AmazonS3Client s3Client = (AmazonS3Client) this.getSharedResource(AWSCLIENT);
      if (s3Client == null) {
        ClientConfiguration config = new ClientConfiguration();
        String strTimeout = this.getParameterValue(arg0.paramListID, "AWSTIMEOUT");
        config.setSocketTimeout(strTimeout == null ? 1 * 60 * 1000 : Integer.parseInt(strTimeout));
        s3Client =
            new AmazonS3Client(new BasicAWSCredentials(this.getParameterValue(arg0.paramListID,
                "AWSKEY"), this.getParameterValue(arg0.paramListID, "AWSSECRET")), config);
        this.setSharedResource(AWSCLIENT, s3Client);
      }
      return new S3ManagedFastInputChannel(arg0.filePath, arg0.id, s3Client);


    }

    URLConnection con = url.openConnection();
    return new ManagedFastInputChannel(arg0.filePath, arg0.id, con.getInputStream(),
        con.getContentLength());


  }

  @Override
  protected void initWorker() {

    for (ManagedInputChannel mi : this.mvReadyFiles) {
      if (!mi.fileExists()) {
        throw new KETLError("File " + mi.getName() + " could not be found");
      }
    }
    while (this.mCurrentFileChannel == null && this.mvReadyFiles.size() > 0) {
      try {
        this.mCurrentFileChannel = this.getReader(this.mvReadyFiles.remove(0));
      } catch (Exception e) {
        throw new KETLError(e);
      }
    }

    super.initWorker();

  }

  public String getFilePath() {
    return this.mCurrentFileChannel.getAbsolutePath();
  }

  public String getFileName() {
    return this.mCurrentFileChannel.getName();
  }

  public String getFileId() {
    return this.mCurrentFileChannel.getId();
  }

  /**
   * Gets the files.
   * 
   * @return the files
   * 
   * @throws Exception the exception
   */
  private boolean getFiles() throws Exception {

    List<FileToRead> files = new ArrayList<FileToRead>();
    for (int i = 0; i < this.maParameters.size(); i++) {

      String searchPath = this.getParameterValue(i, NIOFileReader.SEARCHPATH);

      if (this.isObjectList(searchPath)) {
        List<String[]> fileNames =
            ResourcePool.getMetadata().getObjectList(searchPath,
                this.getJobExecutor().getCurrentETLJob().getLoadID());
        for (String[] element : fileNames) {
          files.add(new FileToRead(element[0], element[1], i));
        }
      } else {
        String[] fileNames = FileTools.getFilenames(searchPath);
        if (fileNames != null) {
          for (String element : fileNames) {
            files.add(new FileToRead(element, element, i));
          }
        }
      }
      java.util.Collections.sort(files);
    }

    if (files.size() == 0)
      return false;

    List<FileToRead> partitionFileList = new ArrayList<FileToRead>();

    for (int i = 0; i < files.size(); i++) {
      if (i % this.partitions == this.partitionID)
        partitionFileList.add(files.get(i));
    }

    FileToRead[] finalFileList = new FileToRead[partitionFileList.size()];
    partitionFileList.toArray(finalFileList);

    if (finalFileList.length > 0) {
      // if files found is 0 and no missing files were identified then fail
      this.iMissingFiles =
          this.getJobExecutor().getCurrentETLJob().getCounter(this.getName() + ".missingFiles");

      if (this.getFileChannels(finalFileList) <= 0 && this.iMissingFiles.value() == 0) {
        return false;
      }
    }


    return true;
  }

  /** The complete file list. */
  private List completeFileList = null;

  private int mMaxMissingFiles = 0;

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#getOpenFiles()
   */
  public List getOpenFiles() {

    if (this.completeFileList == null) {
      List files = new ArrayList();
      for (int i = 0; i < this.maParameters.size(); i++) {
        String[] fileNames =
            FileTools.getFilenames(this.getParameterValue(i, NIOFileReader.SEARCHPATH));

        if (fileNames != null) {
          for (String element : fileNames) {
            files.add(element);
          }

        }
      }
      this.completeFileList = files;
    }

    return this.completeFileList;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.ETLStep#getQAClass(java.lang.String)
   */
  @Override
  public String getQAClass(String strQAType) {
    if (strQAType.equalsIgnoreCase(QAEventGenerator.SIZE_TAG)) {
      return QAForFileReader.QA_SIZE_CLASSNAME;
    }

    if (strQAType.equalsIgnoreCase(QAEventGenerator.STRUCTURE_TAG)) {
      return QAForFileReader.QA_STRUCTURE_CLASSNAME;
    }

    if (strQAType.equalsIgnoreCase(QAEventGenerator.VALUE_TAG)) {
      return QAForFileReader.QA_VALUE_CLASSNAME;
    }

    if (strQAType.equalsIgnoreCase(QAEventGenerator.AMOUNT_TAG)) {
      return QAForFileReader.QA_AMOUNT_CLASSNAME;
    }

    if (strQAType.equalsIgnoreCase(QAEventGenerator.AGE_TAG)) {
      return QAForFileReader.QA_AGE_CLASSNAME;
    }

    if (strQAType.equalsIgnoreCase(QAEventGenerator.ITEMCHECK_TAG)) {
      return QAForFileReader.QA_ITEM_CHECK_CLASSNAME;
    }

    if (strQAType.equalsIgnoreCase(QAEventGenerator.RECORDCHECK_TAG)) {
      return QAForFileReader.QA_RECORD_CHECK_CLASSNAME;
    }

    return super.getQAClass(strQAType);
  }

  // Returns the number of actually opened paths...
  /**
   * Gets the reader.
   * 
   * @param file the file
   * 
   * @return the reader
   * 
   * @throws Exception the exception
   */
  private ManagedInputChannel getReader(ManagedInputChannel file) throws Exception {

    try {
      CodingErrorAction action = CodingErrorAction.REPORT;
      if (this.mCodingErrorAction.equalsIgnoreCase(NIOFileReader.IGNORE_ACTION))
        action = CodingErrorAction.IGNORE;
      else if (this.mCodingErrorAction.equalsIgnoreCase(NIOFileReader.IGNORE_ACTION))
        action = CodingErrorAction.IGNORE;

      file.setReader(new FieldLevelFastInputChannel(file.getChannel(), "r", this.mIOBufferSize,
          this.mCharacterSet, this.mZipped, action));

      if (this.mbAllowInvalidLastRecord) {
        file.getReader().allowForNoDelimeterAtEOF(true);
      } else
        file.getReader().allowForNoDelimeterAtEOF(false);

      ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
          "Reading file " + file.getAbsolutePath());
      try {
        for (int x = 0; x < this.miSkipLines; x++) {

          for (ETLOutPort element : this.mOutPorts) {
            SourceFieldDefinition sf = ((FileETLOutPort) element).sf;

            int res;

            do {
              if (sf.FixedLength > 0) {
                res =
                    file.getReader().readFixedLengthField(sf.FixedLength,
                        sf.getQuoteStartAsChars(), sf.getQuoteEndAsChars(), this.buf);
              } else {
                res =
                    file.getReader().readDelimitedField(sf.getDelimiterAsChars(),
                        sf.getQuoteStartAsChars(), sf.getQuoteEndAsChars(), sf.mEscapeDoubleQuotes,
                        sf.escapeChar, sf.MaxLength, sf.AverageLength, this.buf, sf.AutoTruncate);
              }

              if (res < 0) {
                char[] tmp = (char[]) this.handlePortEventCode(res, 4);
                if (tmp != null)
                  this.buf = tmp;
              }
            } while (res < 0);
          }
        }
      } catch (EOFException e) {
        ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Attempted to skip "
            + this.miSkipLines + " records but end of file reached");
        this.close(file, NIOFileReader.OK_RECORD);
        file = null;
      }

    } catch (Exception e) {
      this.close(file, NIOFileReader.OK_RECORD);
      for (Object o : this.mvReadyFiles) {
        ManagedInputChannel fc = (ManagedInputChannel) o;
        this.close(fc, NIOFileReader.OK_RECORD);
      }
      throw new Exception("Failed to open file: " + e.toString());
    }

    return file;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#getSkipLines()
   */
  public int getSkipLines() {
    return this.miSkipLines;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#getSourceFieldDefinition()
   */
  public SourceFieldDefinition[] getSourceFieldDefinition() {
    SourceFieldDefinition[] sf = new SourceFieldDefinition[this.mOutPorts.length];

    for (int i = 0; i < sf.length; i++) {
      sf[i] = ((FileETLOutPort) this.mOutPorts[i]).sf;
    }
    return sf;
  }

  /**
   * Handle event.
   * 
   * @param eventCode the event code
   * @param portIndex the port index
   * 
   * @return the object
   * 
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws KETLThreadException the KETL thread exception
   */
  protected Object handleEvent(int eventCode, int portIndex) throws IOException,
      KETLThreadException {
    switch (eventCode) {
      case FieldLevelFastInputChannel.END_OF_FILE:
        if (this.mCurrentFileChannel.getReader().isEndOfFile()) {
          this.close(this.mCurrentFileChannel,
              portIndex < this.mOutPorts.length - 1 ? NIOFileReader.PARTIAL_RECORD
                  : NIOFileReader.OK_RECORD);
          return null;
        } else
          throw new KETLThreadException("Problem passing field", this);

      case FieldLevelFastInputChannel.BUFFER_TO_SMALL:
        // increase buffer size and try again
        if (this.buf.length > this.mMaxLineLength * 4) {
          throw new KETLThreadException("Field " + this.mOutPorts[portIndex].mstrName
              + " length is greater than max line length of " + this.mMaxLineLength, this);
        }
        this.buf = new char[this.buf.length * 2];
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
            "Increased buffer size to allow for larger fields");
        return this.buf;
      default:
        throw new KETLThreadException("Result from field level parser unknown: " + eventCode, this);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#handleEventCode(int)
   */
  @Override
  public Object handleEventCode(int eventCode) {
    // TODO Auto-generated method stub
    return super.handleEventCode(eventCode);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#handleException(java.lang.Exception)
   */
  @Override
  public Object handleException(Exception e) throws Exception {
    if (e instanceof EOFException) {
      this.close(this.mCurrentFileChannel, NIOFileReader.OK_RECORD);
      while (this.mvReadyFiles.size() > 0) {
        this.mCurrentFileChannel = this.getReader(this.mvReadyFiles.remove(0));
        if (this.mCurrentFileChannel != null)
          return this.mCurrentFileChannel.getReader();
      }

      if (this.mDeleteSource)
        this.deleteFiles();
      else if (this.mMoveSource != null)
        this.moveFiles();

      return null;
    }
    return super.handleException(e);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#handlePortEventCode(int, int)
   */
  @Override
  public Object handlePortEventCode(int eventCode, int portIndex) throws IOException,
      KETLThreadException {
    switch (eventCode) {
      case FieldLevelFastInputChannel.END_OF_FILE:
        if (this.mCurrentFileChannel.getReader().isEndOfFile()) {
          this.close(this.getCurrentFileChannel(),
              portIndex < this.mOutPorts.length - 1 ? NIOFileReader.PARTIAL_RECORD
                  : NIOFileReader.OK_RECORD);
          return null;
        } else
          throw new KETLThreadException("Problem passing field", this);

      case FieldLevelFastInputChannel.BUFFER_TO_SMALL:
        // increase buffer size and try again
        if (this.buf.length > this.mMaxLineLength * 4) {
          throw new KETLThreadException("Field " + this.mOutPorts[portIndex].mstrName
              + " length is greater than max line length of " + this.mMaxLineLength, this);
        }
        this.buf = new char[this.buf.length * 2];
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
            "Increased buffer size to allow for larger fields");
        return this.buf;
      default:
        throw new KETLThreadException("Result from field level parser unknown: " + eventCode, this);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#handlePortException(java.lang.Exception, int)
   */
  @Override
  public Object handlePortException(Exception e, int portIndex) throws KETLThreadException {
    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
        "Unexpected error reading file " + e.toString());
    if (this.mRecordCounter >= 0)
      throw new KETLThreadException("Check record " + this.mRecordCounter
          + (portIndex >= 0 ? ", field " + (portIndex + 1) : ""), this);

    while (this.mvReadyFiles.size() > 0) {
      this.mCurrentFileChannel = this.mvReadyFiles.remove(0);
      try {
        this.close(this.mCurrentFileChannel, NIOFileReader.OK_RECORD);
      } catch (IOException e1) {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Could not close file channel - "
            + e.toString());
      }
    }

    return null;
  }

  /**
   * @param arg1
   * @param arg0
   * @param arg2
   */
  /*
   * private void handleError(DataItem arg1, SourceFieldDefinition arg0, Exception arg2) {
   * arg1.setNull(arg0.DataType); arg1.setError(arg2.toString()); this.miErrorCount++; if
   * (this.miErrorCount > this.miErrorLimit) throw new KETLException("Error limit of " +
   * this.miErrorLimit + " reached, last error: " + arg2.toString()); }
   */
  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.qa.QAForFileReader#ignoreLastRecord()
   */
  public boolean ignoreLastRecord() {
    return this.mbAllowInvalidLastRecord;
  }

  /**
   * DOCUMENT ME!.
   * 
   * @param xmlSourceNode DOCUMENT ME!
   * 
   * @return DOCUMENT ME!
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  @Override
  protected int initialize(Node xmlSourceNode) throws KETLThreadException {
    int res;

    if ((res = super.initialize(xmlSourceNode)) != 0) {
      return res;
    }

    if (this.maParameters == null) {
      ResourcePool.LogMessage(
          this,
          ResourcePool.ERROR_MESSAGE,
          "No complete parameter sets found, check that the following exist:\n"
              + this.getRequiredTagsMessage());

      return -2;
    }

    this.mAllowDuplicates =
        XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(),
            NIOFileReader.ALLOW_DUPLICATES_ATTRIBUTE, false);

    this.mMaxMissingFiles =
        XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(),
            NIOFileReader.MAX_MISSING_FILES_ATTRIBUTE, 0);
    this.mCharacterSet =
        XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
            NIOFileReader.CHARACTERSET_ATTRIB, java.nio.charset.Charset.defaultCharset().name());
    this.mDeleteSource =
        XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(),
            NIOFileReader.DELETESOURCE_ATTRIB, false);
    this.mMoveSource =
        XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
            NIOFileReader.MOVESOURCE_ATTRIB, null);
    this.mCharacterSet =
        XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
            NIOFileReader.CHARACTERSET_ATTRIB, null);
    this.mCodingErrorAction =
        XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
            NIOFileReader.CODINGERRORACTION_ATTRIB, NIOFileReader.REPORT_ACTION);
    this.mZipped =
        XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(), NIOFileReader.ZIPPED, false);
    this.miSkipLines =
        XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), NIOFileReader.SKIP_LINES, 0);
    this.mstrDefaultRecordDelimter =
        XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
            NIOFileReader.RECORD_DELIMITER, NIOFileReader.DEFAULT_RECORD_DELIMITER);
    this.mbAllowInvalidLastRecord =
        (XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(),
            NIOFileReader.ALLOW_INVALID_LAST_RECORD,
            NIOFileReader.DEFAULT_ALLOW_INVALID_LAST_RECORD));
    this.mIOBufferSize =
        XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), "IOBUFFER", 16384);
    this.mMaxLineLength =
        XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), "MAXLINELENGTH", 16384);

    this.mBufferLength = this.mMaxLineLength;
    SourceFieldDefinition lastNonInternalSf = null;
    for (int i = 0; i < this.mOutPorts.length; i++) {
      SourceFieldDefinition sf = ((FileETLOutPort) this.mOutPorts[i]).getSourceFieldDefinition();

      // seed the average field length
      if (sf.AverageLength == 0)
        sf.AverageLength = FieldLevelFastInputChannel.MAXFIELDLENGTH;
      // seed the max field length
      if (sf.MaxLength < 0)
        sf.MaxLength = this.mMaxLineLength;

      if (this.mBufferLength < ((sf.getQuoteStart() == null ? 0 : sf.getQuoteStartLength())
          + sf.MaxLength + (sf.getQuoteEnd() == null ? 0 : sf.getQuoteEndLength())))
        this.mBufferLength =
            ((sf.getQuoteStart() == null ? 0 : sf.getQuoteStartLength()) + sf.MaxLength + (sf
                .getQuoteEnd() == null ? 0 : sf.getQuoteEndLength()));

      // find last non internal field
      if (sf.hasInternal() == false)
        lastNonInternalSf = sf;
    }

    // for last record set delimeter
    if (lastNonInternalSf != null)
      lastNonInternalSf.setDelimiter(this.mstrDefaultRecordDelimter);

    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
        "Initial max line length accounting for quotes: " + this.mBufferLength);
    this.buf = new char[this.mBufferLength];

    for (ETLOutPort element : this.mOutPorts) {
      SourceFieldDefinition sf = ((FileETLOutPort) element).sf;

      if (java.util.Date.class.isAssignableFrom(sf.DataType)) {
        sf.DateFormatter = new FastSimpleDateFormat(sf.FormatString);
        sf.position = new ParsePosition(0);
      }

    }

    try {
      if (this.getFiles() == false) {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "No files found");

        for (int i = 0; i < this.maParameters.size(); i++) {
          String searchPath = this.getParameterValue(i, NIOFileReader.SEARCHPATH);
          ResourcePool
              .LogMessage(this, ResourcePool.ERROR_MESSAGE, "Search path(s): " + searchPath);
        }

        throw new KETLThreadException("No files found, check search paths", this);
      }
    } catch (Exception e) {
      throw new KETLThreadException(e, this);
    }

    // NOTE: Should return a declared constant not 0 or 1
    return 0;
  }

  /**
   * Move files.
   */
  private void moveFiles() {
    for (Object o : this.maFiles) {
      File fn = new File((String) o);

      // Destination directory
      File dir = new File(this.mMoveSource);

      // Move file to new directory
      if (fn.renameTo(new File(dir, fn.getName())) == false)
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
            "Failed to move file: " + fn.getAbsolutePath());
      else
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
            "Moved file: " + fn.getAbsolutePath());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
   */
  @Override
  protected ETLOutPort getNewOutPort(com.kni.etl.ketl.ETLStep srcStep) {
    return new FileETLOutPort(this, this);
  }

  /**
   * Gets the buffer.
   * 
   * @return the buffer
   */
  public char[] getBuffer() {
    return this.buf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
   */
  @Override
  protected void close(boolean success, boolean jobFailed) {

    if (this.getSharedResource(AWSCLIENT) != null) {
      ((AmazonS3Client) this.getSharedResource(AWSCLIENT)).shutdown();
      this.setSharedResource(AWSCLIENT, null);
    }
    if (this.mvReadyFiles == null)
      return;

    for (Object o : this.mvReadyFiles) {
      ManagedInputChannel rf = (ManagedInputChannel) o;
      try {
        rf.close();
      } catch (IOException e) {
        ResourcePool.LogException(e, this);
      }
    }

  }

}
