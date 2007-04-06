/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.reader;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.CodingErrorAction;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Vector;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.FieldLevelFastInputChannel;
import com.kni.etl.SourceFieldDefinition;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.qa.QAEventGenerator;
import com.kni.etl.ketl.qa.QAForFileReader;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.ManagedFastInputChannel;
import com.kni.etl.util.XMLHelper;
import com.kni.util.Arrays;
import com.kni.util.FileTools;

public class NIOFileReader extends ETLReader implements QAForFileReader {

    public class FileETLOutPort extends ETLOutPort {

        SourceFieldDefinition sf;

        public SourceFieldDefinition getSourceFieldDefinition() {
            if (this.sf == null)
                this.sf = this.getSourceFieldDefinitions(this);
            return this.sf;
        }

        Class[] typeMap = { String.class, Double.class, Integer.class, Float.class, Long.class, Short.class,
                Date.class, Boolean.class, Byte.class, Byte[].class, Character.class, Character[].class };

        String[] typeMethods = { "FieldLevelFastInputChannel.toString(${chars}, ${length})",
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
                "FieldLevelFastInputChannel.toCharArray(${chars}, ${length})" };

        public FileETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        private final SourceFieldDefinition getSourceFieldDefinitions(FileETLOutPort port) {
            NamedNodeMap nmAttrs;
            String mstrDefaultFieldDelimeter = null;
            Element nlOut = port.getXMLConfig();

            SourceFieldDefinition srcFieldDefinition = new SourceFieldDefinition();
            nmAttrs = nlOut.getAttributes();
            nmAttrs.getNamedItem(NIOFileReader.NAME);

            if (mstrDefaultFieldDelimeter == null) {
                mstrDefaultFieldDelimeter = XMLHelper.getAttributeAsString(nlOut.getParentNode().getAttributes(),
                        NIOFileReader.DELIMITER, null);
            }

            if (mstrDefaultFieldDelimeter == null) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                        "FileReader: No default delimiter has been specified, system default field delimiter '"
                                + NIOFileReader.DEFAULT_FIELD_DELIMITER + "' will be used for fields without delimiters specified.");
                mstrDefaultFieldDelimeter = NIOFileReader.DEFAULT_FIELD_DELIMITER;
            }

            srcFieldDefinition.MaxLength = XMLHelper.getAttributeAsInt(nmAttrs, NIOFileReader.MAXIMUM_LENGTH,
                    srcFieldDefinition.MaxLength);
            srcFieldDefinition.FixedLength = XMLHelper.getAttributeAsInt(nmAttrs, NIOFileReader.FIXED_LENGTH,
                    srcFieldDefinition.FixedLength);
            srcFieldDefinition.ReadOrder = EngineConstants.resolveValueFromConstant(XMLHelper.getAttributeAsString(
                    nmAttrs, NIOFileReader.READ_ORDER, Integer.toString(srcFieldDefinition.ReadOrder)), srcFieldDefinition.ReadOrder);
            srcFieldDefinition.ReadOrderSequence = XMLHelper.getAttributeAsInt(nmAttrs, NIOFileReader.READ_ORDER_SEQUENCE,
                    srcFieldDefinition.ReadOrderSequence);

            srcFieldDefinition.AutoTruncate = XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileReader.AUTOTRUNCATE,
                    srcFieldDefinition.AutoTruncate);

            srcFieldDefinition.setDelimiter(XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.DELIMITER,
                    mstrDefaultFieldDelimeter));

            srcFieldDefinition.setEscapeCharacter(XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.ESCAPE_CHAR, null));
            srcFieldDefinition.setEscapeDoubleQuotes(XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileReader.ESCAPE_DOUBLEQUOTES,
                    false));

            srcFieldDefinition.setNullIf(XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.NULLIF, null));

            srcFieldDefinition.setQuoteStart(XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.QUOTESTART, srcFieldDefinition
                    .getQuoteStart()));
            srcFieldDefinition.setQuoteEnd(XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.QUOTEEND, srcFieldDefinition
                    .getQuoteEnd()));

            srcFieldDefinition.FormatString = XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.FORMAT_STRING,
                    srcFieldDefinition.FormatString);
            srcFieldDefinition.DefaultValue = XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.DEFAULT_VALUE,
                    srcFieldDefinition.DefaultValue);
            srcFieldDefinition.keepDelimiter = XMLHelper.getAttributeAsBoolean(nmAttrs, "KEEPDELIMITER", false);

            srcFieldDefinition.PartitionField = XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileReader.PARTITION_KEY, false);

            srcFieldDefinition.ObjectType = EngineConstants.resolveObjectNameToID(XMLHelper.getAttributeAsString(
                    nmAttrs, "OBJECTTYPE", null));

            srcFieldDefinition.DataType = port.getPortClass();

            String trimStr = XMLHelper.getAttributeAsString(nmAttrs, NIOFileReader.TRIM, "FALSE");

            if ((trimStr != null) && trimStr.equalsIgnoreCase("TRUE")) {
                srcFieldDefinition.TrimValue = true;
            }

            return srcFieldDefinition;
        }

        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            this.getSourceFieldDefinition();

            String sfRef = this.mstrName + "FieldDef";
            NIOFileReader.this.getCodeField("SourceFieldDefinition",
                    "((com.kni.etl.ketl.reader.NIOFileReader.FileETLOutPort)this.getOwner().getOutPort("
                            + portReferenceIndex + ")).getSourceFieldDefinition()", false, true, sfRef);

            StringBuilder code = new StringBuilder("\n// handle negative codes and keep trying to resolve\ndo { res = ");
            // if fixed length then grab fixed length field else use delimiter, ////TODO: should resolve sf values to
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
                    + Arrays.searchArray(NIOFileReader.this.mOutPorts, this) + "); if(tmp != null) buf=tmp;}} while(res < 0);");

            // if not used handle
            if (this.isUsed()) {
                // max length check required
                if (this.sf.MaxLength > -1)
                    code.append("res = res > " + this.sf.MaxLength + "?" + this.sf.MaxLength + ":res;");

                // null check required
                if (this.sf.NullIf != null)
                    code.append("res =  com.kni.etl.ketl.reader.NIOFileReader.charArrayEquals(buf, res, "
                            + NIOFileReader.this.getCodeField("char[]", "\"" + new String(this.sf.NullIfCharArray) + "\".toCharArray()", true,
                                    true, null) + "," + this.sf.NullIfCharArray.length + ")?0:res;");

                if (this.sf.DefaultValue != null) {
                    if (this.sf.DataType == String.class) {
                        code.append("if(res == 0) " + this.getCodeGenerationReferenceObject() + "["
                                + this.mesStep.getUsedPortIndex(this) + "] = "
                                + NIOFileReader.this.getCodeField("String", "\"" + this.sf.DefaultValue + "\"", true, true, null) + ";");
                    }
                    else
                        code.append("if(res == 0) "
                                + NIOFileReader.this.getCodeField("String", "\"" + this.sf.DefaultValue + "\"", true, true, null)
                                + ".getChars(0," + this.sf.DefaultValue.length() + ",buf,0);");
                }

                code.append("try{" + (this.sf.position != null ? sfRef + ".position.setIndex(0);\n" : "")
                        + this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this)
                        + "] = (res == 0?null:");

                int res = Arrays.searchArray(this.typeMap, this.sf.DataType);
                String method;
                if (res < 0)
                    method = NIOFileReader.this.getMethodMapFromSystemXML(
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
                method = EngineConstants.replaceParameter(method, "dateformatter", sfRef + ".DateFormatter");

                code.append(method + ");");

                code.append("} catch (Exception e) { this.getOwner().handlePortException(e," + portReferenceIndex
                        + "); }");

            }

            return code.toString();
        }

        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }

    }

    public static String ALLOW_DUPLICATES_ATTRIBUTE = "ALLOWDUPLICATES";

    public static final String AUTOTRUNCATE = "AUTOTRUNCATE";

    public static String CHARACTERSET_ATTRIB = "CHARACTERSET";
    public static String CODINGERRORACTION_ATTRIB = "CODINGERRORACTION";
    public static final String DATATYPE = "DATATYPE";
    public static String DEFAULT_FIELD_DELIMITER = ",";
    public static boolean DEFAULT_ALLOW_INVALID_LAST_RECORD = false;
    public static String DEFAULT_RECORD_DELIMITER = "\n";

    public static String DEFAULT_VALUE = "DEFAULTVALUE";
    public static String DELETESOURCE_ATTRIB = "DELETESOURCE";
    public static String DELIMITER = "DELIMITER";
    public static final String ESCAPE_CHAR = "ESCAPECHARACTER";
    public static final String ESCAPE_DOUBLEQUOTES = "ESCAPEDOUBLEQUOTES";
    public static final String FIXED_LENGTH = "FIXEDLENGTH";

    public static String FORMAT_STRING = "FORMATSTRING";
    public static String IGNORE_ACTION = "IGNORE";
    public static String ALLOW_INVALID_LAST_RECORD = "ALLOWINVALIDLASTRECORD";
    public static int MAX_RECORD_DELIMITER_LENGTH = 1;
    public static String MAXIMUM_LENGTH = "MAXIMUMLENGTH";
    public static String MOVESOURCE_ATTRIB = "MOVESOURCE";
    public static String NAME = "NAME";
    public static final String NULLIF = "NULLIF";
    protected static final int OK_RECORD = 0;
    private static final int PARTIAL_RECORD = -1;

    public static final String PARTITION_KEY = "PARTITIONKEY";
    public static String PATH = "PATH";
    public static String QUOTEEND = "QUOTEEND";
    public static String QUOTESTART = "QUOTESTART";
    public static String READ_ORDER = "READORDER";
    public static String READ_ORDER_SEQUENCE = "READORDERSEQUENCE";
    public static String RECORD_DELIMITER = "RECORD_DELIMITER";
    public static String REPLACE_ACTION = "REPLACE";
    public static String REPORT_ACTION = "REPORT";
    public static String SAMPLE_EVERY_ATTRIBUTE = "SAMPLEEVERY";
    public static String SEARCHPATH = "SEARCHPATH";
    public static String SKIP_LINES = "SKIPLINES";
    public static String SORT_BUFFER_PER_FILE = "SORTBUFFERPERFILE";
    public static String TRIM = "TRIM";
    public static String ZIPPED = "ZIPPED";

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

    static public ArrayList dedupFileList(ArrayList pSource) {
        HashSet nl = new HashSet();
        for (Object o : pSource) {
            String file = (String) o;
            if (nl.add(file) == false)
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                        "Duplicate file found in search will be ignored, use " + NIOFileReader.ALLOW_DUPLICATES_ATTRIBUTE
                                + "=\"TRUE\" attribute to allow for duplicate files. File: " + file);
        }

        return new ArrayList(nl);
    }

    private char[] buf;

    protected long bytesRead = 0;
    protected ArrayList maFiles = new ArrayList();
    protected boolean mAllowDuplicates = false;
    private boolean mbAllowInvalidLastRecord;
    private char mcDefaultRecordDelimter;
    private String mCharacterSet, mCodingErrorAction;
    private ManagedFastInputChannel mCurrentFileChannel = null;
    private boolean mDeleteSource = false;
    private int mIOBufferSize;
    private int miSkipLines;
    private int mMaxLineLength;
    private String mMoveSource = null;
    private String mstrDefaultFieldDelimeter;
    private String mstrDefaultRecordDelimter;
    protected Vector<ManagedFastInputChannel> mvReadyFiles = new Vector<ManagedFastInputChannel>();
    protected int openChannels = 0;

    public NIOFileReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    protected final void close(ManagedFastInputChannel file, int pCause) throws IOException {
        switch (pCause) {
        case PARTIAL_RECORD:
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Partial record at end of file");
            break;
        }
        file.close();

        this.openChannels--;
    }

    private void deleteFiles() {
        for (Object o : this.maFiles) {
            File fn = new File((String) o);

            if (fn.exists()) {
                if (fn.delete()) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deleted file: " + fn.getAbsolutePath());
                }
                else
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Failed to delete file: "
                            + fn.getAbsolutePath());
            }
        }
    }

    protected String generateCoreImports() {
        return super.generateCoreImports() + "import com.kni.etl.util.ManagedFastInputChannel;\n"
                + "import com.kni.etl.FieldLevelFastInputChannel;\n" + "import com.kni.etl.SourceFieldDefinition;\n";
    }

    public int getOpenChannels() {
        return this.openChannels;
    }

    private String ocNm;

    private int mBufferLength;

    private boolean mZipped;
    private static String CURRENT_FILE_CHANNEL = "((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getCurrentFileChannel().mReader";

    protected String generatePortMappingCode() throws KETLThreadException {
        StringBuilder sb = new StringBuilder();
        // declare fields
        this.ocNm = this.getCodeField("int", "((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getOpenChannels()",
                false, false, "openChannel");
        this.getCodeField("FieldLevelFastInputChannel", this.ocNm + " > 0?" + NIOFileReader.CURRENT_FILE_CHANNEL + ":null", false, true,
                "mReader");
        this.getCodeField("char[]", "((com.kni.etl.ketl.reader.NIOFileReader)this.getOwner()).getBuffer()", false,
                true, "buf");
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

    public ManagedFastInputChannel getCurrentFileChannel() {
        return this.mCurrentFileChannel;
    }

    /**
     * @return
     */
    public String getDefaultFieldDelimeter() {
        return this.mstrDefaultFieldDelimeter;
    }

    /**
     * @return
     */
    public char getDefaultRecordDelimter() {
        return this.mcDefaultRecordDelimter;
    }

    // Returns the number of actually opened paths...
    int getFileChannels(FileToRead[] astrPaths) throws Exception {
        int iNumPaths = 0;

        if (astrPaths == null) {
            return 0;
        }

        if (this.mAllowDuplicates == false) {
            this.maFiles = NIOFileReader.dedupFileList(this.maFiles);
        }

        for (FileToRead element : astrPaths) {
            FileInputStream fi;

            try {
                File f = new File(element.filePath);
                this.bytesRead += f.length();
                fi = new FileInputStream(f);

                this.openChannels++;

                ManagedFastInputChannel rf = new ManagedFastInputChannel();
                rf.mfChannel = fi.getChannel();
                rf.mPath = element.filePath;
                this.mvReadyFiles.add(rf);
                this.maFiles.add(element);
                iNumPaths++;
            } catch (Exception e) {
                while (this.mvReadyFiles.size() > 0) {
                    ManagedFastInputChannel fs = (ManagedFastInputChannel) this.mvReadyFiles.remove(0);
                    this.close(fs, NIOFileReader.OK_RECORD);
                }
                throw new Exception("Failed to open file: " + e.toString());
            }

        }

        return iNumPaths;
    }

    class FileToRead {

        String filePath;
        int paramListID;

        public FileToRead(String name, int paramListID) {
            super();
            this.filePath = name;
            this.paramListID = paramListID;
        }

    }

    private boolean getFiles() throws Exception {

        ArrayList files = new ArrayList();
        for (int i = 0; i < this.maParameters.size(); i++) {
            String[] fileNames = FileTools.getFilenames(this.getParameterValue(i, NIOFileReader.SEARCHPATH));

            if (fileNames != null) {
                for (String element : fileNames) {
                    files.add(new FileToRead(element, i));
                }

            }
        }

        if (files.size() == 0)
            return false;

        ArrayList partitionFileList = new ArrayList();

        for (int i = 0; i < files.size(); i++) {
            if (i % this.partitions == this.partitionID)
                partitionFileList.add(files.get(i));
        }

        FileToRead[] finalFileList = new FileToRead[partitionFileList.size()];
        partitionFileList.toArray(finalFileList);

        if (finalFileList.length > 0) {
            if (this.getFileChannels(finalFileList) <= 0) {
                return false;
            }
        }

        while (this.mCurrentFileChannel == null && this.mvReadyFiles.size() > 0)
            this.mCurrentFileChannel = this.getReader((ManagedFastInputChannel) this.mvReadyFiles.remove(0));

        return true;
    }

    private ArrayList completeFileList = null;

    public ArrayList getOpenFiles() {

        if (this.completeFileList == null) {
            ArrayList files = new ArrayList();
            for (int i = 0; i < this.maParameters.size(); i++) {
                String[] fileNames = FileTools.getFilenames(this.getParameterValue(i, NIOFileReader.SEARCHPATH));

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
    private ManagedFastInputChannel getReader(ManagedFastInputChannel file) throws Exception {

        try {
            CodingErrorAction action = CodingErrorAction.REPORT;
            if (this.mCodingErrorAction.equalsIgnoreCase(NIOFileReader.IGNORE_ACTION))
                action = CodingErrorAction.IGNORE;
            else if (this.mCodingErrorAction.equalsIgnoreCase(NIOFileReader.IGNORE_ACTION))
                action = CodingErrorAction.IGNORE;

            file.mReader = new FieldLevelFastInputChannel(file.mfChannel, "r", this.mIOBufferSize, this.mCharacterSet,
                    this.mZipped, action);

            if (this.mbAllowInvalidLastRecord) {
                file.mReader.allowForNoDelimeterAtEOF(true);
            }
            else
                file.mReader.allowForNoDelimeterAtEOF(false);

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Reading file " + file.mPath);
            try {
                for (int x = 0; x < this.miSkipLines; x++) {

                    for (ETLOutPort element : this.mOutPorts) {
                        SourceFieldDefinition sf = ((FileETLOutPort) element).sf;

                        int res;

                        do {
                            if (sf.FixedLength > 0) {
                                res = file.mReader.readFixedLengthField(sf.FixedLength, sf.getQuoteStartAsChars(), sf
                                        .getQuoteEndAsChars(), this.buf);
                            }
                            else {
                                res = file.mReader.readDelimitedField(sf.getDelimiterAsChars(), sf
                                        .getQuoteStartAsChars(), sf.getQuoteEndAsChars(), sf.mEscapeDoubleQuotes,
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
                ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Attempted to skip " + this.miSkipLines
                        + " records but end of file reached");
                this.close(file, NIOFileReader.OK_RECORD);
                file = null;
            }

        } catch (Exception e) {
            this.close(file, NIOFileReader.OK_RECORD);
            for (Object o : this.mvReadyFiles) {
                ManagedFastInputChannel fc = (ManagedFastInputChannel) o;
                this.close(fc, NIOFileReader.OK_RECORD);
            }
            throw new Exception("Failed to open file: " + e.toString());
        }

        return file;
    }

    /**
     * @return
     */
    public int getSkipLines() {
        return this.miSkipLines;
    }

    /**
     * @return
     */
    public SourceFieldDefinition[] getSourceFieldDefinition() {
        SourceFieldDefinition[] sf = new SourceFieldDefinition[this.mOutPorts.length];

        for (int i = 0; i < sf.length; i++) {
            sf[i] = ((FileETLOutPort) this.mOutPorts[i]).sf;
        }
        return sf;
    }

    protected Object handleEvent(int eventCode, int portIndex) throws IOException, KETLThreadException {
        switch (eventCode) {
        case FieldLevelFastInputChannel.END_OF_FILE:
            if (this.mCurrentFileChannel.mReader.isEndOfFile()) {
                this.close(this.mCurrentFileChannel, portIndex < this.mOutPorts.length - 1 ? NIOFileReader.PARTIAL_RECORD : NIOFileReader.OK_RECORD);
                return null;
            }
            else
                throw new KETLThreadException("Problem passing field", this);

        case FieldLevelFastInputChannel.BUFFER_TO_SMALL:
            // increase buffer size and try again
            if (this.buf.length > this.mMaxLineLength * 4) {
                throw new KETLThreadException("Field " + this.mOutPorts[portIndex].mstrName
                        + " length is greater than max line length of " + this.mMaxLineLength, this);
            }
            this.buf = new char[this.buf.length * 2];
            ResourcePool
                    .LogMessage(this, ResourcePool.INFO_MESSAGE, "Increased buffer size to allow for larger fields");
            return this.buf;
        default:
            throw new KETLThreadException("Result from field level parser unknown: " + eventCode, this);
        }
    }

    @Override
    public Object handleEventCode(int eventCode) {
        // TODO Auto-generated method stub
        return super.handleEventCode(eventCode);
    }

    @Override
    public Object handleException(Exception e) throws Exception {
        if (e instanceof EOFException) {
            this.close(this.mCurrentFileChannel, NIOFileReader.OK_RECORD);
            while (this.mvReadyFiles.size() > 0) {
                this.mCurrentFileChannel = this.getReader((ManagedFastInputChannel) this.mvReadyFiles.remove(0));
                if (this.mCurrentFileChannel != null)
                    return this.mCurrentFileChannel.mReader;
            }

            if (this.mDeleteSource)
                this.deleteFiles();
            else if (this.mMoveSource != null)
                this.moveFiles();

            return null;
        }
        return super.handleException(e);
    }

    @Override
    public Object handlePortEventCode(int eventCode, int portIndex) throws IOException, KETLThreadException {
        switch (eventCode) {
        case FieldLevelFastInputChannel.END_OF_FILE:
            if (this.mCurrentFileChannel.mReader.isEndOfFile()) {
                this.close(this.getCurrentFileChannel(),
                        portIndex < this.mOutPorts.length - 1 ? NIOFileReader.PARTIAL_RECORD : NIOFileReader.OK_RECORD);
                return null;
            }
            else
                throw new KETLThreadException("Problem passing field", this);

        case FieldLevelFastInputChannel.BUFFER_TO_SMALL:
            // increase buffer size and try again
            if (this.buf.length > this.mMaxLineLength * 4) {
                throw new KETLThreadException("Field " + this.mOutPorts[portIndex].mstrName
                        + " length is greater than max line length of " + this.mMaxLineLength, this);
            }
            this.buf = new char[this.buf.length * 2];
            ResourcePool
                    .LogMessage(this, ResourcePool.INFO_MESSAGE, "Increased buffer size to allow for larger fields");
            return this.buf;
        default:
            throw new KETLThreadException("Result from field level parser unknown: " + eventCode, this);
        }
    }

    @Override
    public Object handlePortException(Exception e, int portIndex) throws KETLThreadException {
        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Unexpected error reading file " + e.toString());
        if (this.mRecordCounter >= 0)
            throw new KETLThreadException("Check record " + this.mRecordCounter
                    + (portIndex >= 0 ? ", field " + (portIndex + 1) : ""), this);

        while (this.mvReadyFiles.size() > 0) {
            this.mCurrentFileChannel = (ManagedFastInputChannel) this.mvReadyFiles.remove(0);
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
     * arg1.setNull(arg0.DataType); arg1.setError(arg2.toString()); this.miErrorCount++; if (this.miErrorCount >
     * this.miErrorLimit) throw new KETLException("Error limit of " + this.miErrorLimit + " reached, last error: " +
     * arg2.toString()); }
     */
    /**
     * @return
     */
    public boolean ignoreLastRecord() {
        return this.mbAllowInvalidLastRecord;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param xmlSourceNode DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws KETLThreadException
     */
    @Override
    protected int initialize(Node xmlSourceNode) throws KETLThreadException {
        int res;

        if ((res = super.initialize(xmlSourceNode)) != 0) {
            return res;
        }

        if (this.maParameters == null) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                    "No complete parameter sets found, check that the following exist:\n" + this.getRequiredTagsMessage());

            return -2;
        }

        this.mAllowDuplicates = XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(),
                NIOFileReader.ALLOW_DUPLICATES_ATTRIBUTE, false);

        this.mCharacterSet = XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(), NIOFileReader.CHARACTERSET_ATTRIB,
                java.nio.charset.Charset.defaultCharset().name());
        this.mDeleteSource = XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(), NIOFileReader.DELETESOURCE_ATTRIB, false);
        this.mMoveSource = XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(), NIOFileReader.MOVESOURCE_ATTRIB, null);
        this.mCharacterSet = XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(), NIOFileReader.CHARACTERSET_ATTRIB, null);
        this.mCodingErrorAction = XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
                NIOFileReader.CODINGERRORACTION_ATTRIB, NIOFileReader.REPORT_ACTION);
        this.mZipped = XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(), NIOFileReader.ZIPPED, false);
        this.miSkipLines = XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), NIOFileReader.SKIP_LINES, 0);
        this.mstrDefaultRecordDelimter = XMLHelper.getAttributeAsString(xmlSourceNode.getAttributes(),
                NIOFileReader.RECORD_DELIMITER, NIOFileReader.DEFAULT_RECORD_DELIMITER);
        this.mbAllowInvalidLastRecord = (XMLHelper.getAttributeAsBoolean(xmlSourceNode.getAttributes(),
                NIOFileReader.ALLOW_INVALID_LAST_RECORD, NIOFileReader.DEFAULT_ALLOW_INVALID_LAST_RECORD));
        this.mIOBufferSize = XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), "IOBUFFER", 16384);
        this.mMaxLineLength = XMLHelper.getAttributeAsInt(xmlSourceNode.getAttributes(), "MAXLINELENGTH", 16384);

        this.mBufferLength = this.mMaxLineLength;
        for (int i = 0; i < this.mOutPorts.length; i++) {
            SourceFieldDefinition sf = ((FileETLOutPort) this.mOutPorts[i]).getSourceFieldDefinition();

            // seed the average field length
            if (sf.AverageLength == 0)
                sf.AverageLength = FieldLevelFastInputChannel.MAXFIELDLENGTH;
            // seed the max field length
            if (sf.MaxLength < 0)
                sf.MaxLength = this.mMaxLineLength;

            if (this.mBufferLength < ((sf.getQuoteStart() == null ? 0 : sf.getQuoteStartLength()) + sf.MaxLength + (sf
                    .getQuoteEnd() == null ? 0 : sf.getQuoteEndLength())))
                this.mBufferLength = ((sf.getQuoteStart() == null ? 0 : sf.getQuoteStartLength()) + sf.MaxLength + (sf
                        .getQuoteEnd() == null ? 0 : sf.getQuoteEndLength()));

            // for last record set delimeter
            if (i == this.mOutPorts.length - 1)
                sf.setDelimiter(this.mstrDefaultRecordDelimter);
        }

        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Initial max line length accounting for quotes: "
                + this.mBufferLength);
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
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Search path(s): " + searchPath);
                }

                throw new KETLThreadException("No files found, check search paths", this);
            }
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        // NOTE: Should return a declared constant not 0 or 1
        return 0;
    }

    private void moveFiles() {
        for (Object o : this.maFiles) {
            File fn = new File((String) o);

            // Destination directory
            File dir = new File(this.mMoveSource);

            // Move file to new directory
            if (fn.renameTo(new File(dir, fn.getName())) == false)
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Failed to move file: "
                        + fn.getAbsolutePath());
            else
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Moved file: " + fn.getAbsolutePath());
        }
    }

    @Override
    protected ETLOutPort getNewOutPort(com.kni.etl.ketl.ETLStep srcStep) {
        return new FileETLOutPort((ETLStep) this, (ETLStep) this);
    }

    public char[] getBuffer() {
        return this.buf;
    }

    @Override
    protected void close(boolean success) {

        if (this.mvReadyFiles == null)
            return;

        for (Object o : this.mvReadyFiles) {
            ManagedFastInputChannel rf = (ManagedFastInputChannel) o;
            try {
                rf.close();
            } catch (IOException e) {
                ResourcePool.LogException(e, this);
            }
        }

    }

}
