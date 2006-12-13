/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on May 10, 2006
 * 
 */
package com.kni.etl.ketl_v1;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.DataItemHelper;
import com.kni.etl.EngineConstants;
import com.kni.etl.FieldLevelFastInputChannel;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.ketl.reader.FileReader;
import com.kni.etl.ketl.reader.NIOFileReader;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class ParallelInlineSortFileReader {

    private char[] buf;
    private int bytesRead = 0;
    private int[] aCurrentResultRecordsStatus;
    private ResultRecord aCurrentResultRecords[];
    private ArrayList maFiles = new ArrayList();
    private boolean mAllowDuplicates = false;
    private boolean mbIgnoreLastRecord;
    private char mcDefaultRecordDelimter;
    private String mCharacterSet, mCodingErrorAction;
    private boolean mDeleteSource = false;
    private int miSourceFieldDefinitionsLength;
    private int mIOBufferSize;
    private int miSkipLines;
    private int mMaxLineLength;
    private String mMoveSource = null;
    private int mSamplingRate;
    private SourceFieldDefinition[] msfDefinition;
    String[] msRequiredTags = { FileReader.SEARCHPATH };
    private String mstrDefaultRecordDelimter;
    private long miRecordCount = 0;
    private Date startDate;
    private boolean mbSamplingEnabled = false,mZipped = false;
    private boolean mbSortRequired = false;

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

    public static final SourceFieldDefinition[] getSourceFieldDefinitions(Object o, NodeList nlOut) {
        NamedNodeMap nmAttrs;
        ArrayList srcFieldDefinitions = new ArrayList();
        String mstrDefaultFieldDelimeter = null;

        for (int i = 0; i < nlOut.getLength(); i++) {
            // See if we can match the name of the datasource...
            if (nlOut.item(i).getNodeName().compareTo(FileReader.OUT_TAG) == 0) {
                SourceFieldDefinition srcFieldDefinition = new SourceFieldDefinition();
                nmAttrs = nlOut.item(i).getAttributes();
                nmAttrs.getNamedItem(FileReader.NAME);

                if (mstrDefaultFieldDelimeter == null) {
                    mstrDefaultFieldDelimeter = XMLHelper.getAttributeAsString(nlOut.item(i).getParentNode()
                            .getAttributes(), FileReader.DELIMITER, null);
                }

                if (mstrDefaultFieldDelimeter == null) {
                    ResourcePool.LogMessage(o,ResourcePool.INFO_MESSAGE,
                            "FileReader: No default delimiter has been specified, system default field delimiter '"
                                    + FileReader.DEFAULT_FIELD_DELIMITER
                                    + "' will be used for fields without delimiters specified.");
                    mstrDefaultFieldDelimeter = FileReader.DEFAULT_FIELD_DELIMITER;
                }

                srcFieldDefinition.MaxLength = XMLHelper.getAttributeAsInt(nmAttrs, FileReader.MAXIMUM_LENGTH,
                        srcFieldDefinition.MaxLength);
                srcFieldDefinition.FixedLength = XMLHelper.getAttributeAsInt(nmAttrs, FileReader.FIXED_LENGTH,
                        srcFieldDefinition.FixedLength);
                srcFieldDefinition.ReadOrder = EngineConstants.resolveValueFromConstant(XMLHelper.getAttributeAsString(
                        nmAttrs, FileReader.READ_ORDER, Integer.toString(srcFieldDefinition.ReadOrder)),
                        srcFieldDefinition.ReadOrder);
                srcFieldDefinition.ReadOrderSequence = XMLHelper.getAttributeAsInt(nmAttrs, FileReader.READ_ORDER_SEQUENCE,
                        srcFieldDefinition.ReadOrderSequence);
                
                srcFieldDefinition.AutoTruncate = XMLHelper.getAttributeAsBoolean(nmAttrs, FileReader.AUTOTRUNCATE,
                        srcFieldDefinition.AutoTruncate);

                srcFieldDefinition.setDelimiter(XMLHelper.getAttributeAsString(nmAttrs, FileReader.DELIMITER,
                        mstrDefaultFieldDelimeter));

                srcFieldDefinition.setEscapeCharacter(XMLHelper.getAttributeAsString(nmAttrs, FileReader.ESCAPE_CHAR, null));
                srcFieldDefinition.setEscapeDoubleQuotes(XMLHelper.getAttributeAsBoolean(nmAttrs, FileReader.ESCAPE_DOUBLEQUOTES, false));

                srcFieldDefinition.setNullIf(XMLHelper.getAttributeAsString(nmAttrs, FileReader.NULLIF, null));

                srcFieldDefinition.setQuoteStart(XMLHelper.getAttributeAsString(nmAttrs, FileReader.QUOTESTART, srcFieldDefinition
                        .getQuoteStart()));
                srcFieldDefinition.setQuoteEnd(XMLHelper.getAttributeAsString(nmAttrs, FileReader.QUOTEEND, srcFieldDefinition
                        .getQuoteEnd()));

                srcFieldDefinition.FormatString = XMLHelper.getAttributeAsString(nmAttrs, FileReader.FORMAT_STRING,
                        srcFieldDefinition.FormatString);
                srcFieldDefinition.DefaultValue = XMLHelper.getAttributeAsString(nmAttrs, FileReader.DEFAULT_VALUE,
                        srcFieldDefinition.DefaultValue);

                srcFieldDefinition.PartitionField = XMLHelper.getAttributeAsBoolean(nmAttrs, FileReader.PARTITION_KEY, false);

                srcFieldDefinition.ObjectType = EngineConstants.resolveObjectNameToID(XMLHelper.getAttributeAsString(
                        nmAttrs, "OBJECTTYPE", null));

                srcFieldDefinition.DataType = DataItemHelper.getDataTypeIDbyName(XMLHelper.getAttributeAsString(
                        nmAttrs, FileReader.DATATYPE, null));

                String trimStr = XMLHelper.getAttributeAsString(nmAttrs, FileReader.TRIM, "FALSE");

                if ((trimStr != null) && trimStr.equalsIgnoreCase("TRUE")) {
                    srcFieldDefinition.TrimValue = true;
                }

                srcFieldDefinitions.add(srcFieldDefinition);
            }
        }

        // give file format configuration to parallel file reader.
        Object[] res = srcFieldDefinitions.toArray();
        SourceFieldDefinition[] msfDefintion = new SourceFieldDefinition[res.length];

        System.arraycopy(res, 0, msfDefintion, 0, res.length);

        return msfDefintion;
    }
    
    /**
     * 
     */
    public ParallelInlineSortFileReader(Node pConfigNode, NodeList pOuts) throws Exception {
        super();

        this.mAllowDuplicates = XMLHelper.getAttributeAsBoolean(pConfigNode.getAttributes(),
                NIOFileReader.ALLOW_DUPLICATES_ATTRIBUTE, false);
        this.mSamplingRate = XMLHelper.getAttributeAsInt(pConfigNode.getAttributes(),
                NIOFileReader.SAMPLE_EVERY_ATTRIBUTE, -1);

        this.mZipped = XMLHelper.getAttributeAsBoolean(pConfigNode.getAttributes(),
                NIOFileReader.ZIPPED, false);

        this.mbSamplingEnabled = this.mSamplingRate > 1;

        this.mCharacterSet = XMLHelper.getAttributeAsString(pConfigNode.getAttributes(),
                NIOFileReader.CHARACTERSET_ATTRIB, java.nio.charset.Charset.defaultCharset().name());
        this.mDeleteSource = XMLHelper.getAttributeAsBoolean(pConfigNode.getAttributes(),
                NIOFileReader.DELETESOURCE_ATTRIB, false);
        this.mMoveSource = XMLHelper.getAttributeAsString(pConfigNode.getAttributes(), NIOFileReader.MOVESOURCE_ATTRIB,
                null);
        this.mCharacterSet = XMLHelper.getAttributeAsString(pConfigNode.getAttributes(),
                NIOFileReader.CHARACTERSET_ATTRIB, null);
        this.mCodingErrorAction = XMLHelper.getAttributeAsString(pConfigNode.getAttributes(),
                NIOFileReader.CODINGERRORACTION_ATTRIB, NIOFileReader.REPORT_ACTION);
        this.miSkipLines = XMLHelper.getAttributeAsInt(pConfigNode.getAttributes(), NIOFileReader.SKIP_LINES, 0);
        this.mstrDefaultRecordDelimter = XMLHelper.getAttributeAsString(pConfigNode.getAttributes(),
                NIOFileReader.RECORD_DELIMITER, NIOFileReader.DEFAULT_RECORD_DELIMITER);
        this.mbIgnoreLastRecord = (XMLHelper.getAttributeAsBoolean(pConfigNode.getAttributes(),
                NIOFileReader.ALLOW_INVALID_LAST_RECORD, NIOFileReader.DEFAULT_ALLOW_INVALID_LAST_RECORD));
        this.mIOBufferSize = XMLHelper.getAttributeAsInt(pConfigNode.getAttributes(), "IOBUFFER", 16384);
        this.mMaxLineLength = XMLHelper.getAttributeAsInt(pConfigNode.getAttributes(), "MAXLINELENGTH", 16384);

        this.msfDefinition = getSourceFieldDefinitions(this, pOuts);
        if (this.msfDefinition == null || this.msfDefinition.length == 0) {
            throw new Exception("ERROR: Outs must be specified to read source");
        }

        this.miSourceFieldDefinitionsLength = this.msfDefinition.length;

        int len = this.mMaxLineLength;
        for (int i = 0; i < this.miSourceFieldDefinitionsLength; i++) {
            SourceFieldDefinition sf = this.msfDefinition[i];

            // seed the average field length
            if (sf.AverageLength == 0)
                sf.AverageLength = FieldLevelFastInputChannel.MAXFIELDLENGTH;
            // seed the max field length
            if (sf.MaxLength < 0)
                sf.MaxLength = this.mMaxLineLength;

            if (len < ((sf.getQuoteStart() == null ? 0 : sf.getQuoteStartLength()) + sf.MaxLength + (sf.getQuoteEnd() == null ? 0
                    : sf.getQuoteEndLength())))
                len = ((sf.getQuoteStart() == null ? 0 : sf.getQuoteStartLength()) + sf.MaxLength + (sf.getQuoteEnd() == null ? 0
                        : sf.getQuoteEndLength()));

            if (sf.ReadOrderSequence != -1)
                this.mbSortRequired = true;

            // for last record set delimeter
            if (i == this.miSourceFieldDefinitionsLength - 1)
                sf.setDelimiter(this.mstrDefaultRecordDelimter);
        }

        if (this.mbSortRequired) {
            mResultRecordComparator = this.writeCompareClassToFile();
        }
        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Initial max line length accounting for quotes: " + len);
        this.buf = new char[len];

        for (int i = 0; i < this.miSourceFieldDefinitionsLength; i++) {
            SourceFieldDefinition sf = this.msfDefinition[i];

            if (sf.DataType == BaseDataItem.DATE) {
                sf.DateFormatter = new FastSimpleDateFormat(sf.FormatString);
                sf.position = new ParsePosition(0);
            }

        }
    }

    public void addFiles(List pFiles) {
        this.maFiles.addAll(pFiles);
    }

    private class ReadyFile {

        FileChannel mfChannel;
        String mPath;
        int mRecordPos = 0;
        FieldLevelFastInputChannel mReader;

        void close() throws IOException {
            mfChannel.close();
            if (this.mReader != null)
                this.mReader.close();
        }

        public String toString() {
            return "File: " + mPath + " Record: " + mRecordPos;
        }
    }

    private static final int NO_FIELD = -1;
    private static final int OK_RECORD = 0;
    private static final int PARTIAL_RECORD = -1;

    ReadyFile[] mCurrentFileChannels;
    private int miSampleCounter = 0;
    private int miTotalFilesCnt = -1;

    private void openFiles() throws Exception {
        // add file streams to channel reader
        if (this.mAllowDuplicates == false) {
            this.maFiles = NIOFileReader.dedupFileList(this.maFiles);
        }

        mCurrentFileChannels = new ReadyFile[this.maFiles.size()];
        int pos = 0;
        for (Object o : this.maFiles) {
            String astrPath = (String) o;
            FileInputStream fi;

            try {
                File f = new File(astrPath);
                this.bytesRead = 0;
                fi = new FileInputStream(f);

                ReadyFile rf = new ReadyFile();
                rf.mfChannel = fi.getChannel();
                rf.mPath = astrPath;
                this.mCurrentFileChannels[pos++] = rf;
            } catch (Exception e) {
                for (int i = 0; i < this.mCurrentFileChannels.length; i++) {
                    this.close(i, OK_RECORD);
                }
                throw new Exception("Failed to open file: " + e.toString());
            }

        }

        // open files
        for (int i = 0; i < this.mCurrentFileChannels.length; i++) {
            this.getReader(i);
        }

        this.miTotalFilesCnt = this.mCurrentFileChannels.length;
        this.aCurrentResultRecords = new ResultRecord[this.miTotalFilesCnt];
        this.aCurrentResultRecordsStatus = new int[this.miTotalFilesCnt];

    }

    // Returns the number of actually opened paths...
    void getReader(int pos) throws Exception {

        ReadyFile file = this.mCurrentFileChannels[pos];

        try {
            CodingErrorAction action = CodingErrorAction.REPORT;
            if (this.mCodingErrorAction.equalsIgnoreCase(NIOFileReader.IGNORE_ACTION))
                action = CodingErrorAction.IGNORE;
            else if (this.mCodingErrorAction.equalsIgnoreCase(NIOFileReader.IGNORE_ACTION))
                action = CodingErrorAction.IGNORE;

            file.mReader = new FieldLevelFastInputChannel(file.mfChannel, "r", mIOBufferSize, this.mCharacterSet,this.mZipped,
                    action);

            if (this.mbIgnoreLastRecord == false) {
                file.mReader.allowForNoDelimeterAtEOF(true);
            }
            else
                file.mReader.allowForNoDelimeterAtEOF(false);

            ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Reading file " + file.mPath);
            try {
                file.mRecordPos = 1;
                for (int x = 0; x < this.miSkipLines; x++) {

                    for (int i = 0; i < this.miSourceFieldDefinitionsLength; i++) {
                        SourceFieldDefinition sf = this.msfDefinition[i];

                        int res;
                        if (sf.FixedLength > 0) {
                            res = file.mReader.readFixedLengthField(sf.FixedLength, sf.getQuoteStartAsChars(), sf
                                    .getQuoteEndAsChars(), buf);
                        }
                        else {
                            res = file.mReader.readDelimitedField(sf.getDelimiterAsChars(), sf.getQuoteStartAsChars(),
                                    sf.getQuoteEndAsChars(), sf.mEscapeDoubleQuotes, sf.escapeChar, sf.MaxLength,
                                    sf.AverageLength, buf, sf.AutoTruncate);
                        }

                        switch (res) {
                        case FieldLevelFastInputChannel.END_OF_FILE:
                            if (file.mReader.isEndOfFile()) {
                                this.close(pos, OK_RECORD);
                                file = null;
                            }
                            break;
                        case FieldLevelFastInputChannel.BUFFER_TO_SMALL:
                            // increase buffer size and try again
                            if (buf.length > this.mMaxLineLength * 4) {
                                throw new Exception(file + " Field " + i
                                        + " length is greater than max line length of " + this.mMaxLineLength);
                            }
                            this.buf = new char[buf.length * 2];
                            ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Increased buffer size to allow for larger fields");
                            i--;
                            break;
                        default:
                            this.bytesRead += res;
                        }

                    }

                    file.mRecordPos++;
                }
            } catch (EOFException e) {
                ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Attempted to skip " + this.miSkipLines
                        + " records but end of file reached");
                this.close(pos, OK_RECORD);
                file = null;
            }

        } catch (Exception e) {
            for (int i = 0; i < this.mCurrentFileChannels.length; i++) {
                this.close(i, OK_RECORD);
            }
            throw new Exception("Failed to open file: " + e.toString());
        }

    }

    int miPrevious = 0;

    final public ResultRecord getNextLine() throws Exception {

        this.getNext();

        if (this.mbSortRequired)
            return this.sortAndRetrieve();

        for (int i = 0; i < this.miTotalFilesCnt; i++) {
            if (aCurrentResultRecordsStatus[i] == 1) {
                aCurrentResultRecordsStatus[i] = 0;
                return this.aCurrentResultRecords[i];
            }
        }

        return null;
    }

    final private void getNext() throws Exception {

        int i = -1;

        if (miTotalFilesCnt == -1) {
            this.openFiles();
        }

        for (int fileID = 0; fileID < this.miTotalFilesCnt; fileID++) {
            nextFile: {
                if (aCurrentResultRecordsStatus[fileID] == 0) {
                    ResultRecord pResultRecord = new ResultRecord(this.miSourceFieldDefinitionsLength);

                    ReadyFile currentFileChannel = this.mCurrentFileChannels[fileID];
                    try {
                        try {
                            boolean passRecord;
                            // sampling loop, only will cycle once if not in sampling mode
                            do {
                                // disable passing of data if skipping record for sampling
                                passRecord = (this.mbSamplingEnabled == false || (this.miSampleCounter
                                        % this.mSamplingRate == 0));

                                for (i = 0; i < this.miSourceFieldDefinitionsLength; i++) {

                                    // get current source field definition
                                    SourceFieldDefinition sf = this.msfDefinition[i];

                                    // get data item to be written to
                                    DataItem fld = pResultRecord.LineFields[i];
                                    fld.ObjectType = sf.ObjectType;

                                    // if fixed length then grab fixed length field else use delimiter
                                    int res = sf.FixedLength > 0 ? currentFileChannel.mReader.readFixedLengthField(
                                            sf.FixedLength, sf.getQuoteStartAsChars(), sf.getQuoteEndAsChars(), buf)
                                            : currentFileChannel.mReader.readDelimitedField(sf.getDelimiterAsChars(),
                                                    sf.getQuoteStartAsChars(), sf.getQuoteEndAsChars(),
                                                    sf.mEscapeDoubleQuotes, sf.escapeChar, sf.MaxLength,
                                                    sf.AverageLength, buf, sf.AutoTruncate);

                                    // if res < 0 then handle
                                    switch (res) {
                                    case FieldLevelFastInputChannel.END_OF_FILE:
                                        if (currentFileChannel.mReader.isEndOfFile()) {
                                            this.close(fileID,
                                                    i < this.miSourceFieldDefinitionsLength - 1 ? PARTIAL_RECORD
                                                            : OK_RECORD);
                                            break nextFile;
                                        }

                                        throw new Exception("Problem passing field");

                                    case FieldLevelFastInputChannel.BUFFER_TO_SMALL:
                                        // increase buffer size and try again
                                        if (buf.length > this.mMaxLineLength * 4) {
                                            throw new Exception(this.mCurrentFileChannels[fileID] + " Field " + i
                                                    + " length is greater than max line length of "
                                                    + this.mMaxLineLength);
                                        }
                                        this.buf = new char[buf.length * 2];
                                        ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE,
                                                "Increased buffer size to allow for larger fields");
                                        i--;
                                        break;
                                    default:

                                        if (res >= 0 && passRecord) {

                                            this.bytesRead += res;

                                            // trim if over max length
                                            if (sf.MaxLength > -1 && res > sf.MaxLength) {
                                                res = sf.MaxLength;
                                            }

                                            boolean nullValue = false;

                                            // if null if check requested for field to check
                                            if (sf.NullIf != null
                                                    && charArrayEquals(buf, res, sf.NullIfCharArray,
                                                            sf.NullIfCharArray.length)) {
                                                nullValue = true;
                                            }

                                            boolean useDefaultString = false;
                                            if (sf.DefaultValue != null && (res == 0 || nullValue)) {
                                                if (sf.DataType == BaseDataItem.STRING)
                                                    useDefaultString = true;
                                                else
                                                    sf.DefaultValue.getChars(0, sf.DefaultValue.length(), buf, 0);
                                                res = sf.DefaultValue.length();
                                            }

                                            switch (sf.DataType) {
                                            case BaseDataItem.STRING:
                                                // strings can have 0 length, don't set null on 0 length
                                                if (nullValue)
                                                    fld.setNull(sf.DataType);
                                                else if (useDefaultString)
                                                    fld.setString(sf.DefaultValue);
                                                else {
                                                    String tmp = FieldLevelFastInputChannel.toString(buf, res);
                                                    if (sf.TrimValue)
                                                        tmp = tmp.trim();
                                                    fld.setString(tmp);
                                                }
                                                break;
                                            case BaseDataItem.INTEGER:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    try {
                                                        fld.setInt(FieldLevelFastInputChannel.toInteger(buf, res));
                                                    } catch (Exception e) {
                                                        handleError(fld, sf, e);
                                                    }
                                                break;
                                            case BaseDataItem.LONG:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    try {
                                                        fld.setLong(FieldLevelFastInputChannel.toLong(buf, res));
                                                    } catch (Exception e) {
                                                        handleError(fld, sf, e);
                                                    }
                                                break;
                                            case BaseDataItem.FLOAT:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    try {
                                                        fld.setFloat(FieldLevelFastInputChannel.toFloat(buf, res));
                                                    } catch (Exception e) {
                                                        handleError(fld, sf, e);
                                                    }
                                                break;
                                            case BaseDataItem.DATE:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else {
                                                    java.util.Date dt = FieldLevelFastInputChannel.toDate(buf, res,
                                                            sf.DateFormatter, sf.position);
                                                    if (dt == null)
                                                        handleError(fld, sf, new KETLException(
                                                                "Failed to parse date, pattern expected "
                                                                        + sf.DateFormatter.toPattern() + " value was "
                                                                        + new String(buf, 0, res)));
                                                    else
                                                        fld.setDate(dt);
                                                }
                                                sf.position.setIndex(0);
                                                break;
                                            case BaseDataItem.DOUBLE:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    try {
                                                        fld.setDouble(FieldLevelFastInputChannel.toDouble(buf, res));
                                                    } catch (Exception e) {
                                                        handleError(fld, sf, e);
                                                    }
                                                break;
                                            case BaseDataItem.CHAR:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    fld.setChar(FieldLevelFastInputChannel.toChar(buf, res));
                                                break;
                                            case BaseDataItem.CHARARRAY:
                                                // chararrays can have 0 length, don't set null on 0 length
                                                if (nullValue)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    fld.setCharArray(FieldLevelFastInputChannel.toCharArray(buf, res));
                                                break;
                                            case BaseDataItem.BOOLEAN:
                                                if (nullValue || res == 0)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    fld.setBoolean(FieldLevelFastInputChannel.toBoolean(buf, res));
                                                break;
                                            case BaseDataItem.BYTEARRAY:
                                                // bytearrays can have 0 length, don't set null on 0 length
                                                if (nullValue)
                                                    fld.setNull(sf.DataType);
                                                else
                                                    fld.setByteArray(FieldLevelFastInputChannel.toByteArray(buf, res));
                                                break;
                                            }

                                        }
                                        else if (passRecord) // only throw exception if record was meant to be passed
                                            throw new Exception("Result from field level parser unknown: " + res);
                                    }
                                }

                                this.miSampleCounter++;
                            } while (passRecord == false);

                            aCurrentResultRecordsStatus[fileID] = 1;
                            this.aCurrentResultRecords[fileID] = pResultRecord;
                            this.mCurrentFileChannels[fileID].mRecordPos++;
                        } catch (EOFException e) {
                            this.close(fileID, OK_RECORD);

                            if (this.mDeleteSource)
                                this.deleteFiles();
                            else if (this.mMoveSource != null)
                                this.moveFiles();

                            break nextFile;
                        }
                    } catch (Exception e) {
                        ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, "Unexpected error reading file " + e.toString());
                        if (this.mCurrentFileChannels[fileID].mRecordPos > 0)
                            ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, this.mCurrentFileChannels[fileID]
                                    + (i >= 0 ? ", field " + (i + 1) : ""));

                        for (int x = 0; x < mCurrentFileChannels.length; x++) {
                            try {
                                this.close(x, OK_RECORD);
                            } catch (IOException e1) {
                                ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, "Could not close file channel - " + e.toString());
                            }
                        }

                        throw e;
                    }
                }
            }
        }
    }

    private void deleteFiles() {
        for (Object o : this.maFiles) {
            File fn = new File((String) o);

            if (fn.exists()) {
                if (fn.delete()) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Deleted file: " + fn.getAbsolutePath());
                }
                else
                    ResourcePool.LogMessage(this,  ResourcePool.ERROR_MESSAGE, "Failed to delete file: " + fn.getAbsolutePath());
            }
        }
    }

    private void moveFiles() {
        for (Object o : this.maFiles) {
            File fn = new File((String) o);

            // Destination directory
            File dir = new File(this.mMoveSource);

            // Move file to new directory
            if (fn.renameTo(new File(dir, fn.getName())) == false)
                ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, "Failed to move file: " + fn.getAbsolutePath());
            else
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Moved file: " + fn.getAbsolutePath());
        }
    }

    public int getBytesRead() {
        return this.bytesRead;
    }

    private Comparator writeCompareClassToFile() throws Exception {

        ArrayList ls = new ArrayList();

        for (int fi = 0; fi < miSourceFieldDefinitionsLength; fi++) {
            if (msfDefinition[fi].ReadOrderSequence > 0) {
                ls.add(fi);
            }
        }

        Collections.sort(ls);
        Integer[] SortIndex = new Integer[ls.size()];
        ls.toArray(SortIndex);

        File nf = new File("CompareRecord" + Thread.currentThread().getId() + ".java");
        FileWriter fr = new FileWriter(nf);

        StringBuilder sb = new StringBuilder(
                "import com.kni.etl.ResultRecord;\nimport java.util.Comparator;\nfinal public class CompareRecord"
                        + Thread.currentThread().getId() + " implements Comparator{");

        sb
                .append("final public int compare(Object p0, Object p1) { ResultRecord left = (ResultRecord) p0; ResultRecord right = (ResultRecord) p1;\nint res;\nreturn ");

        for (int i = 0; i < SortIndex.length; i++) {

            if (i < (SortIndex.length - 1)) {
                sb.append("((res =");
            }

            if (this.msfDefinition[SortIndex[i]].ReadOrder == 0) {
                sb.append("left.LineFields[" + SortIndex[i] + "].compare(right.LineFields[" + SortIndex[i] + "])");
            }
            else {
                sb.append("right.LineFields[" + SortIndex[i] + "].compare(left.LineFields[" + SortIndex[i] + "])");
            }

            if (i < (SortIndex.length - 1)) {
                sb.append(") == 0) ?");
            }
        }

        for (int i = 0; i < (SortIndex.length - 1); i++) {
            sb.append(":res ");
        }

        sb.append(";\n}}");

        fr.write(sb.toString());
        fr.close();

        String[] source = { nf.getAbsolutePath() };
        StringWriter st = new StringWriter();
        PrintWriter out = new PrintWriter(st);
        int iReturnValue = com.sun.tools.javac.Main.compile(source, out);

        if (iReturnValue != 0) {
            throw new Exception("Compilation error, see below\n" + st.toString());
        }

        FileClassLoader n = new FileClassLoader();

        Class cl = n.getClass("CompareRecord" + Thread.currentThread().getId() + ".class", "CompareRecord"
                + Thread.currentThread().getId());

        return (Comparator) cl.newInstance();
    }

    final private ResultRecord sortAndRetrieve() {
        // Find record to return
        int left = -1;
        for (int i = 0; i < this.miTotalFilesCnt; i++) {
            if (aCurrentResultRecordsStatus[i] == 1) {
                if (left == -1)
                    left = i;
                else if (this.mResultRecordComparator.compare(this.aCurrentResultRecords[left],
                        this.aCurrentResultRecords[i]) < 0)
                    left = i;
            }
        }

        if (left == -1)
            return null;

        this.aCurrentResultRecordsStatus[left] = 0;
        return this.aCurrentResultRecords[left];
    }

    final private void close(int pos, int pCause) throws IOException {
        switch (pCause) {
        case PARTIAL_RECORD:
            ResourcePool.LogMessage(this,ResourcePool.WARNING_MESSAGE, "Partial record at end of file");
            break;
        }

        if (this.mCurrentFileChannels[pos] == null)
            return;

        this.mCurrentFileChannels[pos].close();
        ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Closing file " + this.mCurrentFileChannels[pos].mPath);
        this.mCurrentFileChannels[pos] = null;
        this.aCurrentResultRecordsStatus[pos] = -1;

    }

    private int miErrorCount = 0;
    private int miErrorLimit = 0;
    private Comparator mResultRecordComparator;

    private void handleError(DataItem arg1, SourceFieldDefinition arg0, Exception arg2) throws KETLException {
        arg1.setNull(arg0.DataType);
        arg1.setError(arg2.toString());
        this.miErrorCount++;
        if (this.miErrorCount > this.miErrorLimit)
            throw new KETLException("Error limit of " + this.miErrorLimit + " reached, last error: " + arg2.toString());
    }

}
