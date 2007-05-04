/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.DestinationFieldDefinition;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

/**
 * <p>
 * Title: JDBCWriter
 * </p>
 * <p>
 * Description: Writes a DataItem array to a JDBC datasource, based on ETLWriter
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * 
 * @author Nicholas Wakefield
 * @version 1.0
 */
public class NIOFileWriter extends ETLWriter implements DefaultWriterCore {

    public NIOFileWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    static final int DEFAULT_BUFFER_SIZE = 163840;
    private static String DEFAULT_VALUE = "DEFAULTVALUE";
    private static String DELIMITER = "DELIMITER";
    private static String DELIMITER_AT_END = "DELIMITER_AT_END";
    public static String CHARACTERSET_ATTRIB = "CHARACTERSET";
    private static String FIXEDWIDTH = "FIXEDWIDTH";
    private static String FORMAT_STRING = "FORMATSTRING";
    private static String ESCAPE_CHARACTER_STRING = "ESCAPECHARACTER";
    private static String IN = "IN";
    private static String ENABLE_LINEFEED = "ENABLE_LINEFEED";
    private static String LINEFEED_CHARACTER = "LINEFEED";
    private static String MAXIMUM_LENGTH = "MAXIMUMLENGTH";
    private static String NAME = "NAME";
    private static String WRITE_BUFFER = "WRITEBUFFER";
    private static String FILEPATH = "FILEPATH";
    private String mLinefeed;
    private boolean mbDelimiterAtEnd = false;
    private DestinationFieldDefinition[] mDestFieldDefinitions = null;
    private int miOutputBufferSize = 16384;
    private String msDefaultDelimiter = null;
    private int miDestFieldArrayLength = -1;
    private String mCharSet = null;
    String[] msRequiredTags = { NIOFileWriter.FILEPATH };

    ArrayList mWriterList = new ArrayList();
    OutputFile[] mWriters;
    private String msEscapeChar;

    @Override
    public int complete() throws KETLThreadException {
        // If there were no input rows, then we won't even have an upsert object...
        if (this.mWriters == null) {
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "No records processed.");

            return 0;
        }

        for (OutputFile element : this.mWriters)
            try {
                element.close();
            } catch (IOException e) {
                throw new KETLThreadException(e, this);
            }
        this.mWriters = null;

        return 0;
    }

    class OutputFile {

        FileOutputStream stream;
        FileChannel channel;
        Writer writer;

        void open(String filePath) throws FileNotFoundException {
            this.stream = new FileOutputStream(filePath);
            this.channel = this.stream.getChannel();
            CharsetEncoder charSet = (NIOFileWriter.this.mCharSet == null ? java.nio.charset.Charset.defaultCharset()
                    .newEncoder() : java.nio.charset.Charset.forName(NIOFileWriter.this.mCharSet).newEncoder());

            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Writing to file " + filePath
                    + ", character set " + charSet.charset().displayName());
            this.writer = java.nio.channels.Channels.newWriter(this.channel, charSet,
                    NIOFileWriter.this.miOutputBufferSize);

        }

        void close() throws IOException {
            this.writer.close();
            this.channel.close();
            this.stream.close();
        }
    }

    void createOutputFile(String filePath) throws FileNotFoundException {
        // add file streams to parallel stream parser
        OutputFile out = new OutputFile();
        out.open(filePath);
        this.mWriterList.add(out);
    }

    // Return 0 if success, otherwise error code...
    @Override
    public int initialize(Node xmlDestNode) throws KETLThreadException {
        int cd = super.initialize(xmlDestNode);

        if (cd != 0)
            return cd;

        // Get the attributes
        NamedNodeMap nmAttrs = xmlDestNode.getAttributes();
        this.mCharSet = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), NIOFileWriter.CHARACTERSET_ATTRIB,
                null);

        if (nmAttrs == null) {
            return 2;
        }

        if (this.maParameters == null) {
            throw new KETLThreadException("No complete parameter sets found, check that the following exist:\n"
                    + this.getRequiredTagsMessage(), this);
        }

        ArrayList files = new ArrayList();
        for (int i = 0; i < this.maParameters.size(); i++) {
            String filePath = this.getParameterValue(i, NIOFileWriter.FILEPATH);

            if (filePath != null) {
                files.add(filePath);
            }
        }

        try {
            for (int i = 0; i < files.size(); i++) {
                if (i % this.partitions == this.partitionID)
                    this.createOutputFile((String) files.get(i));
            }

            this.mWriters = new OutputFile[this.mWriterList.size()];
            this.mWriterList.toArray(this.mWriters);
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        // If this is our first call to upsert, we'll need to initialize it now that we know it's types...
        if (this.mWriters == null || files.size() == 0) {
            throw new KETLThreadException("No output files specified", this);
        }

        // build source file definition
        ArrayList destFieldDefinitions = new ArrayList();

        NodeList nl = xmlDestNode.getChildNodes();

        boolean AppendLineFeed = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(),
                NIOFileWriter.ENABLE_LINEFEED, true);
        this.mbDelimiterAtEnd = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(),
                NIOFileWriter.DELIMITER_AT_END, false);
        this.msDefaultDelimiter = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), NIOFileWriter.DELIMITER,
                "\t");
        this.msEscapeChar = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
                NIOFileWriter.ESCAPE_CHARACTER_STRING, "\\");

        String tmpLineFeed = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
                NIOFileWriter.LINEFEED_CHARACTER, "DOS");

        if (tmpLineFeed.compareTo("DOS") == 0) {
            this.mLinefeed = new String("\r\n");
        }
        else {
            this.mLinefeed = new String("\n");
        }

        this.miOutputBufferSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(), NIOFileWriter.WRITE_BUFFER,
                this.miOutputBufferSize);

        if (this.miOutputBufferSize == 0) {
            this.miOutputBufferSize = NIOFileWriter.DEFAULT_BUFFER_SIZE;
        }

        for (int i = 0; i < nl.getLength(); i++) {
            // See if we can match the name of the datasource...
            if (nl.item(i).getNodeName().compareTo(NIOFileWriter.IN) == 0) {
                DestinationFieldDefinition destFieldDefinition = new DestinationFieldDefinition(this.mCharSet);
                nmAttrs = nl.item(i).getAttributes();
                nmAttrs.getNamedItem(NIOFileWriter.NAME);

                destFieldDefinition.MaxLength = XMLHelper.getAttributeAsInt(nmAttrs, NIOFileWriter.MAXIMUM_LENGTH,
                        destFieldDefinition.MaxLength);
                destFieldDefinition.FixedWidth = XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileWriter.FIXEDWIDTH,
                        false);

                destFieldDefinition.Delimiter = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.DELIMITER,
                        this.msDefaultDelimiter);
                destFieldDefinition.FormatString = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.FORMAT_STRING,
                        destFieldDefinition.FormatString);
                destFieldDefinition.DefaultValue = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.DEFAULT_VALUE,
                        destFieldDefinition.DefaultValue);

                destFieldDefinitions.add(destFieldDefinition);
            }
        }

        // give file format configuration to parallel file reader.
        Object[] res = destFieldDefinitions.toArray();
        this.miDestFieldArrayLength = res.length;

        DestinationFieldDefinition[] sRes = new DestinationFieldDefinition[this.miDestFieldArrayLength];

        System.arraycopy(res, 0, sRes, 0, this.miDestFieldArrayLength);

        if (AppendLineFeed) {
            sRes[this.miDestFieldArrayLength - 1].AppendLineFeed = true;
        }

        if (this.mbDelimiterAtEnd == false) {
            sRes[this.miDestFieldArrayLength - 1].Delimiter = null;
        }

        this.mDestFieldDefinitions = sRes;

        // NOTE: Should return a declared constant not 0 or 1
        return 0;
    }

    StringBuilder sb = new StringBuilder();

    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {

        // Read record and write to output buffer.
        for (int i = 0; i < pRecordWidth; i++) {
            int pos = this.sb.length();
            // if record exists
            try {
                Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue()
                        : pInputRecords[this.mInPorts[i].getSourcePortIndex()];

                // if byte array is null then apply default value
                if (data == null) {
                    if (this.mDestFieldDefinitions[i].DefaultValue != null) {
                        this.sb.append(this.escape(this.mDestFieldDefinitions[i].DefaultValue,
                                this.mDestFieldDefinitions[i].Delimiter, !this.mDestFieldDefinitions[i].FixedWidth));
                    }
                }
                else {
                    if (this.mDestFieldDefinitions[i].FormatString != null) {
                        int idx = this.mInPorts[i].getSourcePortIndex();
                        this.sb.append(this.escape(this.mDestFieldDefinitions[i].getFormat(pExpectedDataTypes[idx])
                                .format(data), this.mDestFieldDefinitions[i].Delimiter,
                                !this.mDestFieldDefinitions[i].FixedWidth));
                    }
                    else
                        this.sb.append(this.escape(data.toString(), this.mDestFieldDefinitions[i].Delimiter,
                                !this.mDestFieldDefinitions[i].FixedWidth));
                }

                // get max length of item to place inoutput buffer
                if ((this.mDestFieldDefinitions[i].MaxLength != -1)
                        && (this.sb.length() - pos > this.mDestFieldDefinitions[i].MaxLength)) {
                    this.sb.setLength(pos + this.mDestFieldDefinitions[i].MaxLength);
                }
                else if (this.mDestFieldDefinitions[i].FixedWidth) {
                    int rem = (pos + this.mDestFieldDefinitions[i].MaxLength) - this.sb.length();
                    while (rem > 0) {
                        this.sb.append(' ');
                        rem--;
                    }
                }

                // write delimiter to main buffer
                if (this.mDestFieldDefinitions[i].Delimiter != null) {
                    this.sb.append(this.mDestFieldDefinitions[i].Delimiter);
                }

                // write linefeed is required
                if (this.mDestFieldDefinitions[i].AppendLineFeed) {
                    this.sb.append(this.mLinefeed);
                }
            } catch (Exception e1) {
                throw new KETLWriteException(e1);
            }
        }

        // all done write to streams
        for (int i = this.mWriters.length - 1; i >= 0; i--) {
            try {
                this.mWriters[i].writer.append(this.sb.toString());
            } catch (IOException e) {
                throw new KETLWriteException(e);
            }
        }

        this.sb.setLength(0);
        return this.mWriters.length;
    }

    private CharSequence escape(String datum, String del, boolean hasDelimeter) {

        if (hasDelimeter && datum != null && del != null) {
            if (datum.contains(del))
                return datum.replace(del, this.msEscapeChar + del);
        }
        return datum;
    }

    @Override
    protected void close(boolean success) {
        if (this.mWriters == null)
            return;

        for (int i = this.mWriters.length - 1; i >= 0; i--) {
            try {
                this.mWriters[i].close();
            } catch (IOException e) {
                ResourcePool.LogException(e, this);
            }
        }

    }

}
