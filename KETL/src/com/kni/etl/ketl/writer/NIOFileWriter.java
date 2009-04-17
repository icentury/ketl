/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.writer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

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

public class NIOFileWriter extends ETLWriter implements DefaultWriterCore {

	/**
	 * Instantiates a new NIO file writer.
	 * 
	 * @param pXMLConfig
	 *            the XML config
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public NIOFileWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
			throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/** The Constant DEFAULT_BUFFER_SIZE. */
	static final int DEFAULT_BUFFER_SIZE = 65536;

	static final String SKIP = "SKIP";

	static final String FILE_NAME = "FILENAME";

	static final String SUB_PARTITION = "SUBPARTITION";

	static final String FILENAME_FORMAT = "FILENAMEFORMAT";

	static final String FILEPATH_FORMAT = "FILEPATHFORMAT";

	private static final String QUOTES = "QUOTE";

	private static final String ESCAPE_WHEN_QUOTED = "ESCAPEWHENQUOTED";

	static final String ZIP_ATTRIB = "ZIP";

	/** The DEFAUL t_ VALUE. */
	private static String DEFAULT_VALUE = "DEFAULTVALUE";

	/** The DELIMITER. */
	private static String DELIMITER = "DELIMITER";

	/** The DELIMITE r_ A t_ END. */
	private static String DELIMITER_AT_END = "DELIMITER_AT_END";

	/** The CHARACTERSE t_ ATTRIB. */
	public static String CHARACTERSET_ATTRIB = "CHARACTERSET";

	/** The FIXEDWIDTH. */
	private static String FIXEDWIDTH = "FIXEDWIDTH";

	/** The FORMA t_ STRING. */
	private static String FORMAT_STRING = "FORMATSTRING";

	/** The ESCAP e_ CHARACTE r_ STRING. */
	private static String ESCAPE_CHARACTER_STRING = "ESCAPECHARACTER";

	/** The IN. */
	private static String IN = "IN";

	/** The ENABL e_ LINEFEED. */
	private static String ENABLE_LINEFEED = "ENABLE_LINEFEED";

	/** The LINEFEE d_ CHARACTER. */
	private static String LINEFEED_CHARACTER = "LINEFEED";

	/** The MAXIMU m_ LENGTH. */
	private static String MAXIMUM_LENGTH = "MAXIMUMLENGTH";

	/** The NAME. */
	private static String NAME = "NAME";

	/** The WRIT e_ BUFFER. */
	private static String WRITE_BUFFER = "WRITEBUFFER";

	/** The FILEPATH. */
	static String FILEPATH = "FILEPATH";

	/** The linefeed. */
	private String mLinefeed;

	/** The mb delimiter at end. */
	private boolean mbDelimiterAtEnd = false;

	/** The dest field definitions. */
	private DestinationFieldDefinition[] mDestFieldDefinitions = null;

	/** The mi output buffer size. */
	int miOutputBufferSize = 65536;

	/** The ms default delimiter. */
	private String msDefaultDelimiter = null;

	/** The mi dest field array length. */
	private int miDestFieldArrayLength = -1;

	/** The char set. */
	String mCharSet = null;

	/** The ms required tags. */
	String[] msRequiredTags = { NIOFileWriter.FILEPATH };

	/** The writer list. */
	List<OutputFile> mWriterList = new ArrayList<OutputFile>();

	/** The writers. */
	OutputFile[] mWriters;

	Map<String, OutputFile> mWriterMap = new HashMap<String, OutputFile>();

	/** The ms escape char. */
	private String msEscapeChar;

	private boolean fileNameInPort;

	private String targetFilePath;

	private String fileNameFormat, filePathFormat;

	private String mQuoteStrings;

	boolean mZip;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#complete()
	 */
	@Override
	public int complete() throws KETLThreadException {
		// If there were no input rows, then we won't even have an upsert object...
		if (this.mWriterList == null) {
			ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "No records processed.");

			return 0;
		}

		for (OutputFile element : this.mWriterList)
			try {
				element.close();
			} catch (IOException e) {
				throw new KETLThreadException(e, this);
			}
		this.mWriters = null;

		return 0;
	}

	/**
	 * Creates the output file.
	 * 
	 * @param filePath
	 *            the file path
	 * @throws IOException
	 */
	void createOutputFile(String filePath) throws IOException {
		// add file streams to parallel stream parser
		OutputFile out = new OutputFile(this.mCharSet,this.mZip,this.miOutputBufferSize);
		out.open(filePath);
		this.mWriterList.add(out);
	}

	// Return 0 if success, otherwise error code...
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
	 */
	@Override
	public int initialize(Node xmlDestNode) throws KETLThreadException {
		int cd = super.initialize(xmlDestNode);

		if (cd != 0)
			return cd;

		// Get the attributes
		NamedNodeMap nmAttrs = xmlDestNode.getAttributes();
		this.mCharSet = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), NIOFileWriter.CHARACTERSET_ATTRIB,
				null);

		this.mZip = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(), NIOFileWriter.ZIP_ATTRIB, false);

		if (nmAttrs == null) {
			return 2;
		}

		if (this.maParameters == null) {
			throw new KETLThreadException("No complete parameter sets found, check that the following exist:\n"
					+ this.getRequiredTagsMessage(), this);
		}

		buildFieldDestinations(xmlDestNode);

		if (this.fileNameInPort == false) {
			List<String> files = new ArrayList<String>();
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
		} else {
			targetFilePath = this.getParameterValue(0, NIOFileWriter.FILEPATH);
		}

		// NOTE: Should return a declared constant not 0 or 1
		return 0;
	}

	private void buildFieldDestinations(Node xmlDestNode) throws KETLThreadException {
		NamedNodeMap nmAttrs;
		// build source file definition
		List<DestinationFieldDefinition> destFieldDefinitions = new ArrayList<DestinationFieldDefinition>();

		NodeList nl = xmlDestNode.getChildNodes();

		boolean AppendLineFeed = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(),
				NIOFileWriter.ENABLE_LINEFEED, true);
		this.mbDelimiterAtEnd = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(),
				NIOFileWriter.DELIMITER_AT_END, false);
		this.escapeEvenWhenQuoted = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(),
				NIOFileWriter.ESCAPE_WHEN_QUOTED, false);

		this.mQuoteStrings = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), NIOFileWriter.QUOTES, null);

		this.msDefaultDelimiter = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), NIOFileWriter.DELIMITER,
				"\t");
		this.msEscapeChar = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
				NIOFileWriter.ESCAPE_CHARACTER_STRING, "\\");

		String tmpLineFeed = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
				NIOFileWriter.LINEFEED_CHARACTER, "DOS");

		if (tmpLineFeed.compareTo("DOS") == 0) {
			this.mLinefeed = new String("\r\n");
		} else {
			this.mLinefeed = new String("\n");
		}

		this.miOutputBufferSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(), NIOFileWriter.WRITE_BUFFER,
				this.miOutputBufferSize);

		if (this.miOutputBufferSize == 0) {
			this.miOutputBufferSize = NIOFileWriter.DEFAULT_BUFFER_SIZE;
		}

		int lastField = -1, pos = 0;
		for (int i = 0; i < nl.getLength(); i++) {
			// See if we can match the name of the datasource...
			if (nl.item(i).getNodeName().compareTo(NIOFileWriter.IN) == 0) {
				DestinationFieldDefinition destFieldDefinition = new DestinationFieldDefinition(this.mCharSet);
				nmAttrs = nl.item(i).getAttributes();
				nmAttrs.getNamedItem(NIOFileWriter.NAME);

				destFieldDefinition.quoteString = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.QUOTES, null);
				if (destFieldDefinition.quoteString != null)
					destFieldDefinition.quoteEnabled = true;
				else if (this.mQuoteStrings == null && destFieldDefinition.quoteString == null)
					destFieldDefinition.quoteEnabled = false;
				else
					destFieldDefinition.quoteString = this.mQuoteStrings;

				destFieldDefinition.MaxLength = XMLHelper.getAttributeAsInt(nmAttrs, NIOFileWriter.MAXIMUM_LENGTH,
						destFieldDefinition.MaxLength);
				destFieldDefinition.FixedWidth = XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileWriter.FIXEDWIDTH,
						false);

				destFieldDefinition.Delimiter = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.DELIMITER,
						this.msDefaultDelimiter);

				destFieldDefinition.alwaysEscape = destFieldDefinition.Delimiter;

				destFieldDefinition.FormatString = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.FORMAT_STRING,
						destFieldDefinition.FormatString);
				destFieldDefinition.DefaultValue = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.DEFAULT_VALUE,
						destFieldDefinition.DefaultValue);

				if (XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileWriter.FILE_NAME, false)) {
					this.fileNameInPort = true;
					destFieldDefinition.fileNamePort = true;

					this.fileNameFormat = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.FILENAME_FORMAT,
							"{FILENAME}.{PARTITION}{SUBPARTITION}");
					this.filePathFormat = XMLHelper.getAttributeAsString(nmAttrs, NIOFileWriter.FILEPATH_FORMAT,
							targetFilePath);

				} else
					destFieldDefinition.fileNamePort = false;

				if (XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileWriter.SUB_PARTITION, false)) {
					destFieldDefinition.subPartitionPort = true;
				} else
					destFieldDefinition.subPartitionPort = false;

				destFieldDefinition.skip = XMLHelper.getAttributeAsBoolean(nmAttrs, NIOFileWriter.SKIP, false);

				if (destFieldDefinition.skip == false & lastField < pos) {
					lastField = pos;
				}
				pos++;
				destFieldDefinitions.add(destFieldDefinition);
			}
		}

		if (lastField == -1)
			throw new KETLThreadException("All destination fields are marked as skip or no fields are defined", this);
		// give file format configuration to parallel file reader.
		Object[] res = destFieldDefinitions.toArray();
		this.miDestFieldArrayLength = res.length;

		DestinationFieldDefinition[] sRes = new DestinationFieldDefinition[this.miDestFieldArrayLength];

		System.arraycopy(res, 0, sRes, 0, this.miDestFieldArrayLength);

		if (AppendLineFeed) {
			sRes[lastField].AppendLineFeed = true;
		}

		if (this.mbDelimiterAtEnd == false) {
			sRes[lastField].Delimiter = null;
		}

		this.mDestFieldDefinitions = sRes;
	}

	/** The sb. */
	StringBuilder sb = new StringBuilder();

	private boolean escapeEvenWhenQuoted = false;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
			throws KETLWriteException {

		String fileName = "NA", subPartition = null;
		// Read record and write to output buffer.
		for (int i = 0; i < pRecordWidth; i++) {
			// if record exists
			try {
				Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue()
						: pInputRecords[this.mInPorts[i].getSourcePortIndex()];

				// if null then quotes have been enabled globally but will only apply if the value
				// isn't a number. this can be determined here, as class is calculated at runtime
				if (this.mDestFieldDefinitions[i].quoteEnabled == null
						&& this.mDestFieldDefinitions[i].quoteString != null) {
					this.mDestFieldDefinitions[i].quoteEnabled = !Number.class.isAssignableFrom(this.mInPorts[i]
							.getPortClass());
				}

				if (this.fileNameInPort && this.mDestFieldDefinitions[i].fileNamePort) {
					fileName = data.toString();
				}

				if (this.fileNameInPort && this.mDestFieldDefinitions[i].subPartitionPort) {
					subPartition = data.toString();
				}

				if (this.mDestFieldDefinitions[i].skip)
					continue;

				String outData = null;
				// if byte array is null then apply default value
				if (data == null) {
					if (this.mDestFieldDefinitions[i].DefaultValue != null) {
						outData = this.escape(this.mDestFieldDefinitions[i].DefaultValue,
								this.mDestFieldDefinitions[i].Delimiter, !this.mDestFieldDefinitions[i].FixedWidth,
								this.mDestFieldDefinitions[i].alwaysEscape, this.mDestFieldDefinitions[i].quoteEnabled);
					}
				} else {
					if (this.mDestFieldDefinitions[i].FormatString != null) {
						int idx = this.mInPorts[i].getSourcePortIndex();
						outData = this.escape(this.mDestFieldDefinitions[i].getFormat(pExpectedDataTypes[idx]).format(
								data), this.mDestFieldDefinitions[i].Delimiter,
								!this.mDestFieldDefinitions[i].FixedWidth, this.mDestFieldDefinitions[i].alwaysEscape,
								this.mDestFieldDefinitions[i].quoteEnabled);
					} else if (this.mInPorts[i].getPortClass() == Double.class
							|| this.mInPorts[i].getPortClass() == Float.class) {
						Double val = (Double) data;
						if (val > Long.MAX_VALUE || val < Long.MIN_VALUE || val - val.longValue() != 0)
							data = val.toString();
						else
							data = val.longValue();
					}

					outData = this.escape(data.toString(), this.mDestFieldDefinitions[i].Delimiter,
							!this.mDestFieldDefinitions[i].FixedWidth, this.mDestFieldDefinitions[i].alwaysEscape,
							this.mDestFieldDefinitions[i].quoteEnabled);
				}

				// get max length of item to place inoutput buffer
				if (outData != null && this.mDestFieldDefinitions[i].MaxLength != -1
						&& outData.length() > this.mDestFieldDefinitions[i].MaxLength) {
					outData = outData.substring(0, this.mDestFieldDefinitions[i].MaxLength);
				}

				if (this.mDestFieldDefinitions[i].FixedWidth) {
					int rem = this.mDestFieldDefinitions[i].MaxLength - (outData == null ? 0 : outData.length());
					this.sb.append(outData);
					while (rem > 0) {
						this.sb.append(' ');
						rem--;
					}
				} else if (outData != null) {
					if (this.mDestFieldDefinitions[i].quoteEnabled)
						outData = quote(outData, this.mDestFieldDefinitions[i].quoteString, this.msEscapeChar);
					this.sb.append(outData);
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

		try {
			int recs = 0;
			if (this.fileNameInPort) {

				String key = subPartition == null ? fileName : fileName + subPartition;
				OutputFile wr = this.mWriterMap.get(key);

				if (wr == null) {
					wr = createNewWriterMap(fileName, subPartition);
					this.mWriterMap.put(key, wr);
				}
				recs++;
				wr.writer.append(this.sb.toString());
			} else {
				// all done write to streams
				for (OutputFile wr : this.mWriters) {
					recs++;
					wr.writer.append(this.sb.toString());
				}
			}

			this.sb.setLength(0);
			return recs;

		} catch (IOException e) {
			throw new KETLWriteException(e);
		}

	}

	private OutputFile createNewWriterMap(String fileName, String subPartition) throws KETLWriteException, IOException {
		OutputFile out = new OutputFile(this.mCharSet,this.mZip,this.miOutputBufferSize);

		String path = "";
		if (this.targetFilePath != null)
			path = this.targetFilePath + File.separator;
		else
			path = "." + File.separator;

		if (this.filePathFormat != null) {
			path = this.filePathFormat;
			path = path.replace("{PARTITION}", (this.partitions > 1 ? Integer.toString(this.partitionID) : ""));
			path = path.replace("{SUBPARTITION}", subPartition == null ? "" : subPartition);
			File f = new File(path);
			if (f.exists() == false) {
				ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Creating " + path
						+ " directory " + f.getAbsolutePath());
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
		out.open(path + fn);

		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Dynamically declaring target file: " + fn);

		this.mWriterList.add(out);

		return out;
	}

	/**
	 * Escape.
	 * 
	 * @param datum
	 *            the datum
	 * @param del
	 *            the del
	 * @param hasDelimeter
	 *            the has delimeter
	 * @param alwaysEscape
	 * @param quoted
	 * 
	 * @return the char sequence
	 */
	private String escape(String datum, String del, boolean hasDelimeter, String alwaysEscape, Boolean quoted) {

		// no need to escape as the quoting will deal with escaping
		if (quoted && this.escapeEvenWhenQuoted == false)
			return datum;

		del = del == null ? alwaysEscape : del;

		if (hasDelimeter && datum != null && del != null) {

			if (datum.contains(this.msEscapeChar))
				datum = datum.replace(this.msEscapeChar, this.msEscapeChar + this.msEscapeChar);
			if (datum.contains(del))
				return datum.replace(del, this.msEscapeChar + del);
		}
		return datum;
	}

	private static String quote(String datum, String quotes, String escapeCharacter) {
		// if (datum.contains(escapeCharacter))
		// datum = datum.replace(escapeCharacter, escapeCharacter + escapeCharacter);
		if (datum.contains(quotes))
			return datum.replace(quotes, escapeCharacter + quotes);
		return (quotes != null && datum != null) ? quotes + datum + quotes : datum;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {

		if (this.mWriterList == null)
			return;

		for (OutputFile wr : this.mWriterList) {
			try {
				wr.close();
			} catch (IOException e) {
				ResourcePool.LogException(e, this);
			}
		}

	}

}
