package com.kni.etl.ketl.writer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
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

public class CompressedFileWriter extends ETLWriter implements DefaultWriterCore {

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
	public CompressedFileWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
			throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/** The Constant DEFAULT_BUFFER_SIZE. */
	static final int DEFAULT_BUFFER_SIZE = 163840;

	private static final String FORMAT_ATTRIB = "FORMAT";

	private static final String COMRPESSION_BUFFER = "COMPRESSIONBUFFER";

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
	private static String FILEPATH = "FILEPATH";

	/** The linefeed. */
	private String mLinefeed;

	/** The mb delimiter at end. */
	private boolean mbDelimiterAtEnd = false;

	/** The dest field definitions. */
	private DestinationFieldDefinition[] mDestFieldDefinitions = null;

	/** The mi output buffer size. */
	private int miOutputBufferSize = 16384;

	/** The ms default delimiter. */
	private String msDefaultDelimiter = null;

	/** The mi dest field array length. */
	private int miDestFieldArrayLength = -1;

	/** The char set. */
	private String mCharSet = null;

	/** The ms required tags. */
	String[] msRequiredTags = { CompressedFileWriter.FILEPATH };

	/** The writer list. */
	ArrayList mWriterList = new ArrayList();

	/** The writers. */
	OutputFile[] mWriters;

	/** The ms escape char. */
	private String msEscapeChar;

	private String mCompressionType;

	private int mCompressionBufferSize = 512;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#complete()
	 */
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

	/**
	 * The Class OutputFile.
	 */
	class OutputFile {

		/** The stream. */
		private DeflaterOutputStream stream;

		/** The writer. */
		Writer writer;

		private FileOutputStream fileStream;

		private WritableByteChannel channel;

		/**
		 * Open.
		 * 
		 * @param filePath
		 *            the file path
		 * @throws IOException
		 */
		void open(String filePath) throws IOException {
			this.fileStream = new FileOutputStream(filePath);
			if (mCompressionType.equalsIgnoreCase("ZIP")) {
				this.stream = new ZipOutputStream(this.fileStream);
			} else
				this.stream = new GZIPOutputStream(this.fileStream, mCompressionBufferSize);

			this.channel = java.nio.channels.Channels.newChannel(this.stream);

			CharsetEncoder charSet = (CompressedFileWriter.this.mCharSet == null ? java.nio.charset.Charset
					.defaultCharset().newEncoder() : java.nio.charset.Charset.forName(
					CompressedFileWriter.this.mCharSet).newEncoder());

			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Writing to file " + filePath
					+ ", character set " + charSet.charset().displayName());
			this.writer = java.nio.channels.Channels.newWriter(this.channel, charSet,
					CompressedFileWriter.this.miOutputBufferSize);

		}

		/**
		 * Close.
		 * 
		 * @throws IOException
		 *             Signals that an I/O exception has occurred.
		 */
		void close() throws IOException {
			this.writer.close();
			this.channel.close();
			this.stream.close();
			this.fileStream.close();
		}
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
		OutputFile out = new OutputFile();
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
		this.mCharSet = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), CHARACTERSET_ATTRIB, null);
		this.mCompressionType = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(), FORMAT_ATTRIB, "GZIP");
		if (nmAttrs == null) {
			return 2;
		}

		if (this.maParameters == null) {
			throw new KETLThreadException("No complete parameter sets found, check that the following exist:\n"
					+ this.getRequiredTagsMessage(), this);
		}

		ArrayList files = new ArrayList();
		for (int i = 0; i < this.maParameters.size(); i++) {
			String filePath = this.getParameterValue(i, CompressedFileWriter.FILEPATH);

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
				CompressedFileWriter.ENABLE_LINEFEED, true);
		this.mbDelimiterAtEnd = XMLHelper.getAttributeAsBoolean(xmlDestNode.getAttributes(),
				CompressedFileWriter.DELIMITER_AT_END, false);
		this.msDefaultDelimiter = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
				CompressedFileWriter.DELIMITER, "\t");
		this.msEscapeChar = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
				CompressedFileWriter.ESCAPE_CHARACTER_STRING, "\\");

		String tmpLineFeed = XMLHelper.getAttributeAsString(xmlDestNode.getAttributes(),
				CompressedFileWriter.LINEFEED_CHARACTER, "DOS");

		if (tmpLineFeed.compareTo("DOS") == 0) {
			this.mLinefeed = new String("\r\n");
		} else {
			this.mLinefeed = new String("\n");
		}

		this.mCompressionBufferSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(),
				CompressedFileWriter.COMRPESSION_BUFFER, 512);
		this.miOutputBufferSize = XMLHelper.getAttributeAsInt(xmlDestNode.getAttributes(),
				CompressedFileWriter.WRITE_BUFFER, this.miOutputBufferSize);

		if (this.miOutputBufferSize == 0) {
			this.miOutputBufferSize = NIOFileWriter.DEFAULT_BUFFER_SIZE;
		}

		for (int i = 0; i < nl.getLength(); i++) {
			// See if we can match the name of the datasource...
			if (nl.item(i).getNodeName().compareTo(CompressedFileWriter.IN) == 0) {
				DestinationFieldDefinition destFieldDefinition = new DestinationFieldDefinition(this.mCharSet);
				nmAttrs = nl.item(i).getAttributes();
				nmAttrs.getNamedItem(CompressedFileWriter.NAME);

				destFieldDefinition.MaxLength = XMLHelper.getAttributeAsInt(nmAttrs,
						CompressedFileWriter.MAXIMUM_LENGTH, destFieldDefinition.MaxLength);
				destFieldDefinition.FixedWidth = XMLHelper.getAttributeAsBoolean(nmAttrs,
						CompressedFileWriter.FIXEDWIDTH, false);

				destFieldDefinition.Delimiter = XMLHelper.getAttributeAsString(nmAttrs, CompressedFileWriter.DELIMITER,
						this.msDefaultDelimiter);
				destFieldDefinition.FormatString = XMLHelper.getAttributeAsString(nmAttrs,
						CompressedFileWriter.FORMAT_STRING, destFieldDefinition.FormatString);
				destFieldDefinition.DefaultValue = XMLHelper.getAttributeAsString(nmAttrs,
						CompressedFileWriter.DEFAULT_VALUE, destFieldDefinition.DefaultValue);

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

	/** The sb. */
	StringBuilder sb = new StringBuilder();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
	 */
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
				} else {
					if (this.mDestFieldDefinitions[i].FormatString != null) {
						int idx = this.mInPorts[i].getSourcePortIndex();
						this.sb.append(this.escape(this.mDestFieldDefinitions[i].getFormat(pExpectedDataTypes[idx])
								.format(data), this.mDestFieldDefinitions[i].Delimiter,
								!this.mDestFieldDefinitions[i].FixedWidth));
					} else
						this.sb.append(this.escape(data.toString(), this.mDestFieldDefinitions[i].Delimiter,
								!this.mDestFieldDefinitions[i].FixedWidth));
				}

				// get max length of item to place inoutput buffer
				if ((this.mDestFieldDefinitions[i].MaxLength != -1)
						&& (this.sb.length() - pos > this.mDestFieldDefinitions[i].MaxLength)) {
					this.sb.setLength(pos + this.mDestFieldDefinitions[i].MaxLength);
				} else if (this.mDestFieldDefinitions[i].FixedWidth) {
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

	/**
	 * Escape.
	 * 
	 * @param datum
	 *            the datum
	 * @param del
	 *            the del
	 * @param hasDelimeter
	 *            the has delimeter
	 * 
	 * @return the char sequence
	 */
	private CharSequence escape(String datum, String del, boolean hasDelimeter) {

		if (hasDelimeter && datum != null && del != null) {
			if (datum.contains(del))
				return datum.replace(del, this.msEscapeChar + del);
		}
		return datum;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
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