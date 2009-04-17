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
/*
 * Created on Mar 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.reader;

import java.text.DateFormat;
import java.util.Date;

import org.w3c.dom.Node;

import com.kni.etl.DataItemHelper;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.DateAdd;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class SequenceGenerator.
 * 
 * @author nwakefield Creation Date: Mar 17, 2003
 */
public class SequenceGenerator extends ETLReader implements DefaultReaderCore {

	/**
	 * Instantiates a new sequence generator.
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
	public SequenceGenerator(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
			throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/** The Constant DATATYPE. */
	public static final String DATATYPE = "DATATYPE";

	/** The Constant VALUES. */
	public static final String VALUES = "VALUES";

	/** The value counter. */
	int mValueCounter = 0;

	/** The values requested. */
	int mValuesRequested;

	/**
	 * The Class SequenceOutPort.
	 */
	class SequenceOutPort extends ETLOutPort {

		/** The counter. */
		Counter counter = null;

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#containsCode()
		 */
		@Override
		public boolean containsCode() throws KETLThreadException {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
			int res = super.initialize(xmlConfig);
			if (res != 0)
				return res;

			this.counter = new Counter();

			int type = DataItemHelper.getDataTypeIDbyName(XMLHelper.getAttributeAsString(this.getXMLConfig()
					.getAttributes(), SequenceGenerator.DATATYPE, "STRING"));

			this.counter.type = type;

			String startValue = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "STARTVALUE", null);
			String incrementValue = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "INCREMENT",
					null);
			if (this.isConstant() == false) {
				switch (type) {
				case DataItemHelper.STRING:
					throw new KETLThreadException("String not supported for sequence generator", this);
				case DataItemHelper.INTEGER:
					this.counter.mItem = startValue == null ? 0 : Integer.parseInt(startValue);
					this.counter.mIncrement = incrementValue == null ? 1 : Integer.parseInt(incrementValue);
					break;
				case DataItemHelper.LONG:
					this.counter.mItem = startValue == null ? new Long(0) : Long.parseLong(startValue);
					this.counter.mIncrement = incrementValue == null ? new Long(1) : Long.parseLong(incrementValue);
					break;
				case DataItemHelper.FLOAT:
					this.counter.mItem = startValue == null ? new Float(0) : Float.parseFloat(startValue);
					this.counter.mIncrement = incrementValue == null ? new Float(1) : Float.parseFloat(incrementValue);
					break;
				case DataItemHelper.DATE:
					String fmtStr = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "FORMATSTRING",
							null);

					DateFormat fm = (fmtStr == null ? DateFormat.getTimeInstance(DateFormat.LONG)
							: new FastSimpleDateFormat(fmtStr));
					try {
						this.counter.mItem = startValue == null ? new Date() : fm.parse(startValue);
						this.counter.mIncrement = new DateAdd(incrementValue == null ? "1dy" : incrementValue);
					} catch (Exception e) {
						throw new KETLThreadException(e, this);
					}
					break;
				case DataItemHelper.DOUBLE:
					this.counter.mItem = startValue == null ? new Double(0) : Double.parseDouble(startValue);
					this.counter.mIncrement = incrementValue == null ? new Double(1) : Double
							.parseDouble(incrementValue);

					break;
				case DataItemHelper.CHAR:
					this.counter.mItem = startValue == null ? new Character('a') : startValue.charAt(0);
					this.counter.mIncrement = incrementValue == null ? new Character((char) 1) : startValue.charAt(0);

					break;
				default:
					throw new KETLThreadException(this.getPortClass().getCanonicalName()
							+ "  not supported for sequence generator", this);
				}
			}
			return 0;
		}

		/**
		 * Instantiates a new sequence out port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public SequenceOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

	}

	/**
	 * The Class Counter.
	 */
	class Counter {

		/** The type. */
		int type;

		/** The item. */
		Object mItem;

		/** The increment. */
		Object mIncrement;

		/**
		 * Increment.
		 * 
		 * @return the object
		 * 
		 * @throws KETLReadException
		 *             the KETL read exception
		 */
		Object increment() throws KETLReadException {
			Object res = this.mItem;
			switch (this.type) {
			case DataItemHelper.INTEGER:
				this.mItem = ((Integer) this.mItem).intValue() + ((Integer) this.mIncrement).intValue();
				break;
			case DataItemHelper.LONG:
				this.mItem = ((Long) this.mItem).longValue() + ((Long) this.mIncrement).longValue();
				break;
			case DataItemHelper.FLOAT:
				this.mItem = ((Float) this.mItem).floatValue() + ((Float) this.mIncrement).floatValue();

				break;
			case DataItemHelper.DATE:
				this.mItem = new Date(((Date) this.mItem).getTime());
				this.mItem = ((DateAdd) this.mIncrement).increment((Date) this.mItem);
				break;
			case DataItemHelper.DOUBLE:
				this.mItem = ((Double) this.mItem).doubleValue() + ((Double) this.mIncrement).doubleValue();

				break;
			case DataItemHelper.CHAR:
				this.mItem = ((Character) this.mItem).charValue() + ((Character) this.mIncrement).charValue();

				break;
			default:
				throw new KETLReadException("Datatype not supported for sequence generator");
			}
			return res;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.reader.ETLReader#initialize(org.w3c.dom.Node)
	 */
	@Override
	public int initialize(Node pXmlConfig) throws KETLThreadException {
		int res = super.initialize(pXmlConfig);

		this.mValuesRequested = XMLHelper.getAttributeAsInt(pXmlConfig.getAttributes(), SequenceGenerator.VALUES, -1);

		if (this.mValuesRequested < 0) {
			throw new KETLThreadException("Values requested is missing or a negative number", this);
		}
		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.DefaultReaderCore#getNextRecord(java.lang.Object[], java.lang.Class[], int)
	 */
	public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
			throws KETLReadException {

		if (this.mValueCounter++ < this.mValuesRequested) {
			for (int i = 0; i < this.mOutPorts.length; i++) {
				if (this.mOutPorts[i].isUsed()) {

					if (this.mOutPorts[i].isConstant())
						pResultArray[i] = this.mOutPorts[i].getConstantValue();
					else
						pResultArray[i] = ((SequenceOutPort) this.mOutPorts[i]).counter.increment();
				}
			}
		} else
			return DefaultReaderCore.COMPLETE;
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLOutPort getNewOutPort(ETLStep srcStep) {
		return new SequenceOutPort(this, srcStep);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
	}
}
