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

import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title: ETLWriter
 * </p>
 * <p>
 * Description: Abstract base class for ETL destination loading.
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class ConsoleWriter extends ETLWriter implements DefaultWriterCore {

	private enum Type {
		NORMAL, FULL
	};

	private Type type = Type.NORMAL;

	/**
	 * Instantiates a new console writer.
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
	public ConsoleWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
			throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/** The list headers. */
	private boolean listHeaders = true;

	/** The osw. */
	private OutputStreamWriter osw = new OutputStreamWriter(System.out);

	/** The pw. */
	private PrintWriter pw = new PrintWriter(this.osw, true);

	private int recordCount = 1;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] o, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

		if (type == Type.NORMAL) {
			if (this.listHeaders) {
				for (int i = 0; i < this.mInPorts.length; i++) {
					if (i > 0)
						this.pw.print(';');
					this.pw.print(this.mInPorts[i].mstrName + "(" + this.mInPorts[i].getPortClass().getCanonicalName()
							+ ")");
				}
				this.pw.println();
				this.listHeaders = false;
			}
			for (int i = 0; i < this.mInPorts.length; i++) {
				if (i > 0)
					this.pw.print(';');

				Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue() : o[this.mInPorts[i]
						.getSourcePortIndex()];

				if (data == null)
					this.pw.print("[NULL]");
				else if (this.mInPorts[i].isArray()) {
					Object[] ar = (Object[]) data;
					this.pw.print(java.util.Arrays.toString(ar));
				} else
					this.pw.print(data);

			}
			this.pw.println();
		} else if (type == Type.FULL) {
			this.pw.println(this.getName() + " - Record: " + this.recordCount++);
			for (int i = 0; i < this.mInPorts.length; i++) {
				try {
					this.pw.println("\t" + this.mInPorts[i].getPortName() + ": "
							+ o[this.mInPorts[i].getSourcePortIndex()]);
				} catch (KETLThreadException e) {
					throw new KETLWriteException(e);
				}
			}
		}
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {

	}

	@Override
	protected int initialize(Node xmlConfig) throws KETLThreadException {
		int res = super.initialize(xmlConfig);

		this.type = Type.valueOf(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "TYPE", "NORMAL"));
		return res;
	}

}
