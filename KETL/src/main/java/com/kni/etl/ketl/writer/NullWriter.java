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

import org.w3c.dom.Node;

import com.kni.etl.SharedCounter;
import com.kni.etl.dbutils.ResourcePool;
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
public class NullWriter extends ETLWriter implements DefaultWriterCore {

	@Override
	protected String getVersion() {
		return "$LastChangedRevision$";
	}

	/** The report back. */
	long mReportBack = 100000;

	/** The LOGEVER y_ ATTRIB. */
	private final String LOGEVERY_ATTRIB = "LOGEVERY";

	/** The LO g_ ATTRIB. */
	private final String LOG_ATTRIB = "LOG";

	/** The log. */
	private boolean mLog = true;

	/**
	 * Instantiates a new null writer.
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
	public NullWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
	 */
	@Override
	protected int initialize(Node xmlConfig) throws KETLThreadException {
		int res = super.initialize(xmlConfig);
		if (res != 0)
			return res;

		this.mReportBack = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), this.LOGEVERY_ATTRIB, (int) this.mReportBack);
		this.mLog = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), this.LOG_ATTRIB, true);

		this.mSharedCounter = this.getJobExecutor().getCurrentETLJob().getCounter(this.getName());
		return res;
	}

	/** The shared counter. */
	SharedCounter mSharedCounter;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[],
	 * java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] o, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

		if (this.mLog) {
			if (this.mSharedCounter.increment(1) % this.mReportBack == 0)
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Records processed: " + this.mSharedCounter.value());
		}
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#complete()
	 */
	@Override
	public int complete() throws KETLThreadException {
		int res = super.complete();

		if (this.mLog && this.isLastThreadToEnterCompletePhase())
			ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Total Records processed: " + this.mSharedCounter.value());
		return res;
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
