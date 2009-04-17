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
package com.kni.etl.ketl.reader;

import org.w3c.dom.Node;

import com.kni.etl.ETLJob;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class JobExecutingTest.
 */
public class JobExecutingTest extends ETLReader implements DefaultReaderCore {

	/**
	 * Instantiates a new job executing test.
	 * 
	 * @param pXMLConfig the XML config
	 * @param pPartitionID the partition ID
	 * @param pPartition the partition
	 * @param pThreadManager the thread manager
	 * 
	 * @throws KETLThreadException the KETL thread exception
	 */
	public JobExecutingTest(Node pXMLConfig, int pPartitionID, int pPartition,
			ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

		
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLOutPort getNewOutPort(com.kni.etl.ketl.ETLStep srcStep) {
		return new JOBIDETLOutPort(this, this);
	}

	/**
	 * The Class JOBIDETLOutPort.
	 */
	public class JOBIDETLOutPort extends ETLOutPort {

		/**
		 * Instantiates a new JOBIDETL out port.
		 * 
		 * @param esOwningStep the es owning step
		 * @param esSrcStep the es src step
		 */
		public JOBIDETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

		/* (non-Javadoc)
		 * @see com.kni.etl.ketl.ETLPort#containsCode()
		 */
		@Override
		public boolean containsCode() throws KETLThreadException {
			return true;
		}

		/** The job ID. */
		String mJobID = null;

		/* (non-Javadoc)
		 * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException,
				KETLThreadException {
			int res = super.initialize(xmlConfig);
			if (res != 0)
				return res;

			this.mJobID = XMLHelper.getAttributeAsString(this.getXMLConfig()
					.getAttributes(), "JOBID", null);

			return 0;
		}

	}

	/* (non-Javadoc)
	 * @see com.kni.etl.ketl.smp.DefaultReaderCore#getNextRecord(java.lang.Object[], java.lang.Class[], int)
	 */
	public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes,
			int pRecordWidth) throws KETLReadException {

		for (ETLOutPort element : this.mOutPorts) {
			if (element.isUsed()) {

				if (((JOBIDETLOutPort) element).mJobID != null) {
					String res = ((JOBIDETLOutPort) element).mJobID;

					if (res != null) {
						try {
							ETLJob job = ResourcePool.getMetadata().getJob(res);
							if (job != null) {
								ResourcePool.getMetadata().getJobStatus(job);
								if (job.getLoadID() > 0) {

									throw new KETLReadException(
											res
													+ " Job executing, failure requested");
								}
							}
						} catch (Exception e) {
							throw new KETLReadException(e);
						}
					}
				}
			}
		}

		return DefaultReaderCore.COMPLETE;

	}

}
