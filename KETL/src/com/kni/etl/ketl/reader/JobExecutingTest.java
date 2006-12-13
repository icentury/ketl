package com.kni.etl.ketl.reader;

import org.w3c.dom.Node;

import com.kni.etl.ETLJob;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.reader.JDBCReader.JDBCReaderETLOutPort;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

public class JobExecutingTest extends ETLReader implements DefaultReaderCore {

	public JobExecutingTest(Node pXMLConfig, int pPartitionID, int pPartition,
			ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

	}

	@Override
	protected void close(boolean success) {
	}

	@Override
	protected ETLOutPort getNewOutPort(com.kni.etl.ketl.ETLStep srcStep) {
		return new JOBIDETLOutPort((ETLStep) this, (ETLStep) this);
	}

	public class JOBIDETLOutPort extends ETLOutPort {

		public JOBIDETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

		@Override
		public boolean containsCode() throws KETLThreadException {
			return true;
		}

		String mJobID = null;

		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException,
				KETLThreadException {
			int res = super.initialize(xmlConfig);
			if (res != 0)
				return res;

			mJobID = XMLHelper.getAttributeAsString(this.getXMLConfig()
					.getAttributes(), "JOBID", null);

			return 0;
		}

	}

	public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes,
			int pRecordWidth) throws KETLReadException {

		for (int i = 0; i < this.mOutPorts.length; i++) {
			if (this.mOutPorts[i].isUsed()) {

				if (((JOBIDETLOutPort) this.mOutPorts[i]).mJobID != null) {
					String res = ((JOBIDETLOutPort) this.mOutPorts[i]).mJobID;

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

		return COMPLETE;

	}

}
