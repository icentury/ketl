/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.KETLJob;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.lookup.LookupCreatorImpl;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.etl.util.XMLHelper;

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
public class LookupWriter extends ETLWriter implements DefaultWriterCore, LookupCreatorImpl {

    private static final String KEY_ATTRIB = "KEY";

    private static final String VALUE_ATTRIB = "VALUE";

    public int mKeys = 0, mValues = 0;

    public class LookupWriterInPort extends ETLInPort {

        private int mKey;

        private int mValue;

        @Override
        public int initialize(Node xmlNode) throws ClassNotFoundException, KETLThreadException {

            super.initialize(xmlNode);

            // Create a new column definition with the default properties...

            NamedNodeMap attr = xmlNode.getAttributes();

            mKey = XMLHelper.getAttributeAsInt(attr, KEY_ATTRIB, -1);
            mValue = XMLHelper.getAttributeAsInt(attr, VALUE_ATTRIB, -1);

            if (mKey != -1) {
                if (mKey < 1)
                    throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
                            + " KEY order starts at 1, invalid value of " + mKey, this);
                mKeys++;
                mKey--;
            }
            if (mValue != -1) {
                if (mValue < 1)
                    throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
                            + " VALUE order starts at 1, invalid value of " + mValue, this);

                mValues++;
                mValue--;
            }
            return 0;
        }

        public LookupWriterInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new LookupWriterInPort(this, srcStep);
    }

    public LookupWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    private Integer mCachePersistenceID = -1;

    private int mCacheSize;

    private int cachePersistence;

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);
        if (res != 0)
            return res;

        int minSize = NumberFormatter.convertToBytes(EngineConstants.getDefaultCacheSize());
        cachePersistence = EngineConstants.JOB_PERSISTENCE;

        String tmp = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "PERSISTENCE", null);
        if (tmp == null || tmp.equalsIgnoreCase("JOB")) {
            this.mCachePersistenceID = ((Long) this.getJobExecutionID()).intValue();
            cachePersistence = EngineConstants.JOB_PERSISTENCE;
        }
        else if (tmp.equalsIgnoreCase("LOAD")) {
            this.mCachePersistenceID = this.mkjExecutor.getCurrentETLJob().getLoadID();
            cachePersistence = EngineConstants.LOAD_PERSISTENCE;
        }
        else if (tmp.equalsIgnoreCase("STATIC")) {
            cachePersistence = EngineConstants.STATIC_PERSISTENCE;
            this.mCachePersistenceID = null;
        }
        else
            throw new KETLThreadException("PERSISTENCE has to be either JOB,LOAD or STATIC", this);

        mCacheSize = NumberFormatter.convertToBytes(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                "CACHESIZE", null));

        if (mCacheSize == -1)
            mCacheSize = minSize;
        if (mCacheSize < minSize) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                    "Cache cannot be less than 64kb, defaulting to 64kb");
            mCacheSize = minSize;
        }

        if (mKeys == 0) {
            throw new KETLThreadException("No keys have been specified", this);
        }
        if (mValues == 0) {
            throw new KETLThreadException("No return values have been specified", this);
        }
        if (mKeys > 4) {
            ResourcePool
                    .LogMessage(this, ResourcePool.INFO_MESSAGE,
                            "Currently lookups are limited to no more than 4 keys, unless you use a an array object to represent the compound key");
        }

        this.mLookup = ((KETLJob) this.getJobExecutor().getCurrentETLJob()).registerLookupWriteLock(this.getName(),
                this, cachePersistence);
        this.lookupLocked = true;

        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.writer.LookupCreatorImpl#getLookup()
     */
    public PersistentMap getLookup() {

        Class[] types = new Class[mKeys];
        Class[] values = new Class[mValues];
        String[] valueFields = new String[mValues];
        for (int i = 0; i < this.mInPorts.length; i++) {
            LookupWriterInPort port = (LookupWriterInPort) this.mInPorts[i];

            if (port.mKey != -1)
                types[port.mKey] = port.getPortClass();
            if (port.mValue != -1) {
                values[port.mValue] = port.getPortClass();
                valueFields[port.mValue] = port.mstrName;

            }

        }
        
        
       String lookupClass = XMLHelper.getAttributeAsString(this.getXMLConfig()
				.getAttributes(), "LOOKUPCLASS", EngineConstants
				.getDefaultLookupClass());

		try {
			return EngineConstants.getInstanceOfPersistantMap(lookupClass, this
					.getName(), mCacheSize, this.mCachePersistenceID,
					EngineConstants.CACHE_PATH, types, values, valueFields,
					cachePersistence == EngineConstants.JOB_PERSISTENCE ? true
							: false);
		} catch (Throwable e) {
			ResourcePool.LogMessage(Thread.currentThread(),
					ResourcePool.WARNING_MESSAGE,
					"Lookup cache creation failed, trying again, check stack trace");
			e.printStackTrace();
			try {
				return EngineConstants
						.getInstanceOfPersistantMap(
								lookupClass,
								this.getName(),
								mCacheSize,
								this.mCachePersistenceID,
								EngineConstants.CACHE_PATH,
								types,
								values,
								valueFields,
								cachePersistence == EngineConstants.JOB_PERSISTENCE ? true
										: false);
			} catch (Throwable e1) {

				e1.printStackTrace();
				throw new KETLError("LOOKUPCLASS " + lookupClass
						+ " could not be found: " + e.getMessage(), e);
			}
		}
       
    }

    private PersistentMap mLookup;

    public int putNextRecord(Object[] o, Class[] pExpectedDataTypes,
			int pRecordWidth) throws KETLWriteException {
		/*
		 * if (mKeys == 1 && mValues == 1) putKeyObjectDataObject(o); else if
		 * (mKeys == 1 && mValues > 1) putKeyObjectDataArray(o); else
		 */
		// if ( mKeys > 1 && mValues == 1)
		// putKeyArrayDataObject(o);
		// else if (/* mKeys > 1 && mValues > 1)
		try {
			putKeyArrayDataArray(o);
		} catch (Error e) {
			throw new KETLWriteException(e.getMessage());
		}

		return 1;
	}

    private void putKeyArrayDataArray(Object[] o) {
        Object[] elements = new Object[this.mKeys];
        Object[] values = new Object[this.mValues];
        for (int i = 0; i < this.mInPorts.length; i++) {

            LookupWriterInPort port = (LookupWriterInPort) this.mInPorts[i];
            if (port.mKey != -1) {
                elements[port.mKey] = o[port.getSourcePortIndex()];
            }
            else if (port.mValue != -1) {
                values[port.mValue] = o[port.getSourcePortIndex()];
            }

        }

        this.mLookup.put(elements, values);
    }

    boolean lookupLocked = false;
    
    @Override
    public int complete() throws KETLThreadException {
        int res = super.complete();

        if (res != 0)
            return res;
        // submit lookup for use
        if(lookupLocked)
        	((KETLJob) this.getJobExecutor().getCurrentETLJob()).releaseLookupWriteLock(this.getName(), this);

        return 0;
    }

    public PersistentMap swichToReadOnlyMode() {
        this.mLookup.switchToReadOnlyMode();
        return this.mLookup;
    }

    @Override
    protected void close(boolean success) {
    	if(lookupLocked)
        	((KETLJob) this.getJobExecutor().getCurrentETLJob()).releaseLookupWriteLock(this.getName(), this);

        if (this.cachePersistence == EngineConstants.JOB_PERSISTENCE) {
            ((KETLJob) this.getJobExecutor().getCurrentETLJob()).deleteLookup(this.getName());
        }
    }

}
