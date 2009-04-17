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
package com.kni.etl.ketl.transformation;

import java.util.ArrayList;

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.AggregatingTransform;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;
import com.kni.etl.util.aggregator.AggregateException;
import com.kni.etl.util.aggregator.Aggregator;
import com.kni.etl.util.aggregator.Direct;
import com.kni.etl.util.aggregator.ToArray;

// TODO: Auto-generated Javadoc
/**
 * The Class Aggregate.
 */
public class Aggregate extends ETLTransformation implements AggregatingTransform {

    /**
     * Instantiates a new aggregate.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public Aggregate(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /**
     * The Class AggregateETLOutPort.
     */
    class AggregateETLOutPort extends ETLOutPort {

        /** The aggregator. */
        Aggregator aggregator;

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {

            String function = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "FUNCTION", null);

            try {

                if (function == null) {
                    this.aggregator = new Direct();
                }
                else {
                    Class cl = Class.forName(function.contains(".") ? function : "com.kni.etl.util.aggregator."
                            + function);
                    this.aggregator = (Aggregator) cl.newInstance();

                    if (this.aggregator instanceof ToArray) {
                        int limit = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), "MAXELEMENTS", -1);
                        if (limit > 0)
                            ((ToArray) this.aggregator).setArrayLimit(limit);
                    }
                }

            } catch (Exception e) {
                throw new KETLThreadException(e, this);
            }

            // instantiate aggregator class
            // determine if result always creates an array

            return super.initialize(xmlConfig);

        }

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#getPortClass()
         */
        @Override
        public Class getPortClass() throws AggregateException {
            Class cl = super.getPortClass();

            if (cl == null)
                return null;

            this.aggregator.setValueClass(cl);
            return this.aggregator.getValueClass();
        }

        /**
         * Instantiates a new aggregate ETL out port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public AggregateETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new AggregateETLOutPort(this, srcStep);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.AggregatingTransform#getAggregates()
     */
    public Aggregator[] getAggregates() {

        ArrayList res = new ArrayList();
        for (ETLOutPort element : this.mOutPorts)
            if (element.isUsed())
                res.add(((AggregateETLOutPort) element).aggregator);

        Aggregator[] result = new Aggregator[res.size()];
        res.toArray(result);
        return result;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success, boolean jobFailed) {
        // TODO Auto-generated method stub

    }

}
