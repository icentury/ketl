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

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.annotations.Attribute;
import com.kni.etl.ketl.annotations.Parameter;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class HelloWorld.
 */
public class HelloWorld extends ETLReader implements DefaultReaderCore {

    /**
     * Instantiates a new hello world.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public HelloWorld(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /** The Constant VALUES. */
    @Attribute(datatype = "INTEGER")
    public static final String VALUES = "VALUES";

    /** The Constant PHRASE. */
    @Parameter()
    public static final String PHRASE = "PHRASE";

    /** The value counter. */
    private int mValueCounter = 0;
    
    /** The values requested. */
    private int mValuesRequested;
    
    /** The phrase. */
    private String mPhrase;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node pXmlConfig) throws KETLThreadException {
        int res = super.initialize(pXmlConfig);

        // how many rows to generator with Hello world in them
        this.mValuesRequested = XMLHelper.getAttributeAsInt(pXmlConfig.getAttributes(), HelloWorld.VALUES, 1);

        // there may exist multiple parameter lists per step, 0 references the first one and so on
        this.mPhrase = this.getParameterValue(0, HelloWorld.PHRASE);

        if (this.mPhrase == null) {
            this.mPhrase = "Hello World";
        }

        return res;
    }

    // this isn't needed here but if you need attributes at the port level then this is where you do them.
    /**
     * The Class HelloWorldOutPort.
     */
    class HelloWorldOutPort extends ETLOutPort {
        
        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#containsCode()
         */
        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }
        
        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;
           
            return 0;
        }
        
        /**
         * Instantiates a new hello world out port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public HelloWorldOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new HelloWorldOutPort(this, srcStep);
    }
    
    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.DefaultReaderCore#getNextRecord(java.lang.Object[], java.lang.Class[], int)
     */
    public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException {

        // as we are generating records not reading from a source we need to use a counter
        if (this.mValueCounter++ < this.mValuesRequested) {

            // cycle through each port assigning the appropiate value if port used
            for (int i = 0; i < this.mOutPorts.length; i++) {
                if (this.mOutPorts[i].isUsed()) {

                    // if port contains constant then use constant
                    if (this.mOutPorts[i].isConstant())
                        pResultArray[i] = this.mOutPorts[i].getConstantValue();
                    else
                        pResultArray[i] = this.mPhrase;
                }
            }
        }
        else
            return DefaultReaderCore.COMPLETE;

        // return row count, should always be one for a reader
        return 1;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {
        // if true then succesfull
    }
}
