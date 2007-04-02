/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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
import com.kni.etl.ketl.annotations.Attribute;
import com.kni.etl.ketl.annotations.Parameter;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.reader.SequenceGenerator.Counter;
import com.kni.etl.ketl.reader.SequenceGenerator.SequenceOutPort;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.DateAdd;
import com.kni.etl.util.XMLHelper;

public class HelloWorld extends ETLReader implements DefaultReaderCore {

    public HelloWorld(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Attribute(datatype = "INTEGER")
    public static final String VALUES = "VALUES";

    @Parameter()
    public static final String PHRASE = "PHRASE";

    private int mValueCounter = 0;
    private int mValuesRequested;
    private String mPhrase;

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
    class HelloWorldOutPort extends ETLOutPort {
        
        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }
        
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;
           
            return 0;
        }
        
        public HelloWorldOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    
    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new HelloWorldOutPort(this, srcStep);
    }
    
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

    @Override
    protected void close(boolean success) {
        // if true then succesfull
    }
}
