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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

/**
 * The Class RegExFilterTransformation.
 */
public class RegExFilterTransformation extends ETLTransformation {

    /** The Constant REGEXPR_ATTRIB. */
    private static final String REGEXPR_ATTRIB = "REGEXPR";

    /**
     * Instantiates a new reg ex filter transformation.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public RegExFilterTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new RegExFilterOutPort(this, srcStep);
    }

    /**
     * The Class RegExFilterOutPort.
     */
    class RegExFilterOutPort extends ETLOutPort {

        /** The matcher. */
        Matcher matcher;
        
        /** The null matches. */
        boolean nullMatches = false;

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            String regexID = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                    RegExFilterTransformation.REGEXPR_ATTRIB, null);

            if (regexID != null) {
                String regex = XMLHelper.getChildNodeValueAsString(xmlConfig.getParentNode(), "REGEXPR", "ID", regexID,
                        null);
                if (regex != null) {

                    this.nullMatches = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), "NULLMATCHES",
                            this.nullMatches);

                    Pattern pattern = Pattern.compile(regex);
                    this.matcher = pattern.matcher("");
                }
            }

            String tmp = XMLHelper.getTextContent(xmlConfig);

            if (tmp == null || tmp.length() == 0)
                xmlConfig.setTextContent(EngineConstants.VARIABLE_PARAMETER_START
                        + XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NAME", null)
                        + EngineConstants.VARIABLE_PARAMETER_END);

            int res = super.initialize(xmlConfig);

            return res;

        }

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLOutPort#generateCode(int)
         */
        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            String tmp = super.generateCode(portReferenceIndex);

            if (this.matcher == null)
                return tmp;

            return tmp + ";if(((" + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).skipRecord("
                    + this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "],"
                    + this.mesStep.getUsedPortIndex(this) + ")) return SKIP_RECORD";
        }

        /**
         * Instantiates a new reg ex filter out port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public RegExFilterOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /**
     * Skip record.
     * 
     * @param datum the datum
     * @param portIdx the port idx
     * 
     * @return true, if successful
     */
    public boolean skipRecord(Object datum, int portIdx) {

        Matcher regex = ((RegExFilterOutPort) this.mOutPorts[portIdx]).matcher;

        if (datum == null) {
            if (((RegExFilterOutPort) this.mOutPorts[portIdx]).nullMatches == false)
                return true;

            return false;
        }
        regex.reset(datum.toString());
        if (regex.find() == false)
            return true;

        return false;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodHeader()
     */
    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        StringBuilder sb = new StringBuilder(super.getRecordExecuteMethodHeader());

        Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "FILTER", "*", "*");

        if (nl != null) {
            for (Node element : nl) {

                String code = XMLHelper.getTextContent(element);

                if (code == null || code.length() == 0)
                    throw new KETLThreadException("Filter tag requires an expression", this);

                String[] parms = EngineConstants.getParametersFromText(code);

                for (String element0 : parms) {
                    ETLInPort port = this.getInPort(element0);
                    code = EngineConstants.replaceParameter(code, element0, port.generateReference());
                }

                sb.append("if(!(" + code + ")) return SKIP_RECORD;");

            }
        }

        return sb.toString();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success, boolean jobFailed) {
        // TODO Auto-generated method stub

    }

}
