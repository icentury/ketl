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
package com.kni.etl.ketl.merge;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultMergeCore;
import com.kni.etl.ketl.smp.ETLMerge;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class Merge.
 */
public class Merge extends ETLMerge {

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new ETLMergePort(this, srcStep);
    }

    /**
     * The Class ETLMergePort.
     */
    class ETLMergePort extends ETLOutPort {

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLOutPort#generateCode(int)
         */
        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            if (this.isUsed() == false)
                return "";

            // must be pure code then do some replacing
            String baseCode = XMLHelper.getTextContent(this.getXMLConfig());

            if (baseCode == null || baseCode.length() == 0)
                baseCode = "null";
            else {
                String[] params = EngineConstants.getParametersFromText(baseCode);

                for (String element : params) {
                    ETLInPort inport = this.mesStep.getInPort(element);

                    if (inport == null) {
                        // get from parameter list
                        throw new KETLThreadException("Source port " + element + " for step "
                                + this.mesStep.getName() + " could not be found, has it been declared as an IN port", this);
                    }
                    else {
                        baseCode = EngineConstants.replaceParameter(baseCode, element, inport.generateReference());
                        return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this)
                                + "] = " + inport.getCodeGenerationReferenceObject() + "==null?null:" + baseCode;
                    }
                }
            }

            return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = "
                    + baseCode;

        }

        /**
         * Instantiates a new ETL merge port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public ETLMergePort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /**
     * Instantiates a new merge.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public Merge(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLMerge#getRecordExecuteMethodFooter()
     */
    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return pLeftInputRecords!=null&&pRightInputRecords!=null?" + DefaultMergeCore.SUCCESS_ADVANCE_BOTH
                + ":(pLeftInputRecords==null?" + DefaultMergeCore.SUCCESS_ADVANCE_RIGHT + ":"
                + DefaultMergeCore.SUCCESS_ADVANCE_LEFT + ");}";
    }
}
