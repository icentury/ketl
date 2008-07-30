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
package com.kni.etl.ketl;

import org.w3c.dom.DOMException;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLOutPort.
 */
public class ETLOutPort extends ETLPort {

    
    /**
     * Instantiates a new ETL out port.
     * 
     * @param esOwningStep the es owning step
     * @param esSrcStep the es src step
     */
    public ETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
        super(esOwningStep, esSrcStep);
        // TODO Auto-generated constructor stub
    }



    /**
     * Gets the channel.
     * 
     * @return the channel
     */
    public String getChannel() {
        return (this.getXMLConfig()).getAttribute("CHANNEL");
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLPort#getPortName()
     */
    @Override
    public String getPortName() throws DOMException, KETLThreadException {
        if (this.mstrName != null)
            return this.mstrName;

        if (XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "CHANNEL", null) == null)
            (this.getXMLConfig()).setAttribute("CHANNEL", "DEFAULT");

        this.mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), ETLPort.NAME_ATTRIB, null);

        if (this.isConstant() == false && this.containsCode() == false) {
            ETLPort port = this.getAssociatedInPort();

            if (this.mstrName == null && port != null)
                (this.getXMLConfig()).setAttribute("NAME", port.mstrName);
        }

        return this.mstrName;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLPort#generateCode(int)
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
                    throw new KETLThreadException("Source port " + element + " for step " + this.mesStep.getName() + " could not be found, has it been declared as an IN port", this);
                }
                else {
                    baseCode = EngineConstants.replaceParameter(baseCode, element, inport.generateReference());
                }
            }
        }

        return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = " + baseCode;

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLPort#getCodeGenerationReferenceObject()
     */
    @Override
    public String getCodeGenerationReferenceObject() {
        return this.mesStep.getCodeGenerationOutputObject(this.getXMLConfig().getAttribute("CHANNEL"));

    }


}
