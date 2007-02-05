package com.kni.etl.ketl;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

public class ETLOutPort extends ETLPort {

    
    public ETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
        super(esOwningStep, esSrcStep);
        // TODO Auto-generated constructor stub
    }



    public String getChannel() {
        return ((Element) this.getXMLConfig()).getAttribute("CHANNEL");
    }

    @Override
    public String getPortName() throws DOMException, KETLThreadException {
        if (this.mstrName != null)
            return this.mstrName;

        if (XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "CHANNEL", null) == null)
            ((Element) this.getXMLConfig()).setAttribute("CHANNEL", "DEFAULT");

        mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), NAME_ATTRIB, null);

        if (this.isConstant() == false && this.containsCode() == false) {
            ETLPort port = this.getAssociatedInPort();

            if (mstrName == null && port != null)
                ((Element) this.getXMLConfig()).setAttribute("NAME", port.mstrName);
        }

        return this.mstrName;
    }

    public String generateCode(int portReferenceIndex) throws KETLThreadException {

        if (this.isUsed() == false)
            return "";

        // must be pure code then do some replacing
        String baseCode = XMLHelper.getTextContent(this.getXMLConfig());

        if (baseCode == null || baseCode.length() == 0)
            baseCode = "null";
        else {
            String[] params = EngineConstants.getParametersFromText(baseCode);

            for (int i = 0; i < params.length; i++) {
                ETLInPort inport = this.mesStep.getInPort(params[i]);

                if (inport == null) {
                    // get from parameter list
                    throw new KETLThreadException("Source port " + params[i] + " for step " + this.mesStep.getName() + " could not be found, has it been declared as an IN port", this);
                }
                else {
                    baseCode = EngineConstants.replaceParameter(baseCode, params[i], inport.generateReference());
                }
            }
        }

        return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = " + baseCode;

    }

    public String getCodeGenerationReferenceObject() {
        return this.mesStep.getCodeGenerationOutputObject(this.getXMLConfig().getAttribute("CHANNEL"));

    }


}
