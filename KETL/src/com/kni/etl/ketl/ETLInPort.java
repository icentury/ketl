package com.kni.etl.ketl;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

public class ETLInPort extends ETLPort {

    public ETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
        super(esOwningStep, esSrcStep);
    }

    @Override
    public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
        int res = super.initialize(xmlConfig);
        if(res != 0)
            return res;
        
        String sort = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "SORT", null);
        
        if(sort == null)
            this.setSort(NO_SORT);
        else if(sort.equalsIgnoreCase("ASC"))
            this.setSort(ASC);
        else if(sort.equalsIgnoreCase("DESC"))
            this.setSort(DESC);
        else
            throw new KETLThreadException("Invalid sort value: " + sort, this);
               
        return 0;
    }

    private String mObjectName;
    
    public void setCodeGenerationReferenceObject(String obj) {
        mObjectName = obj;
    }
    

    final public int getSourcePortIndex() {
        return this.src.getPortIndex();
    }
    
    @Override
    public String getPortName() throws KETLThreadException {
        if (this.mstrName != null)
            return this.mstrName;

        String channel, port;

        mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), NAME_ATTRIB, null);

        if (this.isConstant() == false) {
            if (src == null) {
                String txt = XMLHelper.getTextContent(this.getXMLConfig());
                txt = txt.trim();

                String[] sources = txt.split("\\.");

                if (sources == null || sources.length == 1 || sources.length > 3)
                    throw new KETLThreadException("IN port definition invalid: \"" + txt + "\"", this);

                channel = sources.length == 3 ? sources[1] : "DEFAULT";
                port = sources.length == 3 ? sources[2] : sources[1];
                src = this.mesSrcStep.setOutUsed(channel, port);
            }

            if (mstrName == null) {
                ((Element) this.getXMLConfig()).setAttribute("NAME", src.mstrName);
                mstrName = src.mstrName;
            }
        } 
        
        if (this.mstrName == null)
            throw new KETLThreadException("In port must have a name, check step " + this.mesStep.getName() + ", XML: "
                    + XMLHelper.outputXML(this.getXMLConfig()), this);
        
        return this.mstrName;
    }

    
    final public static int NO_SORT = 0;    
    final public static int ASC = 1;
    final public static int DESC = 2;
   
    ETLOutPort src;
    private int sort = NO_SORT;

    public void setSourcePort(ETLOutPort srcPort) {
        src = srcPort;
    }

    
    public String generateReference() throws KETLThreadException {
        if(this.isConstant())
            return "const_" + this.mstrName;
        
        return "((" + this.getPortClass().getCanonicalName() + ")" + this.mObjectName + "[" + this.mesStep.getUsedPortIndex(this) + "])";
    }

    @Override
    public String getCodeGenerationReferenceObject() {
        return this.mObjectName;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public int getSort() {
        return sort;
    }

}
