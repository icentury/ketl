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

public class Aggregate extends ETLTransformation implements AggregatingTransform {

    public Aggregate(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    class AggregateETLOutPort extends ETLOutPort {

        Aggregator aggregator;

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {

            String function = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "FUNCTION", null);

            try {

                if (function == null) {
                    this.aggregator = new Direct();
                }
                else {
                    Class cl = Class.forName(function.contains(".") ? function : "com.kni.etl.util.aggregator." + function);
                    this.aggregator = (Aggregator) cl.newInstance();
                    
                    if(this.aggregator instanceof ToArray){
                        int limit = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), "MAXELEMENTS", -1);
                        if(limit > 0)
                            ((ToArray)this.aggregator).setArrayLimit(limit);
                    }
                }

            } catch (Exception e) {
                throw new KETLThreadException(e, this);
            }

            // instantiate aggregator class
            // determine if result always creates an array

            return super.initialize(xmlConfig);

        }

        @Override
        public Class getPortClass() throws AggregateException {
            Class cl = super.getPortClass();
            
            if(cl == null) return null;
            
            this.aggregator.setValueClass(cl);            
            return this.aggregator.getValueClass();
        }

        public AggregateETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new AggregateETLOutPort(this, srcStep);
    }

    public Aggregator[] getAggregates() {
        
        ArrayList res = new ArrayList();
        for (int i = 0; i < this.mOutPorts.length; i++)
            if(this.mOutPorts[i].isUsed())
            res.add( ((AggregateETLOutPort) this.mOutPorts[i]).aggregator);

        Aggregator[] result = new Aggregator[res.size()];
        res.toArray(result);
        return result;
    }

    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub
        
    }

}
