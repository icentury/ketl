package com.kni.etl.ketl.transformation;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;

final public class DynamicTransformation extends ETLTransformation {

    public DynamicTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub

    }

}
