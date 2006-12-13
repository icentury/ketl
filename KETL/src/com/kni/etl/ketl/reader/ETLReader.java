package com.kni.etl.ketl.reader;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;

abstract public class ETLReader extends com.kni.etl.ketl.smp.ETLReader {

    public ETLReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

}
