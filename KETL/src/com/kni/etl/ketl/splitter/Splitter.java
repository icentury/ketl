package com.kni.etl.ketl.splitter;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLSplit;
import com.kni.etl.ketl.smp.ETLThreadManager;

public class Splitter extends ETLSplit {

    public Splitter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    protected void close(boolean success) {
    }

}
