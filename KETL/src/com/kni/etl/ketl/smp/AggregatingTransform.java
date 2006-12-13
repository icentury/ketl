package com.kni.etl.ketl.smp;

import com.kni.etl.util.aggregator.Aggregator;

public interface AggregatingTransform {

    Aggregator[] getAggregates();

}
