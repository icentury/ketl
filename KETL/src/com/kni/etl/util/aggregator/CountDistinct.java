package com.kni.etl.util.aggregator;

import java.util.HashSet;
import java.util.Set;

final public class CountDistinct extends Aggregator {

    private Set set = new HashSet();


    @Override
    public void add(Object arg0) {
        // TODO Auto-generated method stub
        set.add(arg0);
    }

    @Override
    public Object getValue() {
        Integer res = set.size();
        set.clear();
        return res;
    }

    @Override
    public Class getValueClass() {
        return Integer.class;
    }

}
