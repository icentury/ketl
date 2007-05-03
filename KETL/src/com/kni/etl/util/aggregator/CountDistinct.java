package com.kni.etl.util.aggregator;

import java.util.HashSet;
import java.util.Set;

final public class CountDistinct extends Aggregator {

    private Set set = new HashSet();

    @Override
    public void add(Object arg0) {
        // TODO Auto-generated method stub
        this.set.add(arg0);
    }

    @Override
    public Object getValue() {
        Integer res = this.set.size();
        this.set.clear();
        return res;
    }

    @Override
    public Class getValueClass() {
        return Integer.class;
    }

}
