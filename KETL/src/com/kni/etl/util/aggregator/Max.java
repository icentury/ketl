package com.kni.etl.util.aggregator;

public class Max extends Aggregator {

    Comparable cmp = null;

    @Override
    public void setValueClass(Class cl) throws AggregateException {
        super.setValueClass(cl);
        if (Comparable.class.isAssignableFrom(cl) == false)
            throw new AggregateException("Cannot perform max on class type: " + cl.getName());
    }

    @Override
    public void add(Object arg0) {
        if (arg0 == null)
            return;
        if (this.cmp == null || this.cmp.compareTo(arg0) < 0)
            this.cmp = (Comparable) arg0;
    }

    @Override
    public Object getValue() {
        Object res = this.cmp;
        this.cmp = null;
        return res;
    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
