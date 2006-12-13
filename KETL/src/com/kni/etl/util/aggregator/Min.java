package com.kni.etl.util.aggregator;

public class Min extends Aggregator {

    @Override
    public void setValueClass(Class cl) throws AggregateException {
        super.setValueClass(cl);
        if (Comparable.class.isAssignableFrom(cl) == false)
            throw new AggregateException("Cannot perform max on class type: " + cl.getName());
    }

    Comparable cmp = null;


    @Override
    public void add(Object arg0) {
        if (arg0 == null)
            return;
        if (cmp==null||cmp.compareTo(arg0) > 0)
            cmp = (Comparable) arg0;
    }

    @Override
    public Object getValue() {
        Object res = cmp;
        cmp = null;
        return res;
    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
