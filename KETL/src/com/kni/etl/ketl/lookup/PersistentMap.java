package com.kni.etl.ketl.lookup;

import java.util.Map;

public interface PersistentMap extends Map {

    String[] mValueFields = null;

    public abstract Object get(Object key, String pField);

    public abstract Object put(Object key, Object value);

    public abstract Class getStorageClass();

    public abstract void delete();

    public abstract void switchToReadOnlyMode();

    public abstract Object getItem(Object pkey) throws Exception;

    public abstract void commit(boolean force);

    public abstract String[] getValueFields();

    public abstract Class[] getValueTypes();

    public abstract Class[] getKeyTypes();

    public abstract int getCacheSize();

    public abstract String getName();

    public abstract void close();

    public abstract void closeCacheEnvironment();

}