package com.kni.etl.ketl.lookup;

public interface LookupCreatorImpl {

    public abstract PersistentMap getLookup();

    public abstract PersistentMap swichToReadOnlyMode();

}