package com.kni.etl.ketl.smp;

import java.util.concurrent.LinkedBlockingQueue;

public abstract class  ManagedBlockingQueue extends LinkedBlockingQueue {

    public ManagedBlockingQueue(int capacity) {
        super(capacity);
    }

    public abstract void setName(String arg0);

    public abstract void registerReader(ETLWorker worker);

    public abstract void registerWriter(ETLWorker worker);

    
}