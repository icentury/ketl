package com.kni.etl.ketl.lookup;

import java.io.File;

import com.kni.etl.EngineConstants;
import com.kni.etl.stringtools.NumberFormatter;

public class SleepycatIndexedMapTest extends IndexedMapTest {

    public SleepycatIndexedMapTest(String name) {
        super(name);
    }

 

    @Override
    PersistentMap getMap() {
        EngineConstants.getSystemXML();
        return new SleepycatIndexedMap("test" + this.getName(), NumberFormatter.convertToBytes(EngineConstants.getDefaultCacheSize()), 0,
                System.getProperty("user.dir") + File.separator + "log", new Class[] { Integer.class },
                new Class[] { String.class }, new String[] { "a" }, false);
    }

}
