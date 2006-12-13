package com.kni.etl.ketl.lookup;

import java.io.File;

import com.kni.etl.EngineConstants;
import com.kni.etl.stringtools.NumberFormatter;

public class SleepycatIndexedMapTest extends IndexedMapTest {

    
    public SleepycatIndexedMapTest(String name) {
        super(name);

        EngineConstants.getSystemXML();
        map = new CachedIndexedMap(new SleepycatIndexedMap("test", NumberFormatter
				.convertToBytes(EngineConstants.getDefaultCacheSize()), 0, System.getProperty("user.dir")
                + File.separator + "log", new Class[] { Long.class, Float.class }, new Class[] { Long.class,
                Float.class }, new String[] { "a", "b" }, false));
    }

}
