package com.kni.etl.functions;

import java.util.HashMap;
import java.util.Map;

import com.kni.etl.ketl.IDCounter;
import com.kni.etl.ketl.exceptions.KETLThreadException;

public class Sequence {
	 static private Map<String,IDCounter> idCounters = new HashMap<String,IDCounter>();
	    
	    final static synchronized public long next(String name,long batchSize) throws KETLThreadException {
	    	try {	    		
				IDCounter idCounter = idCounters.get(name);
				if (idCounter == null) {
					idCounter = new IDCounter(name, batchSize);
					idCounters.put(name, idCounter);
				}
				
				return idCounter.incrementID();
				
			} catch (Exception e) {
				throw new KETLThreadException("IDCounter error " + e.getMessage(),
						e);
			}
	    }
}
