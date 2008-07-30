/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.filter;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.ketl.smp.ETLWorker;


// TODO: Auto-generated Javadoc
/**
 * The Class Filter.
 */
public class Filter {

    /**
     * New filter.
     * 
     * @param filternodes the filternodes
     * @param step the step
     * 
     * @return the filter
     */
    public static Filter newFilter(Node[] filternodes,ETLWorker step) {
        // generate a class to do the filtering
        
        return null;
    }
    
    /**
     * Instantiates a new filter.
     * 
     * @param xmlConfig the xml config
     * @param step the step
     */
    public Filter(Element xmlConfig,ETLWorker step) {
        
    }

    /**
     * Filter.
     * 
     * @param data the data
     * @param len the len
     * 
     * @return the object[][]
     */
    public Object[][] filter(Object[][] data, int len) {
        // TODO Auto-generated method stub
        return null;
    }

}
