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
package com.kni.util;

import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class FileToolsTest.
 */
public class FileToolsTest extends TestCase {

    /**
     * Instantiates a new file tools test.
     * 
     * @param name the name
     */
    public FileToolsTest(String name) {
        super(name);
    }

    /**
     * Test get filenames.
     */
    public void testGetFilenames() {
        String[] res = FileTools.getFilenames("c:\\temp\\*\\*");

        for (String o : res) {
            System.out.println(o);
        }
    }
    /*
     * public void testGetFilenames2() { String[] res = FileTools.getFilenames2("c:\\temp\\*"); for(String o:res){
     * System.out.println(o); } }
     */
}
