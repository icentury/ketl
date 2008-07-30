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

import java.io.File;
import java.util.ArrayList;

import com.kni.etl.stringtools.StringMatcher;

// TODO: Auto-generated Javadoc
/**
 * The Class FileHelpers.
 */
final public class FileHelpers {

    /**
     * Gets the filenames.
     * 
     * @param searchPath the search path
     * 
     * @return the filenames
     */
    public static String[] getFilenames(String searchPath) {
        ArrayList alist = new ArrayList();

        if (searchPath == null) {
            return (null);
        }

        int lastPos = searchPath.lastIndexOf("/");

        if (lastPos == -1) {
            lastPos = searchPath.lastIndexOf("\\");
        }

        if (lastPos > 0) {
            String dirStr = searchPath.substring(0, lastPos);
            String fileSearch = searchPath.substring(lastPos + 1);

            StringMatcher filePattern = null;

            if (fileSearch != null) {
                filePattern = new StringMatcher(fileSearch);
            }
            else {
                return null;
            }

            File dir = new File(dirStr);

            if (dir.exists() == false) {
                return null;
            }

            File[] list = dir.listFiles();

            for (File element : list) {
                if (element.isFile()) {
                    if (filePattern.match(element.getName())) {
                        if (alist.contains(element.getPath()) == false)
                            alist.add(element.getPath());
                    }
                }
            }
        }

        String[] tmp = new String[alist.size()];
        alist.toArray(tmp);
        return tmp;
    }

}
