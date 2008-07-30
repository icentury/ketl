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

import com.kni.etl.stringtools.StringMatcher;

// TODO: Auto-generated Javadoc
/**
 * The Class FileTools.
 */
public class FileTools {

    /**
     * Gets the filenames.
     * 
     * @param searchPath the search path
     * 
     * @return the filenames
     */
    public static String[] getFilenames(String searchPath) {
        String[] fileNames = null;

        if (searchPath == null) {
            return (null);
        }

        searchPath = searchPath.trim();

        int lastPos = searchPath.lastIndexOf("/");

        if (lastPos == -1) {
            lastPos = searchPath.lastIndexOf("\\");
        }

        int fieldCnt = 0;

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
                        if (fileNames == null) {
                            fileNames = new String[fieldCnt + 1];
                        }
                        else {
                            fieldCnt++;

                            String[] tmp = new String[fieldCnt + 1];
                            System.arraycopy(fileNames, 0, tmp, 0, fileNames.length);
                            fileNames = tmp;
                        }

                        fileNames[fieldCnt] = element.getPath();
                    }
                }
            }
        }

        return fileNames;
    }

}
