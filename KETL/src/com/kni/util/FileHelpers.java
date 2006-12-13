/*
 * Created on Apr 6, 2006
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.util;

import java.io.File;
import java.util.ArrayList;

import com.kni.etl.stringtools.StringMatcher;

final public class FileHelpers {

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
            } else {
                return null;
            }

            File dir = new File(dirStr);

            if (dir.exists() == false) {
                return null;
            }

            File[] list = dir.listFiles();

            for (int i = 0; i < list.length; i++) {
                if (list[i].isFile()) {
                    if (filePattern.match(list[i].getName())) {
                        if (alist.contains(list[i].getPath()) == false)
                            alist.add(list[i].getPath());
                    }
                }
            }
        }

        String[] tmp = new String[alist.size()];
        alist.toArray(tmp);
        return tmp;
    }


}
