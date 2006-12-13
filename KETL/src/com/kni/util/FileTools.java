package com.kni.util;

import java.io.File;

import com.kni.etl.stringtools.StringMatcher;


public class FileTools {

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
    
            for (int i = 0; i < list.length; i++) {
                if (list[i].isFile()) {
                    if (filePattern.match(list[i].getName())) {
                        if (fileNames == null) {
                            fileNames = new String[fieldCnt + 1];
                        }
                        else {
                            fieldCnt++;
    
                            String[] tmp = new String[fieldCnt + 1];
                            System.arraycopy(fileNames, 0, tmp, 0, fileNames.length);
                            fileNames = tmp;
                        }
    
                        fileNames[fieldCnt] = list[i].getPath();
                    }
                }
            }
        }
    
        return fileNames;
    }

}
