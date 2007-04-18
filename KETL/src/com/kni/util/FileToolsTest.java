package com.kni.util;

import junit.framework.TestCase;


public class FileToolsTest extends TestCase {

    public FileToolsTest(String name) {
        super(name);
    }

    public void testGetFilenames() {
       String[] res = FileTools.getFilenames("c:\\temp\\*\\*");
       
       for(String o:res){
           System.out.println(o);
       }
    }
/*
    public void testGetFilenames2() {
        String[] res = FileTools.getFilenames2("c:\\temp\\*");
        
        for(String o:res){
            System.out.println(o);
        }
    }
*/
}
