/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Aug 5, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl_v1;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


public class FileClassLoader extends ClassLoader
{
    Exception mException;

    public Class getClass(String pFileName, String pClassName)
        throws Exception
    {
        byte[] b = loadClassData(pFileName);

        return defineClass(pClassName, b, 0, b.length);
    }

    private byte[] loadClassData(String pFileName) throws IOException
    {
        // load the class data from the connection
        File javafile = new File(pFileName);

        //          Create the byte array to hold the data
        byte[] buf = new byte[(int) javafile.length()]; //file is object of java.io.File for which you want the byte array

        FileInputStream is = new FileInputStream(javafile);

        //          Read in the bytes
        int offset = 0;
        int numRead = 0;

        while ((offset < buf.length) && ((numRead = is.read(buf, offset, buf.length - offset)) >= 0))
        { // is is the fileinputstream
            offset += numRead;
        }

        return buf;
    }
}
