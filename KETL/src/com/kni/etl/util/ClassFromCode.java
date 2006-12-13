package com.kni.etl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.kni.etl.ETLJob;
import com.kni.etl.dbutils.ResourcePool;
import com.sun.tools.javac.Main;

public class ClassFromCode {

    static class NetworkClassLoader extends ClassLoader {

        public Class getClass(String pFileName, String pClassName) throws IOException {
            byte[] b = loadClassData(pFileName);

            return defineClass(pClassName, b, 0, b.length);
        }

        private byte[] loadClassData(String pFileName) throws IOException {
            // load the class data from the connection
            File javafile = new File(pFileName);

            // Create the byte array to hold the data
            byte[] buf = new byte[(int) javafile.length()]; // file is object of java.io.File for which you want the
            // byte array

            FileInputStream is = new FileInputStream(javafile);

            // Read in the bytes
            int offset = 0;
            int numRead = 0;

            while ((offset < buf.length) && ((numRead = is.read(buf, offset, buf.length - offset)) >= 0)) { // is is the
                // fileinputstream
                offset += numRead;
            }

            return buf;
        }
    }

    static boolean rebuildClass = true;

    public static Class getDynamicClass(ETLJob ejCurrentJob, String classCode, String className, boolean reuseExisting,
            boolean forceCompilation) throws ClassCompileException, IOException {

        
        String tempdir = System.getProperty("java.io.tmpdir");

        if ((tempdir != null) && (tempdir.endsWith("/") == false) && (tempdir.endsWith("\\") == false)) {
            tempdir = tempdir + "/";
        }
        else if (tempdir == null) {
            tempdir = "";
        }

        String mJavaPackageRootDir = tempdir + "job";
        String mJavaPackageDir = mJavaPackageRootDir + File.separatorChar + ejCurrentJob.getJobID();
        String mJavaFileName = mJavaPackageDir + File.separatorChar + className + ".java";
        String mJavaByteCodeFileName = mJavaPackageDir + File.separatorChar + className + ".class";

        StringBuffer sb = new StringBuffer();

        try {
            File packageDir = new File(mJavaPackageRootDir);
            if (packageDir.exists()) {
                if (packageDir.isDirectory() == false) {
                    throw new ClassCompileException("Job package root directory exists as file already: - "
                            + packageDir.getAbsolutePath());
                }
            }
            else {
                packageDir.mkdir();
            }

            packageDir = new File(mJavaPackageDir);

            if (packageDir.exists()) {
                if (packageDir.isDirectory() == false) {
                    throw new ClassCompileException("Job package directory exists as file already: - "
                            + packageDir.getAbsolutePath());
                }
            }
            else {
                packageDir.mkdir();
            }

            File javaCodeFile = new File(mJavaFileName);

            if (forceCompilation == false && javaCodeFile.exists()) {
                File byteCodeFile = new File(mJavaByteCodeFileName);

                // bytecode more recent than java code then assume uptodate but lets check the code anyway
                if (byteCodeFile.exists() && (byteCodeFile.lastModified() > javaCodeFile.lastModified())) {
                    FileReader inputFileReader = new FileReader(javaCodeFile);
                    int c;

                    while ((c = inputFileReader.read()) != -1) {
                        sb.append((char) c);
                    }

                    // if code matches then reuse existing
                    if (sb.toString().equals(classCode)) {
                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "Compiled copy already exists, using "
                                + mJavaByteCodeFileName);
                        reuseExisting = true;                                                
                    }
                }
            }
        } catch (FileNotFoundException e) {
            reuseExisting = false;
        }

        // reuseExisting = true;
        if (reuseExisting == false) {
            FileOutputStream out; // declare a file output object
            PrintStream p; // declare a print stream object

            // Create a new file output stream
            out = new FileOutputStream(mJavaFileName);

            // Connect print stream to the output stream
            p = new PrintStream(out);

            p.print(classCode);

            p.close();
        }

        String[] javaArgs = new String[] { "-classpath", System.getProperty("java.class.path"), mJavaFileName };
        int iReturnValue = 0;
        if (reuseExisting == false) {
            StringWriter st = new StringWriter();
            PrintWriter out = new PrintWriter(st);
            iReturnValue = Main.compile(javaArgs, out);

            if (reuseExisting == false) {

                // jsJobStatus.setErrorCode(iReturnValue); // Set the return value as the error code
                if (iReturnValue != 0) {
                    throw new ClassCompileException("Compilation error, see below\n" + st.toString());
                }

            }
        }
        if (iReturnValue == 0) {
            // instantiate class
            NetworkClassLoader n = new NetworkClassLoader();

            Class cl = n.getClass(mJavaByteCodeFileName, "job." + ejCurrentJob.getJobID() + "." + className);

            return cl;
        }

        return null;
    }

    static public Class getClass(String className, String classCode) throws ClassCompileException, IOException {

        String tempdir = System.getProperty("java.io.tmpdir");
        if ((tempdir != null) && (tempdir.endsWith("/") == false) && (tempdir.endsWith("\\") == false)) {
            tempdir = tempdir + File.separator;
        }
        else if (tempdir == null) {
            tempdir = "";
        }

        String mJavaFileName = tempdir + className + ".java";
        String mClassFileName = tempdir + className + ".class";

        if (rebuildClass) {
            // Create a new file output stream
            // connected to "myfile.txt"
            FileOutputStream out = new FileOutputStream(mJavaFileName);

            // Connect print stream to the output stream
            PrintStream p = new PrintStream(out);

            p.print(classCode);

            p.close();

            String[] javaArgs = new String[] { "-classpath", System.getProperty("java.class.path"), mJavaFileName };

            StringWriter st = new StringWriter();
            PrintWriter pout = new PrintWriter(st);
            int iReturnValue = Main.compile(javaArgs, pout);

            // jsJobStatus.setErrorCode(iReturnValue); // Set the return value as the error code
            if (iReturnValue != 0) {
                throw new ClassCompileException("Dynamic transform contains an error, see below\n" + st.toString());
            }
        }
        NetworkClassLoader n = new NetworkClassLoader();

        Class cl = n.getClass(mClassFileName, className);

        File f = new File(mClassFileName);
        f.deleteOnExit();
        f = new File(mJavaFileName);
        f.deleteOnExit();

        return cl;

    }
}
