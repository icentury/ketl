/*
 * Created on Mar 24, 2006
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

import com.kni.etl.dbutils.ResourcePool;

final public class Store {

    private long mRecordsHeld = 0;
    private FileChannel mFileChannel = null;
    private FileInputStream mInputStream;
    private FileOutputStream mOutputStream;
    private String mTempFileName;
    private OutputStream mByteOutputStream;
    private ObjectOutputStream mObjectOutputStream;
    private InputStream mByteInputStream;
    private ObjectInputStream mObjectInputStream;

    public Store(int pWriteBufferSize) throws IOException {

        // get temp file
        File fd = File.createTempFile("KETL.", ".spool");
        mTempFileName = fd.getAbsolutePath();
        mOutputStream = new FileOutputStream(fd);
        mFileChannel = mOutputStream.getChannel();
        mByteOutputStream = java.nio.channels.Channels.newOutputStream(mFileChannel);
        mObjectOutputStream = new ObjectOutputStream(this.mByteOutputStream);
    }

    final public void add(Object pObject) throws IOException {

        mObjectOutputStream.writeObject(pObject);
        mRecordsHeld++;

    }

    final public void add(Object[] pObject, int len) throws IOException {

        for (int i = 0; i < len; i++) {
            mObjectOutputStream.writeObject(pObject[i]);
            mRecordsHeld++;
        }

    }

    final public void close() {

        this.writeClose();
        this.readClose();

        File fd = new File(mTempFileName);

        if (fd.exists()) {
            if (fd.delete() == false) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Problems deleting "
                        + fd.getAbsolutePath());
            }
        }
    }

    final private void writeClose() {
        try {
            mObjectOutputStream.close();
        } catch (Exception e) {
        }
        try {
            mByteOutputStream.close();
        } catch (Exception e) {
        }
        try {
            mFileChannel.close();
        } catch (Exception e) {
        }
        try {
            mOutputStream.close();
        } catch (Exception e) {
        }
    }

    final private void readClose() {
        try {
            mObjectInputStream.close();
        } catch (Exception e) {
        }
        try {
            mByteInputStream.close();
        } catch (Exception e) {
        }
        try {
            mFileChannel.close();
        } catch (Exception e) {
        }
        try {
            mInputStream.close();
        } catch (Exception e) {
        }
    }

    final public void commit() throws IOException {
        this.mObjectOutputStream.flush();
        this.mByteOutputStream.flush();
        this.writeClose();
    }

    final protected void finalize() throws Throwable {
        this.commit();
        this.close();
        super.finalize();
    }

    final public boolean hasNext() {
        return (mRecordsHeld == 0) ? false : true;
    }

    final public Object next() throws IOException, ClassNotFoundException {
        mRecordsHeld--;
        
       return this.mObjectInputStream.readObject();
    }

    final public void start(int readBufferSize, int objectBufferSize) throws IOException {
        /*
         * this.mCurrentObjectBufferSize = (recordsHeld < objectBufferSize) ? (int) recordsHeld : objectBufferSize;
         * this.mObjectBuffer = new Spoolable[this.mCurrentObjectBufferSize]; this.mObjectBufferPos =
         * this.mCurrentObjectBufferSize;
         */
        mInputStream = new FileInputStream(mTempFileName);

        mFileChannel = mInputStream.getChannel();
        mByteInputStream = java.nio.channels.Channels.newInputStream(mFileChannel);
        mObjectInputStream = new ObjectInputStream(mByteInputStream);                
    }



}