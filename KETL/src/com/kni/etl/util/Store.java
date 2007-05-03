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
        this.mTempFileName = fd.getAbsolutePath();
        this.mOutputStream = new FileOutputStream(fd);
        this.mFileChannel = this.mOutputStream.getChannel();
        this.mByteOutputStream = java.nio.channels.Channels.newOutputStream(this.mFileChannel);
        this.mObjectOutputStream = new ObjectOutputStream(this.mByteOutputStream);
    }

    final public void add(Object pObject) throws IOException {

        this.mObjectOutputStream.writeObject(pObject);
        this.mRecordsHeld++;

    }

    final public void add(Object[] pObject, int len) throws IOException {

        for (int i = 0; i < len; i++) {
            this.mObjectOutputStream.writeObject(pObject[i]);
            this.mRecordsHeld++;
        }

    }

    final public void close() {

        this.writeClose();
        this.readClose();

        File fd = new File(this.mTempFileName);

        if (fd.exists()) {
            if (fd.delete() == false) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Problems deleting "
                        + fd.getAbsolutePath());
            }
        }
    }

    final private void writeClose() {
        try {
            this.mObjectOutputStream.close();
        } catch (Exception e) {
        }
        try {
            this.mByteOutputStream.close();
        } catch (Exception e) {
        }
        try {
            this.mFileChannel.close();
        } catch (Exception e) {
        }
        try {
            this.mOutputStream.close();
        } catch (Exception e) {
        }
    }

    final private void readClose() {
        try {
            this.mObjectInputStream.close();
        } catch (Exception e) {
        }
        try {
            this.mByteInputStream.close();
        } catch (Exception e) {
        }
        try {
            this.mFileChannel.close();
        } catch (Exception e) {
        }
        try {
            this.mInputStream.close();
        } catch (Exception e) {
        }
    }

    final public void commit() throws IOException {
        this.mObjectOutputStream.flush();
        this.mByteOutputStream.flush();
        this.writeClose();
    }

    @Override
    final protected void finalize() throws Throwable {
        this.commit();
        this.close();
        super.finalize();
    }

    final public boolean hasNext() {
        return (this.mRecordsHeld == 0) ? false : true;
    }

    final public Object next() throws IOException, ClassNotFoundException {
        this.mRecordsHeld--;

        return this.mObjectInputStream.readObject();
    }

    final public void start(int readBufferSize, int objectBufferSize) throws IOException {
        /*
         * this.mCurrentObjectBufferSize = (recordsHeld < objectBufferSize) ? (int) recordsHeld : objectBufferSize;
         * this.mObjectBuffer = new Spoolable[this.mCurrentObjectBufferSize]; this.mObjectBufferPos =
         * this.mCurrentObjectBufferSize;
         */
        this.mInputStream = new FileInputStream(this.mTempFileName);

        this.mFileChannel = this.mInputStream.getChannel();
        this.mByteInputStream = java.nio.channels.Channels.newInputStream(this.mFileChannel);
        this.mObjectInputStream = new ObjectInputStream(this.mByteInputStream);
    }

}