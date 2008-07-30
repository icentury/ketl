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

// TODO: Auto-generated Javadoc
/**
 * The Class Store.
 */
final public class Store {

    /** The records held. */
    private long mRecordsHeld = 0;
    
    /** The file channel. */
    private FileChannel mFileChannel = null;
    
    /** The input stream. */
    private FileInputStream mInputStream;
    
    /** The output stream. */
    private FileOutputStream mOutputStream;
    
    /** The temp file name. */
    private String mTempFileName;
    
    /** The byte output stream. */
    private OutputStream mByteOutputStream;
    
    /** The object output stream. */
    private ObjectOutputStream mObjectOutputStream;
    
    /** The byte input stream. */
    private InputStream mByteInputStream;
    
    /** The object input stream. */
    private ObjectInputStream mObjectInputStream;

    /**
     * Instantiates a new store.
     * 
     * @param pWriteBufferSize the write buffer size
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public Store(int pWriteBufferSize) throws IOException {

        // get temp file
        File fd = File.createTempFile("KETL.", ".spool");
        this.mTempFileName = fd.getAbsolutePath();
        this.mOutputStream = new FileOutputStream(fd);
        this.mFileChannel = this.mOutputStream.getChannel();
        this.mByteOutputStream = java.nio.channels.Channels.newOutputStream(this.mFileChannel);
        this.mObjectOutputStream = new ObjectOutputStream(this.mByteOutputStream);
    }

    /**
     * Add.
     * 
     * @param pObject the object
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    final public void add(Object pObject) throws IOException {

        this.mObjectOutputStream.writeObject(pObject);
        this.mRecordsHeld++;

    }

    /**
     * Add.
     * 
     * @param pObject the object
     * @param len the len
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    final public void add(Object[] pObject, int len) throws IOException {

        for (int i = 0; i < len; i++) {
            this.mObjectOutputStream.writeObject(pObject[i]);
            this.mRecordsHeld++;
        }

    }

    /**
     * Close.
     */
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

    /**
     * Write close.
     */
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

    /**
     * Read close.
     */
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

    /**
     * Commit.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    final public void commit() throws IOException {
        this.mObjectOutputStream.flush();
        this.mByteOutputStream.flush();
        this.writeClose();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    final protected void finalize() throws Throwable {
        this.commit();
        this.close();
        super.finalize();
    }

    /**
     * Checks for next.
     * 
     * @return true, if successful
     */
    final public boolean hasNext() {
        return (this.mRecordsHeld == 0) ? false : true;
    }

    /**
     * Next.
     * 
     * @return the object
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    final public Object next() throws IOException, ClassNotFoundException {
        this.mRecordsHeld--;

        return this.mObjectInputStream.readObject();
    }

    /**
     * Start.
     * 
     * @param readBufferSize the read buffer size
     * @param objectBufferSize the object buffer size
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
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