/**
 *  Copyright (C) 2008 Kinetic Networks, Inc. All Rights Reserved. 
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
package com.kni.etl.ketl.checkpointer;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.ketl.smp.ManagedBlockingQueue;
import com.kni.util.AutoClassCaster;

/**
 * @author sjaini
 * 
 */
public class CheckPointStore implements ExceptionListener{
	File store;
	private Set<ExceptionListener> exceptionListeners = Collections.synchronizedSet(new HashSet<ExceptionListener>());
	private int exceptionCount;

	public File getStoreFilePathRoot() {
		String path = EngineConstants.PARTITION_PATH;
		File partitionRootDir = new File(path);
		return partitionRootDir;
	}

	public void setStore(String relativeFilePath, boolean isNew) throws IOException, KETLException {

		store = new File(getStoreFilePathRoot().getPath() + relativeFilePath +".gz");
		if (!isNew && store.exists()) {
			throw new KETLException("An attempt to create a new file with a path that already exists is made for path" + relativeFilePath);
		} else
			store.createNewFile();
	}

	public void write(ManagedBlockingQueue managedBlockingQueue) throws IOException, InterruptedException {

		FileOutputStream fos = new FileOutputStream(store);
		Object object = managedBlockingQueue.take();
		while (object != ETLWorker.ENDOBJ) {
			byte[] byteCode = AutoClassCaster.serialize(object);
			fos.write(byteCode.length);
			fos.write(byteCode);
			object = managedBlockingQueue.take();
		}
		fos.flush();
		fos.close();

	}
	public void compressWrite(ManagedBlockingQueue managedBlockingQueue) throws IOException, InterruptedException {
		ObjectOutputStream bos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(store)));
		Object object = managedBlockingQueue.take();
		while (object != ETLWorker.ENDOBJ) {
			bos.writeObject(object);
			object = managedBlockingQueue.take();
		}
		bos.flush();
		bos.close();
	}
	
	

	class Reader extends Thread {

		private LinkedBlockingQueue<Object> managedBlockingQueue;

		public Reader(LinkedBlockingQueue<Object> managedBlockingQueue, ExceptionListener el) {
			super();
			this.addExceptionListener(el);
			this.managedBlockingQueue = managedBlockingQueue;
		}

		
		public void addExceptionListener(ExceptionListener l) {
		    if (l != null) {
		      exceptionListeners.add(l);
		    }
		  }

		public void run() {
			try {
				readCompressedInputStream();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void readInputStream() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
			Object returnValue;
			FileInputStream fis;
			fis = new FileInputStream(store);
			byte[] buf;
			int length;
			while ((length = fis.read()) != -1) {
				buf = new byte[length];
				fis.read(buf);
				returnValue = AutoClassCaster.deserialize(buf, 0, length);
				managedBlockingQueue.put(returnValue);
			}
			fis.close();
			managedBlockingQueue.put(ETLWorker.ENDOBJ);
		}
		private void readCompressedInputStream() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
			Object returnValue;
			ObjectInputStream fis;
			fis = new ObjectInputStream(new GZIPInputStream(new FileInputStream(store)));
			
			try {
				while ((returnValue =fis.readObject())!=null) {
					managedBlockingQueue.put(returnValue);
				}
			} catch (EOFException e) {
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "End of file reached" + store.getCanonicalPath());
			}
			fis.close();
			managedBlockingQueue.put(ETLWorker.ENDOBJ);
		}
		private void sendException(Exception x) {
		    if (exceptionListeners.size() == 0) {
		      x.printStackTrace();
		      return;
		    }

		    synchronized (exceptionListeners) {
		      Iterator iter = exceptionListeners.iterator();
		      while (iter.hasNext()) {
		        ExceptionListener l = (ExceptionListener) iter.next();

		        l.exceptionOccurred(x, this);
		      }
		    }
		  }

	}

	public void read(ManagedBlockingQueue managedBlockingQueue) throws IOException, ClassNotFoundException, InterruptedException {
		new Reader(managedBlockingQueue, this).start();
	}

	public void exceptionOccurred(Exception x, Object source) {
		 exceptionCount++;
		    System.err.println("EXCEPTION #" + exceptionCount + ", source="
		        + source);
		    x.printStackTrace();
		
	}
}
interface ExceptionListener {
	  public void exceptionOccurred(Exception x, Object source);
}

