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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadGroup;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.util.AutoClassCaster;
import com.kni.util.Bytes;
import com.kni.util.FastSerializer;

/**
 * @author sjaini
 * 
 */
public class CheckPointStore {

	private boolean compress = false;

	class Reader implements Runnable {

		private Thread parentThread;

		public Reader(Thread thread) {
			this.parentThread = thread;
		}

		private void readCompressedInputStream() throws FileNotFoundException, IOException, ClassNotFoundException,
				InterruptedException {
			Object returnValue;
			ObjectInputStream fis;
			try {

				if (compress)
					fis = new ObjectInputStream(
							new GZIPInputStream(new BufferedInputStream(new FileInputStream(store))));
				else
					fis = new ObjectInputStream(new BufferedInputStream(new FileInputStream(store)));

				try {
					while ((returnValue = fis.readObject()) != null) {
						queue.put(returnValue);
					}
				} catch (EOFException e) {

				}
				fis.close();
				queue.put(ETLWorker.ENDOBJ);

			} catch (Exception e) {
				ResourcePool.logException(e);
				step.setPendingException(e);
				Thread.currentThread();
				while (true) {
					Thread.sleep(1000);					
					if (parentThread.isAlive()) {
						step.interruptAllSteps();
					} else {
						return;
					}
				}
			}

		}

		private void readInputStream() throws ClassNotFoundException,
				InterruptedException, IOException {
			try {
				DataInputStream fis = new DataInputStream(new BufferedInputStream(new FileInputStream(store)));				
				try {
					while (true) {
						queue.put(FastSerializer.deserialize(fis));
					}
				} catch (EOFException e) {

				}
				fis.close();
				queue.put(ETLWorker.ENDOBJ);

			} catch (Exception e) {
				ResourcePool.logException(e);
				step.setPendingException(e);
				Thread.currentThread();
				while (true) {
					Thread.sleep(1000);
					if (parentThread.isAlive()) {
						step.interruptAllSteps();
					} else {
						return;
					}
				}
			}

		}

		public void run() {
			try {
				readInputStream();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	private boolean checkpointExisted;
	private ETLStep step;

	File store, tmpStore;
	private LinkedBlockingQueue<Object> queue;

	public CheckPointStore(ETLStep step) throws KETLThreadException {
		this.step = step;
		File partDir = new File(EngineConstants.PARTITION_PATH + File.separator + step.getPartitionID());

		if (partDir.exists() && partDir.isDirectory() == false)
			throw new KETLThreadException("Partition part error, a file exists which is blocking the partition "
					+ partDir.getAbsolutePath(), this);
		else if (partDir.exists() == false)
			partDir.mkdir();

		String name = step.getJobID() + "." + step.getName() + "." + step.getJobExecutionID();
		store = new File(partDir, name);
		if (store.exists() && 1 == 0) {
			this.checkpointExisted = true;
		} else {
			tmpStore = new File(partDir, name + ".loading");

			if (tmpStore.exists()) {
				tmpStore.delete();
				tmpStore = new File(partDir, name + ".loading");
			}
			this.checkpointExisted = false;
		}
	}

	public boolean checkpointEnabled() {
		return true;
	}

	public void compressWrite(LinkedBlockingQueue<Object> queue) throws IOException, InterruptedException {

		ObjectOutputStream bos;

		if (compress)
			bos = new ObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(tmpStore))));
		else
			bos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tmpStore)));

		Object object = queue.take();
		while (object != ETLWorker.ENDOBJ) {
			bos.writeObject(object);
			object = queue.take();
		}
		bos.flush();
		bos.close();

		// rename file now its loaded
		tmpStore.renameTo(store);
	}

	public boolean exists() {
		return false;
	}

	public LinkedBlockingQueue<Object> getOutputQueue() {
		return this.queue;
	}

	public void read() throws IOException, ClassNotFoundException, InterruptedException {
		this.queue = new LinkedBlockingQueue<Object>(ETLThreadGroup.DEFAULTQUEUESIZE);
		Thread reader = new Thread(step.getThreadManager().getJobThreadGroup(),new Reader(Thread.currentThread()));
		reader.setName("Checkpoint Reader [" + step.getJobID() + "." + step.getName() + " - ["
				+ step.getJobExecutionID() + "][" + step.getPartitionID() + "]");
		reader.start();
	}

	public void write(LinkedBlockingQueue<Object> queue) throws IOException, InterruptedException {
		step.setWaiting("checkpoint to load");
		
		DataOutputStream fos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(store)));
		Object object = queue.take();
		while (object != ETLWorker.ENDOBJ) {
			FastSerializer.serialize(object, fos);
			object = queue.take();
		}
		fos.flush();
		fos.close();
		 
		//this.compressWrite(queue);
		step.setWaiting(null);
	}
}
