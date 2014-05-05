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
import java.io.FilenameFilter;
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
import com.kni.etl.ketl.reader.ETLReader;
import com.kni.etl.ketl.smp.ETLThreadGroup;
import com.kni.etl.ketl.smp.ETLTransform;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.ketl.smp.ManagedBlockingQueue;
import com.kni.util.AutoClassCaster;
import com.kni.util.Bytes;
import com.kni.util.FastSerializer;

/**
 * @author sjaini
 * 
 */
public class CheckPointStore {
	
	boolean useFastSerializer = false;

	private boolean compress = true;

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
					ResourcePool.logException(e);
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
				if (useFastSerializer)
					readInputStream();
				else
					readCompressedInputStream();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	private boolean checkpointExisted;
	private ETLStep step;

	File store, tmpStore;
	private LinkedBlockingQueue<Object> queue;

	public CheckPointStore(ETLStep step) throws KETLThreadException, IOException {
		this.step = step;
		File partDir = new File(EngineConstants.PARTITION_PATH + File.separator + step.getPartitionID());

		if (partDir.exists() && partDir.isDirectory() == false)
			throw new KETLThreadException("Partition part error, a file exists which is blocking the partition "
					+ partDir.getAbsolutePath(), this);
		else if (partDir.exists() == false)
			partDir.mkdir();

		store = getPartitionFile(step, partDir);
		if (store.exists() && 1 == 0) {
			this.checkpointExisted = true;
		} else {
			tmpStore = new File(store.getCanonicalPath() + ".loading");

			if (tmpStore.exists()) {
				tmpStore.delete();
				tmpStore = new File(store.getCanonicalPath() + ".loading");
			}
			this.checkpointExisted = false;
		}
	}

	private static File getPartitionFile(ETLStep step, File partDir) {
		String name = step.getJobID() + "." + step.getName() + "." + step.getJobExecutionID();
		File file = new File(partDir, name);
		return file;
	}

	public boolean checkpointEnabled(ETLStep step) {
		return step.isUseCheckPoint();
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
		if (useFastSerializer) {
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
		else 
			compressWrite(queue);
	}
	public static boolean wasTheStepExecutedSuccessfully(final ETLWorker step) {
		File partDir = new File(EngineConstants.PARTITION_PATH + File.separator + step.getPartitionID());
		File[] thisJobFiles = partDir.listFiles(getFileNameFilter(step));
		return thisJobFiles != null && thisJobFiles.length>0;
	}

	private static FilenameFilter getFileNameFilter(final ETLWorker step) {
		return new FilenameFilter(){

			public boolean accept(File dir, String name) {
					String[] nameParts = name.split("\\.");
					if(name.endsWith("loading"))
						return false;
					else if(nameParts.length>2 && nameParts[0].equals(step.getJobID()) && 
							(nameParts[1].equals(step.getName()) || step instanceof ETLReader) && 
							nameParts[2].equals(""+step.getJobExecutionID()))
						return true;
					else return false;
				
			}
			
		};
	}

	public LinkedBlockingQueue<Object> processCheckPoint(ETLStep thisStep, ManagedBlockingQueue managedBlockingQueue ) throws IOException, InterruptedException, ClassNotFoundException {
		LinkedBlockingQueue<Object> queue;
		if (checkpointEnabled(thisStep)) {
			
			if (!CheckPointStore.wasTheStepExecutedSuccessfully(thisStep)) {
				this.write((LinkedBlockingQueue<Object>)managedBlockingQueue);
			}
			this.read();
			queue = this.getOutputQueue();
		} else {
			queue = managedBlockingQueue;
		}
		return queue;
	}
}
interface ExceptionListener {
	  public void exceptionOccurred(Exception x, Object source);
}
