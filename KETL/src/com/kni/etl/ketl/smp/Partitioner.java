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
package com.kni.etl.ketl.smp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.util.SortedList;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class Partitioner.
 */
final public class Partitioner extends ManagedBlockingQueue {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The capacity. */
	private int mCapacity;

	/** The hash order. */
	private int[] mHashOrder;

	/** The out data. */
	private List[] mOutData;

	/** The hash length. */
	private int mHashLength = 0;

	/** The partition keys. */
	private Node[] mPartitionKeys;

	/** The destination queues. */
	private ManagedBlockingQueue[] mDestinationQueues;

	/** The dest queues. */
	private int mDestQueues = 0;

	/** The name. */
	private String name;

	/** The sorted. */
	private boolean mSorted;

	/**
	 * Instantiates a new partitioner.
	 * 
	 * @param partitionKeys
	 *            the partition keys
	 * @param sortComparator
	 *            the sort comparator
	 * @param targetPartitions
	 *            the target partitions
	 * @param capacity
	 *            the capacity
	 */
	public Partitioner(Node[] partitionKeys, Comparator sortComparator, int targetPartitions, int capacity) {
		// we don't use this queue and instead redirect
		super(1);

		this.mDestQueues = targetPartitions;

		this.mHashOrder = new int[partitionKeys.length];
		this.mOutData = new List[targetPartitions];
		this.mSorted = sortComparator != null;

		for (int i = 0; i < targetPartitions; i++)
			this.mOutData[i] = this.mSorted ? new SortedList(sortComparator) : new ArrayList();

		this.mPartitionKeys = partitionKeys;
		this.mHashLength = partitionKeys.length;
		this.mDestinationQueues = new ManagedBlockingQueue[targetPartitions];
		this.mCapacity = capacity;
	}

	/**
	 * The Class PartitionTargerManagedBlockingQueue.
	 */
	class PartitionTargerManagedBlockingQueue extends ManagedBlockingQueue {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/** The name. */
		private String name;

		/** The parent partitioner. */
		private Partitioner mParentPartitioner;

		/**
		 * The Constructor.
		 * 
		 * @param pCapacity
		 *            the capacity
		 * @param parentPartitioner
		 *            the parent partitioner
		 */
		public PartitionTargerManagedBlockingQueue(Partitioner parentPartitioner, int pCapacity) {
			super(pCapacity);
			this.mParentPartitioner = parentPartitioner;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.smp.MQueue#setName(java.lang.String)
		 */
		@Override
		public void setName(String arg0) {
			this.name = arg0;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.concurrent.LinkedBlockingQueue#toString()
		 */
		@Override
		public String toString() {
			return this.name == null ? "NA" : this.name + this.size();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.smp.MQueue#registerReader(com.kni.etl.ketl.smp.ETLWorker)
		 */
		@Override
		public void registerReader(ETLWorker worker) {
			this.mParentPartitioner.registerReader(worker);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.smp.MQueue#registerWriter(com.kni.etl.ketl.smp.ETLWorker)
		 */
		@Override
		public void registerWriter(ETLWorker worker) {
			this.mParentPartitioner.registerWriter(worker);
		}

	}

	/**
	 * Gets the target source queue.
	 * 
	 * @param i
	 *            the i
	 * 
	 * @return the target source queue
	 */
	public ManagedBlockingQueue getTargetSourceQueue(int i) {
		this.mDestinationQueues[i] = new PartitionTargerManagedBlockingQueue(this, this.mCapacity);
		return this.mDestinationQueues[i];
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ManagedBlockingQueue#registerReader(com.kni.etl.ketl.smp.ETLWorker)
	 */
	@Override
	public void registerReader(ETLWorker worker) {
		for (ETLInPort element : worker.mInPorts) {
			for (org.w3c.dom.Node element0 : this.mPartitionKeys) {
				if (element0 == element.getXMLConfig()) {
					int id = XMLHelper.getAttributeAsInt(element0.getAttributes(), "PARTITIONKEY", -1);
					this.mHashOrder[id - 1] = element.getSourcePortIndex();
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.LinkedBlockingQueue#toString()
	 */
	@Override
	public String toString() {
		return this.name == null ? "NA" : this.name + this.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ManagedBlockingQueue#registerWriter(com.kni.etl.ketl.smp.ETLWorker)
	 */
	@Override
	public void registerWriter(ETLWorker worker) {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ManagedBlockingQueue#setName(java.lang.String)
	 */
	@Override
	public void setName(String arg0) {
		this.name = arg0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.LinkedBlockingQueue#take()
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.MQueue#take()
	 */
	@Override
	public Object take() throws InterruptedException {
		throw new RuntimeException("Method should not be called");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.LinkedBlockingQueue#put(java.lang.Object)
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.MQueue#put(java.lang.Object)
	 */
	@Override
	public void put(Object pO) throws InterruptedException {
		if (pO == com.kni.etl.ketl.smp.ETLWorker.ENDOBJ) {

			if (this.mOutData[0] instanceof SortedList) {
				for (int i = 0; i < this.mDestQueues; i++) {

					((SortedList) this.mOutData[i]).releaseAll();
					if (this.mOutData[i].size() > 0) {
						Object[][] outData = new Object[((SortedList) this.mOutData[i]).fetchSize()][];
						this.mOutData[i].toArray(outData);
						this.mDestinationQueues[i].put(outData);
					}
				}
			}

			for (ManagedBlockingQueue element : this.mDestinationQueues) {
				element.put(pO);
			}
			return;
		}

		Object[][] batch = (Object[][]) pO;

		int size = batch.length;
		for (int r = 0; r < size; r++) {
			Object[] data = batch[r];
			int h = 1;
			for (int i = 0; i < this.mHashLength; i++) {
				int pos = this.mHashOrder[i];
				if (data[pos] == null)
					h += 1;
				if (data[pos] instanceof Number)
					h = h + ((Number) data[pos]).intValue();
				else
					h = 31 * h + data[pos].hashCode();
			}
			int x = Math.abs(h % this.mDestQueues);
			this.mOutData[Math.abs(h % this.mDestQueues)].add(data);
		}

		for (int i = 0; i < this.mDestQueues; i++) {
			if (this.mOutData[i].size() > 0) {

				Object[][] outData = new Object[this.mOutData[i].size()][];
				outData = (Object[][]) this.mOutData[i].toArray(outData);

				if (outData != null)
					this.mDestinationQueues[i].put(outData);

				if (this.mSorted == false)
					this.mOutData[i].clear();

			}
		}
	}

}
