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
package com.kni.etl.ketl;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import com.kni.etl.ETLJob;
import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.lookup.LookupCreatorImpl;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.ketl.smp.ETLCore;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/3/2002 6:26:39 PM)
 * 
 * @author: Administrator
 */
public class KETLJob extends ETLJob {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ETLJob#getJobChildNodes()
	 */

	private boolean paused = false;

	public boolean isPaused() {
		return paused;
	}

	public void pauseJob() {
		this.paused = true;
	}

	public void resumeJob() {
		this.paused = false;
	}

	/**
	 * SQLJob constructor comment.
	 * 
	 * @throws Exception
	 *             the exception
	 */
	public KETLJob() throws Exception {
		super();
	}

	/**
	 * Insert the method's description here. Creation date: (5/9/2002 2:28:24
	 * PM)
	 */
	@Override
	public void cleanup() {
		super.cleanup();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ETLJob#setChildNodes(org.w3c.dom.Node)
	 */
	@Override
	protected Node setChildNodes(Node pParentNode) {
		// turn file into readable nodes
		DocumentBuilder builder = null;
		Document xmlConfig;
		Node e = null;

		try {
			DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
			builder = dmf.newDocumentBuilder();

			Object action = this.getAction(false);

			if (action == null) {
				action = "<ACTION/>";
				ResourcePool.LogMessage("ERROR: XML Job " + this.getJobID() + " has no XML defined for its action.");
			}

			xmlConfig = builder.parse(new InputSource(new StringReader(action.toString())));
			e = pParentNode.getOwnerDocument().importNode(xmlConfig.getFirstChild(), true);
			pParentNode.appendChild(e);
		} catch (org.xml.sax.SAXException e2) {
			ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Parsing XML document, "
					+ e2.toString());

			System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
		} catch (Exception e1) {
			ResourcePool.LogException(e1, this);

			System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
		}

		return e;
	}

	/**
	 * Insert the method's description here. Creation date: (5/9/2002 12:06:44
	 * PM)
	 * 
	 * @throws Throwable
	 *             the throwable
	 */
	@Override
	protected void finalize() throws Throwable {
		// It's good practice to call the superclass's finalize() method,
		// even if you know there is not one currently defined...
		super.finalize();
	}

	/**
	 * Gets the shared lookup.
	 * 
	 * @param pLookupName
	 *            the lookup name
	 * @param owner
	 *            the owner
	 * 
	 * @return the shared lookup
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	final public Map getSharedLookup(String pLookupName, Object owner) throws KETLThreadException, InterruptedException {
		RegisteredLookup res = (RegisteredLookup) this.mLookups.get(pLookupName);

		if (res == null)
			res = ResourcePool.getLookup(pLookupName);

		if (res != null && res.writers > 0) {
			do {
				if (owner instanceof ETLCore) {
					((ETLCore) owner).getOwner().setWaiting("Lookup " + pLookupName);
				}
				Thread.sleep(500);

			} while (res.writers > 0);

			if (owner instanceof ETLCore) {
				((ETLCore) owner).getOwner().setWaiting(null);
			}

		} else if (res == null)
			throw new KETLThreadException("Lookup " + pLookupName + " does not exist", this);

		return res.lookup;
	}

	/**
	 * Register lookup write lock.
	 * 
	 * @param name
	 *            the name
	 * @param lookupImpl
	 *            the lookup impl
	 * @param pPersistence
	 *            the persistence
	 * 
	 * @return the persistent map
	 */
	final public synchronized PersistentMap registerLookupWriteLock(String name, LookupCreatorImpl lookupImpl,
			int pPersistence) {
		RegisteredLookup res = (RegisteredLookup) this.mLookups.get(name);

		if (res == null && pPersistence != EngineConstants.JOB_PERSISTENCE) {
			res = ResourcePool.getLookup(name);
			this.mLookups.put(name, res);
		}

		if (res == null) {

			res = new RegisteredLookup();
			res.name = name;
			res.lookup = lookupImpl.getLookup();
			res.writers++;
			res.persistence = pPersistence;
			res.mSourceLoadID = this.iLoadID;
			res.mSourceJobExecutionID = this.iJobExecutionID;

			this.mLookups.put(name, res);

			if (res.persistence != EngineConstants.JOB_PERSISTENCE) {
				ResourcePool.registerLookup(res);

			}
		} else
			res.writers++;

		return res.lookup;
	}

	/**
	 * Release lookup write lock.
	 * 
	 * @param name
	 *            the name
	 * @param lookupImpl
	 *            the lookup impl
	 */
	final public synchronized void releaseLookupWriteLock(String name, LookupCreatorImpl lookupImpl) {
		RegisteredLookup res = (RegisteredLookup) this.mLookups.get(name);

		if (res == null)
			return;

		if (res.writers == 1) {
			res.lookup = lookupImpl.swichToReadOnlyMode();
		}
		res.writers--;

	}

	/** The lookups. */
	private Map mLookups = new HashMap();

	/**
	 * Delete lookup.
	 * 
	 * @param name
	 *            the name
	 */
	public synchronized void deleteLookup(String name) {
		RegisteredLookup res = (RegisteredLookup) this.mLookups.get(name);

		if (res == null)
			return;
		res.delete();

		this.mLookups.remove(name);

	}
}
