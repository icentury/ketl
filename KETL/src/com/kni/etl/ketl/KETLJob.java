/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

    /**
     * SQLJob constructor comment.
     */
    public KETLJob() throws Exception {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 2:28:24 PM)
     */
    public void cleanup() {
        super.cleanup();
    }

    protected Node setChildNodes(Node pParentNode) {
        // turn file into readable nodes
        DocumentBuilder builder = null;
        Document xmlConfig;
        Node e = null;

        try {
            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            builder = dmf.newDocumentBuilder();

            Object action = this.getAction();

            if (action == null) {
                action = "<ACTION/>";
                ResourcePool.LogMessage("ERROR: XML Job " + this.getJobID() + " has no XML defined for its action.");
            }

            xmlConfig = builder.parse(new InputSource(new StringReader(action.toString())));
            e = pParentNode.getOwnerDocument().importNode(xmlConfig.getFirstChild(), true);
            pParentNode.appendChild(e);
        } catch (org.xml.sax.SAXException e2) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,"Parsing XML document, " + e2.toString());

            System.exit(EngineConstants.INVALID_XML_EXIT_CODE);
        } catch (Exception e1) {
            ResourcePool.LogException(e1, this);

            System.exit(EngineConstants.OTHER_ERROR_EXIT_CODE);
        }

        return e;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 12:06:44 PM)
     */
    protected void finalize() throws Throwable {
        // It's good practice to call the superclass's finalize() method,
        // even if you know there is not one currently defined...
        super.finalize();
    }

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

        }
        else if (res == null)
            throw new KETLThreadException("Lookup " + pLookupName + " does not exist", this);

        return res.lookup;
    }

    final public synchronized PersistentMap registerLookupWriteLock(String name, LookupCreatorImpl lookupImpl,
            int pPersistence) {
        RegisteredLookup res = (RegisteredLookup) this.mLookups.get(name);

        if(res == null && pPersistence != EngineConstants.JOB_PERSISTENCE)
        {    res = ResourcePool.getLookup(name);
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
        }
        else
            res.writers++;

        return res.lookup;
    }

    final public synchronized void releaseLookupWriteLock(String name, LookupCreatorImpl lookupImpl) {
        RegisteredLookup res = (RegisteredLookup) this.mLookups.get(name);

        if (res.writers == 1){
            res.lookup = lookupImpl.swichToReadOnlyMode();
        }
        res.writers--;

    }

    private Map mLookups = new HashMap();

    public synchronized void deleteLookup(String name) {
        RegisteredLookup res = (RegisteredLookup) this.mLookups.get(name);

        if (res == null)
            return;
        res.delete();

        this.mLookups.remove(name);

    }
}
