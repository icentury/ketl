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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;

// TODO: Auto-generated Javadoc
/**
 * The Class Step.
 */

public class Step {

    /** The config. */
    private Element config;
    
    /** The thread group. */
    private HashMap threadGroup = new HashMap();
    
    /** The node class. */
    private Class nodeClass;
    
    /** The name. */
    private String name;

    /**
     * Instantiates a new step.
     * 
     * @param arg0 the arg0
     * @param arg1 the arg1
     * @param arg2 the arg2
     */
    public Step(Element arg0, Class arg1, String arg2) {
        this.setConfig(arg0);
        this.setNodeClass(arg1);
        this.setName(arg2);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.name + " channels - " + Arrays.toString(this.threadGroup.keySet().toArray());
    }

    /**
     * Sets the config.
     * 
     * @param config the new config
     */
    public void setConfig(Element config) {
        this.config = config;
    }

    /**
     * Gets the config.
     * 
     * @return the config
     */
    public Node getConfig() {
        return this.config;
    }

    /**
     * Sets the thread group.
     * 
     * @param threadGroup the new thread group
     */
    public void setThreadGroup(ETLThreadGroup threadGroup) {
        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Mapped " + this.name);

        this.threadGroup.put(threadGroup.getPortName(0), threadGroup);
    }

    /**
     * Sets the thread groups.
     * 
     * @param threadGroup the new thread groups
     */
    public void setThreadGroups(ETLThreadGroup[] threadGroup) {
        for (int i = 0; i < threadGroup.length; i++) {
            ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Mapped " + this.name + " channel "
                    + threadGroup[i].getPortName(i));

            this.threadGroup.put(threadGroup[i].getPortName(i), threadGroup[i]);
        }
    }

    /**
     * Gets the thread group.
     * 
     * @param port the port
     * 
     * @return the thread group
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public ETLThreadGroup getThreadGroup(String port) throws KETLThreadException {

        if (this.threadGroup.containsKey(port) == false)
            throw new KETLThreadException("channel " + this.name + "." + port
                    + " does not exist, check step in channels to resolve error", this);

        Object o = this.threadGroup.get(port);

        if (o == null)
            throw new KETLThreadException("channel " + this.name + "." + port
                    + " has already be taken, use a splitter based step to resolve error", this);

        this.threadGroup.put(port, null);

        ETLThreadGroup et = (ETLThreadGroup) o;

        et.setQueueName(port);
        return et;
    }

    /**
     * Sets the node class.
     * 
     * @param nodeClass the new node class
     */
    public void setNodeClass(Class nodeClass) {
        this.nodeClass = nodeClass;
    }

    /**
     * Gets the node class.
     * 
     * @return the node class
     */
    public Class getNodeClass() {
        return this.nodeClass;
    }

    /**
     * Sets the name.
     * 
     * @param name the new name
     */
    private void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the unassigned channels.
     * 
     * @return the unassigned channels
     */
    public Object[] getUnassignedChannels() {
        ArrayList al = new ArrayList();
        for (Object o : this.threadGroup.entrySet()) {
            Map.Entry mp = (Map.Entry) o;
            if (mp.getValue() != null)
                al.add(mp.getKey());
        }

        if (al.size() == 0)
            return null;

        return al.toArray();
    }
}