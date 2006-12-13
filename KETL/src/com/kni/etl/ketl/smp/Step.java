package com.kni.etl.ketl.smp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;

/**
 * 
 */

public class Step {

    private Element config;
    private HashMap threadGroup = new HashMap();
    private Class nodeClass;
    private String name;

    public Step(Element arg0, Class arg1, String arg2) {
        setConfig(arg0);
        setNodeClass(arg1);
        setName(arg2);
    }

    public String toString() {
        return this.name + " channels - " + Arrays.toString(threadGroup.keySet().toArray());
    }

    public void setConfig(Element config) {
        this.config = config;
    }

    public Node getConfig() {
        return config;
    }

    public void setThreadGroup(ETLThreadGroup threadGroup) {
        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Mapped " + this.name);

        this.threadGroup.put(threadGroup.getPortName(0), threadGroup);
    }

    public void setThreadGroups(ETLThreadGroup[] threadGroup) {
        for (int i = 0; i < threadGroup.length; i++) {
            ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Mapped " + this.name + " channel "
                    + threadGroup[i].getPortName(i));

            this.threadGroup.put(threadGroup[i].getPortName(i), threadGroup[i]);
        }
    }

    public ETLThreadGroup getThreadGroup(String port) throws KETLThreadException {

        if (this.threadGroup.containsKey(port) == false)
            throw new KETLThreadException("channel " + name + "." + port
                    + " does not exist, check step in channels to resolve error", this);

        Object o = this.threadGroup.get(port);

        if (o == null)
            throw new KETLThreadException("channel " + name + "." + port
                    + " has already be taken, use a splitter based step to resolve error", this);

        this.threadGroup.put(port, null);

        ETLThreadGroup et = (ETLThreadGroup) o;

        et.setQueueName(port);
        return et;
    }

    public void setNodeClass(Class nodeClass) {
        this.nodeClass = nodeClass;
    }

    public Class getNodeClass() {
        return nodeClass;
    }

    private void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

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