/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;

public class ParameterList {

    protected HashMap hmParameterList = null;
    private String mPath = "";

    public ParameterList(String strPath) {
        super();
        this.mPath = strPath;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (this.hmParameterList == null)
            return this.mPath + "<Empty>";

        StringBuilder sb = new StringBuilder();
        for (Object o : this.hmParameterList.entrySet()) {
            if (sb.length() > 0)
                sb.append(", ");

            sb.append(((Map.Entry) o).getKey());
            sb.append(":");
            sb.append(((Map.Entry) o).getValue());
        }
        return this.mPath + ":" + sb.toString();
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 4:31:46 PM)
     * 
     * @return java.lang.Object
     * @param oKey java.lang.Object
     */
    public Object getParameter(Object oName) {
        if (this.hmParameterList == null) {
            return null;
        }

        return this.hmParameterList.get(oName);
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 4:33:44 PM)
     * 
     * @param oName java.lang.Object
     * @param oValue java.lang.Object
     */
    public void setParameter(Object oName, Object oValue) {
        if (this.hmParameterList == null) {
            this.hmParameterList = new HashMap();
        }

        this.hmParameterList.put(oName, oValue);
    }

    static public ArrayList recurseParameterList(Node xmlSourceNode, String strParameterListName) {
        return ParameterList.recurseParameterList(xmlSourceNode, strParameterListName, new HashMap(), new HashSet(),
                new ArrayList(), null);
    }

    static public ArrayList recurseParameterList(String strParameterListName) {
        return ParameterList.recurseParameterList(strParameterListName, new HashMap(), new HashSet(), new ArrayList(),
                null);
    }

    static private HashMap copyParameterValuesList(HashMap newParameterValuesList) {
        // duplicate hashmap but don't use same underlying parameter objects
        HashMap newMap = new HashMap();
        for (Object param : newParameterValuesList.entrySet()) {
            String tmp = (String) ((Map.Entry) param).getValue();

            if (tmp != null) {
                newMap.put(((Map.Entry) param).getKey(), tmp);
            }
        }

        return newMap;
    }

    static private ArrayList recurseParameterList(Node xmlSourceNode, String strParameterListName,
            HashMap aParameterValuesList, HashSet aParentParameterLists, ArrayList aParameterStore, String strPath) {

        if (strPath == null)
            strPath = strParameterListName;
        else
            strPath = strPath + "->" + strParameterListName;

        // Duplicate list and add current parameter list
        // this helps protect against loops
        HashMap newParameterValuesList = ParameterList.copyParameterValuesList(aParameterValuesList);
        boolean hasSub = false;
        aParentParameterLists = new HashSet(aParentParameterLists);

        // get a list of all the parameters in the parameter list
        String[] parametersInList = XMLHelper.getDistinctParameterNames(xmlSourceNode, strParameterListName);

        if (parametersInList != null) {

            for (String element : parametersInList) {
                if (newParameterValuesList.containsKey(element) == false) {
                    newParameterValuesList.put(element, null);
                }
            }

            for (Object param : newParameterValuesList.entrySet()) {
                // get parameter value
                ((Map.Entry) param).setValue(XMLHelper.getParameterValueAsString(xmlSourceNode, strParameterListName,
                        (String) ((Map.Entry) param).getKey(), (String) ((Map.Entry) param).getValue()));

            }

            for (Object param : newParameterValuesList.entrySet()) {
                // get sub parameter lists
                String[] subs = XMLHelper.getSubParameterListNames(xmlSourceNode, strParameterListName,
                        (String) ((Map.Entry) param).getKey());

                if (subs != null) {
                    hasSub = true;

                    for (String element : subs) {

                        if (aParentParameterLists.contains(element)) {
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                    "Loop exists in sub parameter list(" + strParameterListName
                                            + ") pointing to itself at a lower level,"
                                            + " no more sub parameter lists will be searched in this branch.");

                        }
                        else {
                            aParentParameterLists.add(element);
                            ParameterList.recurseParameterList(xmlSourceNode, element, newParameterValuesList,
                                    aParentParameterLists, aParameterStore, strPath);
                        }
                    }
                }
            }

        }

        if (hasSub == false) {
            ParameterList.storeParameterSet(newParameterValuesList, aParameterStore, strPath);
        }

        return aParameterStore;
    }

    public String path() {
        return this.mPath;
    }

    static final protected int storeParameterSet(HashMap aParametersAndValues, ArrayList aParameterStore, String strPath) {
        ParameterList parametersToStore = new ParameterList(strPath);

        // parse values for any parameter substitution
        for (Object o : aParametersAndValues.entrySet()) {
            String reqs[] = EngineConstants.getParametersFromText((String) ((Map.Entry) o).getValue());

            if (reqs != null)
                for (String element : reqs)
                    ((Map.Entry) o).setValue(EngineConstants.replaceParameter((String) ((Map.Entry) o).getValue(),
                            element, (String) aParametersAndValues.get(element)));

            parametersToStore.setParameter(((Map.Entry) o).getKey(), ((Map.Entry) o).getValue());
        }

        aParameterStore.add(parametersToStore);

        return 1;
    }

    static private ArrayList recurseParameterList(String strParameterListName, HashMap aParameterValuesList,
            HashSet aParentParameterLists, ArrayList aParameterStore, String strPath) {

        if (strPath == null)
            strPath = strParameterListName;
        else
            strPath = strPath + "->" + strParameterListName;

        // Duplicate list and add current parameter list
        // this helps protect against loops
        HashMap newParameterValuesList = ParameterList.copyParameterValuesList(aParameterValuesList);
        boolean hasSub = false;
        aParentParameterLists = new HashSet(aParentParameterLists);

        // get a list of all the parameters in the parameter list
        String[] parametersInList = XMLHelper.getDistinctParameterNames(null, strParameterListName);

        if (parametersInList != null) {

            for (String element : parametersInList) {
                if (newParameterValuesList.containsKey(element) == false) {
                    newParameterValuesList.put(element, null);
                }
            }

            for (Object param : newParameterValuesList.entrySet()) {
                // get parameter value
                ((Map.Entry) param).setValue(XMLHelper.getParameterValueAsString(null, strParameterListName,
                        (String) ((Map.Entry) param).getKey(), (String) ((Map.Entry) param).getValue()));

            }

            for (Object param : newParameterValuesList.entrySet()) {
                // get sub parameter lists
                String[] subs = XMLHelper.getSubParameterListNames(null, strParameterListName,
                        (String) ((Map.Entry) param).getKey());

                if (subs != null) {
                    hasSub = true;

                    for (String element : subs) {

                        if (aParentParameterLists.contains(element)) {
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                    "Loop exists in sub parameter list(" + strParameterListName
                                            + ") pointing to itself at a lower level,"
                                            + " no more sub parameter lists will be searched in this branch.");

                        }
                        else {
                            aParentParameterLists.add(element);
                            ParameterList.recurseParameterList(element, newParameterValuesList, aParentParameterLists,
                                    aParameterStore, strPath);
                        }
                    }
                }
            }

        }

        if (hasSub == false) {
            ParameterList.storeParameterSet(newParameterValuesList, aParameterStore, strPath);
        }

        return aParameterStore;
    }

}
