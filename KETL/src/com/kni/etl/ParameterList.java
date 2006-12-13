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
        mPath = strPath;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (this.hmParameterList == null)
            return mPath + "<Empty>";

        StringBuilder sb = new StringBuilder();
        for (Object o : this.hmParameterList.entrySet()) {
            if (sb.length() > 0)
                sb.append(", ");

            sb.append(((Map.Entry) o).getKey());
            sb.append(":");
            sb.append(((Map.Entry) o).getValue());
        }
        return mPath + ":" + sb.toString();
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 4:31:46 PM)
     * 
     * @return java.lang.Object
     * @param oKey java.lang.Object
     */
    public Object getParameter(Object oName) {
        if (hmParameterList == null) {
            return null;
        }

        return hmParameterList.get(oName);
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 4:33:44 PM)
     * 
     * @param oName java.lang.Object
     * @param oValue java.lang.Object
     */
    public void setParameter(Object oName, Object oValue) {
        if (hmParameterList == null) {
            hmParameterList = new HashMap();
        }

        hmParameterList.put(oName, oValue);
    }

    static public ArrayList recurseParameterList(Node xmlSourceNode, String strParameterListName) {
        return recurseParameterList(xmlSourceNode, strParameterListName, new HashMap(), new HashSet(), new ArrayList(),
                null);
    }

    static public ArrayList recurseParameterList(String strParameterListName) {
        return recurseParameterList(strParameterListName, new HashMap(), new HashSet(), new ArrayList(), null);
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
        HashMap newParameterValuesList = copyParameterValuesList(aParameterValuesList);
        boolean hasSub = false;
        aParentParameterLists = new HashSet(aParentParameterLists);

        // get a list of all the parameters in the parameter list
        String[] parametersInList = XMLHelper.getDistinctParameterNames(xmlSourceNode, strParameterListName);

        if (parametersInList != null) {

            // cycle through each parameter and add it to list of inherited parameters if it does not exist already
            for (int i = 0; i < parametersInList.length; i++) {
                if (newParameterValuesList.containsKey(parametersInList[i]) == false) {
                    newParameterValuesList.put(parametersInList[i], null);
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

                    for (int i = 0; i < subs.length; i++) {

                        if (aParentParameterLists.contains(subs[i])) {
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                    "Loop exists in sub parameter list(" + strParameterListName
                                            + ") pointing to itself at a lower level,"
                                            + " no more sub parameter lists will be searched in this branch.");

                        }
                        else {
                            aParentParameterLists.add(subs[i]);
                            recurseParameterList(xmlSourceNode, subs[i], newParameterValuesList, aParentParameterLists,
                                    aParameterStore, strPath);
                        }
                    }
                }
            }

        }

        if (hasSub == false) {
            storeParameterSet(newParameterValuesList, aParameterStore, strPath);
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
                for (int i = 0; i < reqs.length; i++)
                    ((Map.Entry) o).setValue(EngineConstants.replaceParameter((String) ((Map.Entry) o).getValue(),
                            reqs[i], (String) aParametersAndValues.get(reqs[i])));

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
        HashMap newParameterValuesList = copyParameterValuesList(aParameterValuesList);
        boolean hasSub = false;
        aParentParameterLists = new HashSet(aParentParameterLists);

        // get a list of all the parameters in the parameter list
        String[] parametersInList = XMLHelper.getDistinctParameterNames(null, strParameterListName);

        if (parametersInList != null) {

            // cycle through each parameter and add it to list of inherited parameters if it does not exist already
            for (int i = 0; i < parametersInList.length; i++) {
                if (newParameterValuesList.containsKey(parametersInList[i]) == false) {
                    newParameterValuesList.put(parametersInList[i], null);
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

                    for (int i = 0; i < subs.length; i++) {

                        if (aParentParameterLists.contains(subs[i])) {
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                    "Loop exists in sub parameter list(" + strParameterListName
                                            + ") pointing to itself at a lower level,"
                                            + " no more sub parameter lists will be searched in this branch.");

                        }
                        else {
                            aParentParameterLists.add(subs[i]);
                            recurseParameterList(subs[i], newParameterValuesList, aParentParameterLists,
                                    aParameterStore, strPath);
                        }
                    }
                }
            }

        }

        if (hasSub == false) {
            storeParameterSet(newParameterValuesList, aParameterStore, strPath);
        }

        return aParameterStore;
    }

}
