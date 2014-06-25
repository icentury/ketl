/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class ParameterList.
 */
public class ParameterList {

  /** The hm parameter list. */
  protected HashMap hmParameterList = null;

  /** The path. */
  private String mPath = "";

  private String mName;

  /**
   * Instantiates a new parameter list.
   * 
   * @param strPath the str path
   * @param strPath2
   */
  public ParameterList(String strName, String strPath) {
    super();
    this.mName = strName;
    this.mPath = strPath;
  }

  public String getName() {
    return this.mName;
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
   * @param oName the o name
   * 
   * @return java.lang.Object
   */
  public Object getParameter(Object oName) {
    if (this.hmParameterList == null) {
      return null;
    }

    return this.hmParameterList.get(oName);
  }

  public Map getParameters() {
    return java.util.Collections.unmodifiableMap(this.hmParameterList);
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

  /**
   * Recurse parameter list.
   * 
   * @param xmlSourceNode the xml source node
   * @param strParameterListName the str parameter list name
   * 
   * @return the array list
   */
  static public List recurseParameterList(Node xmlSourceNode, String strParameterListName) {
    return ParameterList.recurseParameterList(xmlSourceNode, strParameterListName, new HashMap(),
        new HashSet(), new ArrayList(), null);
  }

  /**
   * Recurse parameter list.
   * 
   * @param strParameterListName the str parameter list name
   * 
   * @return the array list
   */
  static public List recurseParameterList(String strParameterListName) {
    return ParameterList.recurseParameterList(strParameterListName, new HashMap(), new HashSet(),
        new ArrayList(), null);
  }

  /**
   * Copy parameter values list.
   * 
   * @param newParameterValuesList the new parameter values list
   * 
   * @return the hash map
   */
  static private Map copyParameterValuesList(Map<String, String> newParameterValuesList) {
    // duplicate hashmap but don't use same underlying parameter objects
    Map newMap = new HashMap();
    for (Map.Entry param : newParameterValuesList.entrySet()) {
      newMap.put(param.getKey(), param.getValue());

    }

    return newMap;
  }

  /**
   * Recurse parameter list.
   * 
   * @param xmlSourceNode the xml source node
   * @param strParameterListName the str parameter list name
   * @param aParameterValuesList the a parameter values list
   * @param aParentParameterLists the a parent parameter lists
   * @param aParameterStore the a parameter store
   * @param strPath the str path
   * 
   * @return the array list
   */
  static private List recurseParameterList(Node xmlSourceNode, String strParameterListName,
      Map<String, String> aParameterValuesList, Set aParentParameterLists, List aParameterStore,
      String strPath) {

    if (strPath == null)
      strPath = strParameterListName;
    else
      strPath = strPath + "->" + strParameterListName;

    // Duplicate list and add current parameter list
    // this helps protect against loops
    Map<String, String> newParameterValuesList =
        ParameterList.copyParameterValuesList(aParameterValuesList);
    boolean hasSub = false;
    aParentParameterLists = new HashSet(aParentParameterLists);

    // get a list of all the parameters in the parameter list
    String[] parametersInList =
        XMLHelper.getDistinctParameterNames(xmlSourceNode, strParameterListName);

    if (parametersInList != null) {

      for (String element : parametersInList) {
        if (newParameterValuesList.containsKey(element) == false) {
          newParameterValuesList.put(element, null);
        }
      }

      for (Object param : newParameterValuesList.entrySet()) {
        // get parameter value
        ((Map.Entry) param).setValue(XMLHelper.getParameterValueAsString(xmlSourceNode,
            strParameterListName, (String) ((Map.Entry) param).getKey(),
            (String) ((Map.Entry) param).getValue()));

      }

      for (Object param : newParameterValuesList.entrySet()) {
        // get sub parameter lists
        String[] subs =
            XMLHelper.getSubParameterListNames(xmlSourceNode, strParameterListName,
                (String) ((Map.Entry) param).getKey());

        if (subs != null) {
          hasSub = true;

          for (String element : subs) {

            if (aParentParameterLists.contains(element)) {
              ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                  "Loop exists in sub parameter list(" + strParameterListName
                      + ") pointing to itself at a lower level,"
                      + " no more sub parameter lists will be searched in this branch.");

            } else {
              aParentParameterLists.add(element);
              ParameterList.recurseParameterList(xmlSourceNode, element, newParameterValuesList,
                  aParentParameterLists, aParameterStore, strPath);
            }
          }
        }
      }

    }

    if (hasSub == false) {
      ParameterList.storeParameterSet(newParameterValuesList, aParameterStore, strPath,
          strParameterListName);
    }

    return aParameterStore;
  }

  /**
   * Path.
   * 
   * @return the string
   */
  public String path() {
    return this.mPath;
  }

  /**
   * Store parameter set.
   * 
   * @param aParametersAndValues the a parameters and values
   * @param aParameterStore the a parameter store
   * @param strPath the str path
   * @param strParameterListName
   * 
   * @return the int
   */
  static final protected int storeParameterSet(Map<String, String> aParametersAndValues,
      List aParameterStore, String strPath, String strParameterListName) {
    ParameterList parametersToStore = new ParameterList(strParameterListName, strPath);

    // parse values for any parameter substitution
    for (Map.Entry o : aParametersAndValues.entrySet()) {
      String reqs[] = EngineConstants.getParametersFromText((String) o.getValue());

      if (reqs != null)
        for (String element : reqs) {
          // only set if found
          if (aParametersAndValues.containsKey(element))
            o.setValue(EngineConstants.replaceParameter((String) o.getValue(), element,
                (String) aParametersAndValues.get(element)));
        }

      parametersToStore.setParameter(((Map.Entry) o).getKey(), ((Map.Entry) o).getValue());
    }

    aParameterStore.add(parametersToStore);

    return 1;
  }

  /**
   * Recurse parameter list.
   * 
   * @param strParameterListName the str parameter list name
   * @param aParameterValuesList the a parameter values list
   * @param aParentParameterLists the a parent parameter lists
   * @param aParameterStore the a parameter store
   * @param strPath the str path
   * 
   * @return the array list
   */
  static private List recurseParameterList(String strParameterListName,
      Map<String, String> aParameterValuesList, Set aParentParameterLists, List aParameterStore,
      String strPath) {

    if (strPath == null)
      strPath = strParameterListName;
    else
      strPath = strPath + "->" + strParameterListName;

    // Duplicate list and add current parameter list
    // this helps protect against loops
    Map<String, String> newParameterValuesList =
        ParameterList.copyParameterValuesList(aParameterValuesList);
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

      for (Map.Entry param : newParameterValuesList.entrySet()) {
        // get parameter value
        param.setValue(XMLHelper.getParameterValueAsString(null, strParameterListName,
            (String) param.getKey(), (String) param.getValue()));

      }

      for (Object param : newParameterValuesList.entrySet()) {
        // get sub parameter lists
        String[] subs =
            XMLHelper.getSubParameterListNames(null, strParameterListName,
                (String) ((Map.Entry) param).getKey());

        if (subs != null) {
          hasSub = true;

          for (String element : subs) {

            if (aParentParameterLists.contains(element)) {
              ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                  "Loop exists in sub parameter list(" + strParameterListName
                      + ") pointing to itself at a lower level,"
                      + " no more sub parameter lists will be searched in this branch.");

            } else {
              aParentParameterLists.add(element);
              ParameterList.recurseParameterList(element, newParameterValuesList,
                  aParentParameterLists, aParameterStore, strPath);
            }
          }
        }
      }

    }

    if (hasSub == false) {
      ParameterList.storeParameterSet(newParameterValuesList, aParameterStore, strPath,
          strParameterListName);
    }

    return aParameterStore;
  }

  public boolean hasParameter(String key) {
    return this.hmParameterList.containsKey(key);
  }

}
