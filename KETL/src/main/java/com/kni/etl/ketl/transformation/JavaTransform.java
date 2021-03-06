package com.kni.etl.ketl.transformation;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.minidev.json.JSONObject;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.reader.JDBCReader;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.ketl.smp.JavaTransformCore;
import com.kni.etl.ketl.transformation.UserDefinedTransform.Input;
import com.kni.etl.ketl.transformation.UserDefinedTransform.Output;
import com.kni.etl.util.XMLHelper;

public class JavaTransform extends ETLTransformation implements JavaTransformCore, UDFConfiguration {

  @Override
  public int complete() throws KETLThreadException {
    if (this.isLastThreadToEnterCompletePhase()) {
      while (this.getThreadManager().countOfStepThreadsAlive(this) > 1) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new KETLError(e);
        }
      }
    }

    customTransform.complete();
    return super.complete();
  }

  private UserDefinedTransform customTransform;
  private JARClassLoader classLoader;

  public JavaTransform(Node pXMLConfig, int pPartitionID, int pPartition,
      ETLThreadManager pThreadManager) throws KETLThreadException {
    super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
  }

  @Override
  protected boolean alwaysOverrideOuts() {
    return true;
  }



  @Override
  protected ETLOutPort getNewOutPort(ETLStep srcStep) {
    return new JavaOutPort(this, srcStep);
  }

  class JavaOutPort extends ETLOutPort {

    @Override
    public boolean containsCode() throws KETLThreadException {
      return true;
    }

    public JavaOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
      super(esOwningStep, esSrcStep);
    }
  }



  class JARClassLoader extends URLClassLoader {

    public JARClassLoader(URL[] urls) {
      super(urls);
    }

    @Override
    public void addURL(URL url) {
      super.addURL(url);
    }

  }

  static private String CLASS_LOADER = "$CLASS_LOADER";

  @Override
  protected void overrideOuts(ETLWorker srcWorker) throws KETLThreadException {
    super.overrideOuts(srcWorker);

    String classPath = this.getParameterValue(0, "CLASSPATH");
    String className =
        XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "TRANSFORMCLASS", null);
    if (className == null)
      className = this.getParameterValue(0, "TRANSFORMCLASS");

    try {
      Class<?> cl;
      if (classPath != null) {
        // load jar from file
        // load class from jar
        cl = (Class<?>) this.getSharedResource(CLASS_LOADER);

        if (cl == null) {
          URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
          classLoader = new JARClassLoader(loader.getURLs());
          ResourcePool.logMessage("Reading customer transform " + classPath);
          URL url = new URL(classPath);
          classLoader.addURL(url);
          cl = classLoader.loadClass(className);
          this.setSharedResource(CLASS_LOADER, cl);
        }

      } else {
        cl = Class.forName(className);
      }

      this.customTransform = (UserDefinedTransform) cl.newInstance();
    } catch (Exception e) {
      throw new KETLThreadException("Error loading custom transform", e);
    }

    List<Input> inputs = new ArrayList<Input>();
    for (ETLOutPort out : srcWorker.getOutPorts()) {
      inputs.add(this.customTransform.newInput(out));
    }
    try {
      this.customTransform.setOptions(this.getTransformBatchManager(), this, this, this);
      this.customTransform.instantiate(inputs);
    } catch (Exception e) {
      throw new KETLThreadException("Error instantiating custom transform", e);
    }
    for (Input in : inputs) {
      if (in.used())
        srcWorker.setOutUsed(in.channel(), in.name());
    }


    if (outsResolved(this.getXMLConfig())) {
      ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE,
          "Outputs resolved in another partition");
      return;
    }

    Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), ETLStep.OUT_TAG, "*", "*");

    String channel = "DEFAULT";
    if (nl != null) {
      for (Node element : nl) {
        channel = XMLHelper.getAttributeAsString(element.getAttributes(), "CHANNEL", channel);
        this.getXMLConfig().removeChild(element);
      }
    }
    for (Output out : this.customTransform.getOutputs()) {
      Element newOut = this.getXMLConfig().getOwnerDocument().createElement(ETLStep.OUT_TAG);

      newOut.setAttribute("NAME", out.fieldName);
      newOut.setAttribute("DATATYPE", out.fieldClass.getName());
      newOut.setAttribute("CHANNEL", channel);
      this.getXMLConfig().appendChild(
          this.getXMLConfig().getOwnerDocument().importNode(newOut, true));
      ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE,
          "Adding port " + XMLHelper.getAttributeAsString(newOut.getAttributes(), "NAME", "N/A")
              + " as " + newOut.getAttribute("DATATYPE"));
    }
  }



  @Override
  public UserDefinedTransform getTransform() {
    return this.customTransform;
  }

  @Override
  protected void close(boolean success, boolean jobFailed) {
    if (classLoader != null)
      try {
        classLoader.close();
      } catch (IOException e) {
        throw new KETLError(e);
      }
  }

  @Override
  public String getAttribute(String arg0) {
    return XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), arg0, null);
  }

  private Map<String, Map<String, JSONObject>> valueMapCache =
      new HashMap<String, Map<String, JSONObject>>();

  @Override
  public Map<String, JSONObject> getValueMapping(String arg0) throws KETLTransformException {
    try {
      if (!this.valueMapCache.containsKey(arg0))
        this.valueMapCache.put(arg0, ResourcePool.getMetadata().getValueMapping(arg0));
      return this.valueMapCache.get(arg0);
    } catch (Exception e) {
      throw new KETLTransformException(e);
    }
  }

  @Override
  public String getParameter(String arg0) {
    int id = this.getParamaterLists(this.getAttribute("PARAMETER_LIST"));
    return this.getParameterValue(id, arg0);
  }

  @Override
  public Connection getConnection(String paramListName) throws KETLTransformException {
    int paramList = this.getParamaterLists(paramListName);

    Properties props;
    try {
      props = JDBCItemHelper.getProperties(this.getParameterListValues(paramList));

      return ResourcePool.getConnection(
          this.getParameterValue(paramList, DBConnection.DRIVER_ATTRIB),
          this.getParameterValue(paramList, DBConnection.URL_ATTRIB),
          this.getParameterValue(paramList, DBConnection.USER_ATTRIB),
          this.getParameterValue(paramList, DBConnection.PASSWORD_ATTRIB),
          this.getParameterValue(paramList, JDBCReader.PRESQL_ATTRIB), true, props);
    } catch (Exception e) {
      throw new KETLTransformException(e);
    }
  }

  public void releaseConnection(Connection con) {
    ResourcePool.releaseConnection(con);
  }

  @Override
  public String getTempDir() {
    return EngineConstants.PARTITION_PATH + File.separator + this.getPartitionID();
  }



}
