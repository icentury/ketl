package com.kni.etl.ketl;

import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.DataItemHelper;
import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;
import com.kni.util.AutoClassCaster;

public class SystemConfigCache implements SystemConfig {

  class Parameter<T> {

    final T defaultValue;

    final String format;

    final String name;
    final boolean required;
    final int type;
    final List<T> validValues;

    public Parameter(String name, String type, List<String> validValues, boolean required,
        String defaultValue, String fmtStr) throws ParseException {
      super();
      this.name = name;
      this.type = DataItemHelper.getDataTypeIDbyName(type);
      this.format = fmtStr;

      Class cls = DataItemHelper.getClassForDataType(this.type);
      Format fmt = this.getFormat();

      this.required = required;
      this.defaultValue =
          defaultValue == null ? null : (T) AutoClassCaster.toObject(defaultValue, cls, fmt);

      this.validValues = new ArrayList<T>();

      for (String o : validValues) {
        this.validValues.add(o == null ? null : (T) AutoClassCaster.toObject(o, cls, fmt));
      }

    }

    public Format getFormat() {
      switch (this.type) {
        case DataItemHelper.DATE:
          return new SimpleDateFormat(format);
      }
      return null;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  enum ParameterType {
    ATTRIBUTE {
      @Override
      public String query() {
        return "ATTRIBUTES//ATTRIBUTE";
      }
    },
    INPORT {
      @Override
      public String query() {
        return "PORTS/IN//ATTRIBUTE";
      }
    },
    OUTPORT {
      @Override
      public String query() {
        return "PORTS/OUT//ATTRIBUTE";
      }
    },
    PARAMETER {
      @Override
      public String query() {
        return "PARAMETERS//PARAMETER";
      }
    },
    POST {
      @Override
      public String query() {
        return "PORTS/POSTSQL//ATTRIBUTE";
      }
    },
    POSTBATCH {
      @Override
      public String query() {
        return "PORTS/POSTBATCHSQL//ATTRIBUTE";
      }
    },
    PRE {
      @Override
      public String query() {
        return "PORTS/PRESQL//ATTRIBUTE";
      }
    },
    PREBATCH {
      @Override
      public String query() {
        return "PORTS/PREBATCHSQL//ATTRIBUTE";
      }
    };

    // template method
    public abstract String query();
  }

  class SystemConfigGroup {

    final String name;

    final SystemConfigItem owner;
    final Map<String, Parameter>[] parameters = new HashMap[ParameterType.values().length];
    final Map<String, Template> templates = new HashMap();

    public SystemConfigGroup(String name, SystemConfigItem owner) {
      this.name = name;
      this.owner = owner;
    }

    public void addParameter(ParameterType parameterType, String name, String type,
        boolean required, List<String> validValues, String defaultValue, String fmtStr)
        throws ParseException {

      if (parameters[parameterType.ordinal()] == null)
        parameters[parameterType.ordinal()] = new HashMap();

      parameters[parameterType.ordinal()].put(name, new Parameter(name, type, validValues,
          required, defaultValue, fmtStr));

    }

    public void addTemplate(String name, String value) {
      this.templates.put(name, new Template(name, value));
    }

    public SystemConfigGroup getDefault() {
      return this.owner.defaultGroup;
    }

    public Map<String, Parameter> getParameters(ParameterType parameter) {
      return this.parameters[parameter.ordinal()] == null ? new HashMap<String, Parameter>()
          : this.parameters[parameter.ordinal()];
    }

    public String getTemplate(String name) {
      Template res = this.templates.get(name);

      return res == null ? null : res.value;
    }

    @Override
    public String toString() {
      return this.name;
    }

    public Parameter getParameter(String pName, ParameterType pType) {
      Map<String, Parameter> res = this.getParameters(pType);

      return res.get(pName);
    }

  }

  class SystemConfigItem {
    final String className;

    final SystemConfigGroup defaultGroup;

    final String name;

    final Map<String, SystemConfigGroup> systemConfigGroups = new HashMap();

    public SystemConfigItem(Node node, Map<String, SystemConfigItem> stepConfigs)
        throws ParseException, KETLThreadException {
      super();
      this.className = XMLHelper.getAttributeAsString(node.getAttributes(), "CLASS", null);
      this.name = XMLHelper.getAttributeAsString(node.getAttributes(), "NAME", this.className);

      this.defaultGroup = new SystemConfigGroup("DEFAULT", this);
      this.systemConfigGroups.put("DEFAULT", this.defaultGroup);
      try {

        // load templates
        NodeList nl =
            (NodeList) mXPath.compile(".//TEMPLATE").evaluate(node, XPathConstants.NODESET);

        for (int i = 0; i < nl.getLength(); i++) {
          Node template = nl.item(i);
          SystemConfigGroup scg =
              this.getSystemConfigGroup(XMLHelper.getAttributeAsString(template.getParentNode()
                  .getAttributes(), "NAME", "DEFAULT"));
          String name = XMLHelper.getAttributeAsString(template.getAttributes(), "NAME", null);
          String value = XMLHelper.getTextContent(template);
          scg.addTemplate(name, value);
        }

        // load attributes and parameters

        for (ParameterType pType : ParameterType.values()) {
          String query = pType.query();
          nl = (NodeList) mXPath.compile(query).evaluate(node, XPathConstants.NODESET);

          for (int i = 0; i < nl.getLength(); i++) {
            Node attribute = nl.item(i);
            SystemConfigGroup scg =
                this.getSystemConfigGroup(XMLHelper.getAttributeAsString(attribute.getParentNode()
                    .getAttributes(), "NAME", "DEFAULT"));
            addAttributeSystemConfigGroup(attribute, scg, pType);
          }

        }

        // register step
        stepConfigs.put(this.className, this);
      } catch (XPathExpressionException e) {
        throw new KETLThreadException(e, Thread.currentThread());
      }

    }

    private void addAttributeSystemConfigGroup(Node parameter, SystemConfigGroup scg,
        ParameterType pType) throws ParseException, XPathExpressionException {
      String name = XMLHelper.getAttributeAsString(parameter.getAttributes(), "NAME", null);
      if (name.equalsIgnoreCase("INCREMENTALCOMMIT")) {
        int i;
        i = 0;
      }
      String type = XMLHelper.getAttributeAsString(parameter.getAttributes(), "TYPE", "STRING");
      if (type.equalsIgnoreCase("TYPE"))
        type = "STRING";

      String defaultValue =
          XMLHelper.getAttributeAsString(parameter.getAttributes(), "DEFAULT", null);
      boolean required =
          XMLHelper.getAttributeAsBoolean(parameter.getAttributes(), "REQUIRED", false);
      String fmtStr = XMLHelper.getAttributeAsString(parameter.getAttributes(), "FORMAT", null);
      List<String> validValues = new ArrayList();
      NodeList nl = (NodeList) mXPath.compile("VALUE").evaluate(parameter, XPathConstants.NODESET);
      for (int i = 0; i < nl.getLength(); i++) {
        validValues.add(XMLHelper.getTextContent(nl.item(i)));
      }
      scg.addParameter(pType, name, type, required, validValues, defaultValue, fmtStr);
    }

    private SystemConfigGroup getSystemConfigGroup(String name) {
      SystemConfigGroup scg = this.systemConfigGroups.get(name);
      if (scg == null) {
        scg = new SystemConfigGroup(name, this);
        this.systemConfigGroups.put(name, scg);
      }

      return scg;
    }

    @Override
    public String toString() {
      return this.name;
    }

  }

  class Template {
    final String name;

    final String value;

    public Template(String name, String value) {
      this.value = value;
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private final XPath mXPath;

  private final Map<String, SystemConfigItem> stepConfigs = new HashMap();;

  public SystemConfigCache() throws ParseException, KETLThreadException {
    Document doc = EngineConstants.getSystemXML();

    XPathFactory xpf = XPathFactory.newInstance();
    mXPath = xpf.newXPath();
    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - "
        + xpf.getClass().getCanonicalName());

    NodeList stepNodes = doc.getElementsByTagName("STEP");

    for (int i = 0; i < stepNodes.getLength(); i++) {
      new SystemConfigItem(stepNodes.item(i), stepConfigs);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.SystemConfig#getRequiredTags(java.lang.Class, java.lang.String)
   */
  public String[] getRequiredTags(Class callingClass, String pGroup) {
    SystemConfigGroup scg = getSystemGroup(pGroup, true, callingClass);

    Set<String> results = new HashSet();

    List<Parameter> list = new ArrayList();
    list.addAll(scg.getParameters(ParameterType.PARAMETER).values());
    list.addAll(scg.getDefault().getParameters(ParameterType.PARAMETER).values());

    if (list != null) {
      for (Parameter parameter : list) {
        if (parameter.required) {
          results.add(parameter.name);
        }
      }
    }

    return results.toArray(new String[results.size()]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.SystemConfig#getRequiredTags(java.lang.Class, java.lang.String)
   */
  public Parameter getParameterOfType(Class callingClass, String pGroup, String pName,
      ParameterType pType, boolean pDefaultAllowed) throws KETLThreadException {
    try {
      Parameter parameter;

      // use supplied group or default
      SystemConfigGroup scg = getSystemGroup(pGroup, pDefaultAllowed, callingClass);
      if ((parameter = scg.getParameter(pName, pType)) != null
          || (parameter = scg.getDefault().getParameter(pName, pType)) != null)
        return parameter;

      // was null again look in superclass
      callingClass = callingClass.getSuperclass();
      if (callingClass == null || callingClass == Object.class)
        throw new NullPointerException();

      return this.getParameterOfType(callingClass, pGroup, pName, pType, pDefaultAllowed);

    } catch (NullPointerException e) {
      throw new KETLThreadException("Parameter missing from system file CLASS=" + callingClass
          + " GROUP=" + pGroup + " NAME=" + pName, this);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.SystemConfig#getStepTemplate(java.lang.Class, java.lang.String,
   * java.lang.String, boolean)
   */
  public final String getStepTemplate(Class callingClass, String pGroup, String pName,
      boolean pDefaultAllowed) throws KETLThreadException {
    try {

      String template;

      // use supplied group or default
      SystemConfigGroup scg = getSystemGroup(pGroup, pDefaultAllowed, callingClass);
      if ((template = scg.getTemplate(pName)) != null
          || (template = scg.getDefault().getTemplate(pName)) != null)
        return template;

      // was null again look in superclass
      callingClass = callingClass.getSuperclass();
      if (callingClass == null || callingClass == Object.class)
        throw new NullPointerException();

      return this.getStepTemplate(callingClass, pGroup, pName, pDefaultAllowed);

    } catch (NullPointerException e) {
      throw new KETLThreadException("Template missing from system file CLASS=" + callingClass
          + " GROUP=" + pGroup + " NAME=" + pName, this);
    }
  }

  private SystemConfigGroup getSystemGroup(String pGroup, boolean pDefaultAllowed,
      Class callingClass) {
    SystemConfigItem stepItem = this.stepConfigs.get(callingClass.getCanonicalName());

    SystemConfigGroup scg = null;
    if (pGroup != null) {
      scg = stepItem.getSystemConfigGroup(pGroup);
    }
    if (scg == null && pDefaultAllowed) {
      scg = stepItem.defaultGroup;
    }
    return scg;
  }

  public static synchronized SystemConfig getInstance() {
    try {
      return new SystemConfigCache();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void refreshStepTemplates(Class className) {}
}
