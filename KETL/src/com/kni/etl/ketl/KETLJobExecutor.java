/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl;

import java.io.FileReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobExecutor;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.EngineConstants;
import com.kni.etl.ParameterList;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.qa.QACollection;
import com.kni.etl.ketl.smp.ETLMerge;
import com.kni.etl.ketl.smp.ETLReader;
import com.kni.etl.ketl.smp.ETLSplit;
import com.kni.etl.ketl.smp.ETLThreadGroup;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.ETLTransform;
import com.kni.etl.ketl.smp.ETLWriter;
import com.kni.etl.ketl.smp.Step;
import com.kni.etl.util.XMLHelper;

public class KETLJobExecutor extends ETLJobExecutor {

    public static final String CLASS_ATTRIB = "CLASS";
    public static final String STEP_TAG = "STEP";

    private static void checkForNonAssignedChannels(Object[] steps) throws KETLThreadException {
        for (int i = 0; i < steps.length; i++) {
            Object[] ports = ((Step) steps[i]).getUnassignedChannels();
            if (ports != null) {
                throw new KETLThreadException("Step '" + ((Step) steps[i]).getName() + "' channel(s) "
                        + Arrays.toString(ports) + " has not been assigned, please remove step or channel", Thread
                        .currentThread());
            }
        }
    }

    static private ArrayList getRequiredParametersFromValidNode(Node node, ArrayList list) {

        // ignore any parameter lists, as these should not be resolved
        if (com.kni.util.Arrays.searchArray(ETLStep.TAGS_NOT_SUPPORTING_PARAMETERS, node.getNodeName()) >= 0)
            return list;

        if (node.getNodeName().equals(EngineConstants.PARAMETER_LIST))
            return list;

        NodeList nl = node.getChildNodes();

        ArrayList al = new ArrayList();
        String txt = node.getNodeValue();
        String[] tmp = null;

        if (txt != null) {
            tmp = EngineConstants.getParametersFromText(txt);
        }
        if (tmp != null)
            Collections.addAll(al, (Object[]) tmp);

        for (int i = 0; i < nl.getLength(); i++) {
            getRequiredParametersFromValidNode(nl.item(i), list);
        }

        NamedNodeMap nm = node.getAttributes();

        if (nm != null) {
            for (int i = 0; i < nm.getLength(); i++) {
                Node attr = nm.item(i);
                txt = attr.getNodeValue();
                tmp = null;
                if (txt != null) {
                    tmp = EngineConstants.getParametersFromText(txt);
                }
                if (tmp != null)
                    Collections.addAll(al, (Object[]) tmp);
            }
        }

        for (int i = 0; i < al.size(); i++) {
            if (list.contains(al.get(i)) == false)
                list.add(al.get(i));
        }

        return list;
    }

    public static void main(String[] args) {
        ETLJobExecutor.execute(args, new KETLJobExecutor(), true);
    }

    // protected HashMap hmConnections = new HashMap();
    ETLJob ejCurrentJob = null;

    private ETLThreadManager em;

    /**
     * KETLJobExecutor constructor comment.
     */
    public KETLJobExecutor() {
        super();
    }

    private ETLThreadManager compileJob(Element job) throws ParserConfigurationException, SQLException, Exception {
        // create list of steps by type
        NodeList ls = job.getElementsByTagName("STEP");

        int batchSize = XMLHelper.getAttributeAsInt(job.getAttributes(), "BATCHSIZE", 1000);
        int queueSize = XMLHelper.getAttributeAsInt(job.getAttributes(), "QUEUESIZE", 5);

        HashMap writers = new HashMap();
        HashMap readers = new HashMap();
        HashMap transforms = new HashMap();
        HashMap splitters = new HashMap();
        HashMap mergers = new HashMap();
        ArrayList pendingInstantiation = new ArrayList();

        for (int i = 0; i < ls.getLength(); i++) {
            Node node = ls.item(i);

            if (node.hasAttributes() == false)
                continue;

            NamedNodeMap nmAttrs = node.getAttributes();

            String className = XMLHelper.getAttributeAsString(nmAttrs, "CLASS", null);

            ETLStep.setOutDefaults((Element) node);

            if (className == null)
                throw new KETLThreadException("Step has no class attribute, check XML", this);

            Class cl = Class.forName(className);

            String name = XMLHelper.getAttributeAsString(nmAttrs, "NAME", null);
            if (name == null)
                throw new KETLThreadException("Step has no name, check XML", this);

            Step step = new Step((Element) node, cl, name);

            // determine type of class
            if (ETLWriter.class.isAssignableFrom(cl))
                writers.put(name, step);
            else if (ETLReader.class.isAssignableFrom(cl))
                readers.put(name, step);
            else if (ETLTransform.class.isAssignableFrom(cl))
                transforms.put(name, step);
            else if (ETLSplit.class.isAssignableFrom(cl))
                splitters.put(name, step);
            else if (ETLMerge.class.isAssignableFrom(cl))
                mergers.put(name, step);

            // make sure each step has these set
            if (((Element) step.getConfig()).hasAttribute("BATCHSIZE") == false)
                ((Element) step.getConfig()).setAttribute("BATCHSIZE", Integer.toString(batchSize));
            if (((Element) step.getConfig()).hasAttribute("QUEUESIZE") == false)
                ((Element) step.getConfig()).setAttribute("QUEUESIZE", Integer.toString(queueSize));

            pendingInstantiation.add(step);
        }

        // instantiate thread manager
        em = new ETLThreadManager(this);

        // partitions
        int partitions = XMLHelper.getAttributeAsInt(job.getAttributes(), "PARRALLISM", 1);// Runtime.getRuntime().availableProcessors());

        if (partitions > 1)
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                    "PARRALLISM greater than 1 is not certified with this release");

        // hashmap of ready sources
        HashMap readySources = new HashMap();
        // instantiate readers
        for (Object o : readers.entrySet()) {
            Map.Entry node = ((Map.Entry) o);
            Step step = (Step) node.getValue();

            int instancePartitions = XMLHelper.getAttributeAsInt(step.getConfig().getAttributes(), "PARRALLISM",
                    partitions);

            if (instancePartitions != 1 && instancePartitions != partitions)
                throw new KETLThreadException("Reader parrallism must either be 1 or equal to the job parallism of "
                        + partitions, this);

            step.setThreadGroup(ETLThreadGroup.newInstance(null, ETLThreadManager.getThreadingType((Element) step
                    .getConfig()), step, instancePartitions, em));

            readySources.put(step.getName(), step);
            pendingInstantiation.remove(step);
        }

        // find all source requests in <IN> channels and see if the source is available
        while (pendingInstantiation.size() > 0) {
            int pendingSize = pendingInstantiation.size();

            for (Object o : pendingInstantiation) {
                Step currentStep = (Step) o;

                String[] sourceNames = ETLStep.getSource((Element) currentStep.getConfig());

                // if merger then we need to get the left and right source steps
                if (mergers.containsKey(currentStep.getName())) {
                    if (sourceNames.length != 2 || sourceNames[ETLStep.LEFT] == null
                            || sourceNames[ETLStep.RIGHT] == null)
                        throw new KETLThreadException("LEFT and RIGHT source need to be specified", this);

                    if (readySources.containsKey(sourceNames[ETLStep.LEFT])
                            && readySources.containsKey(sourceNames[ETLStep.RIGHT])) {
                        currentStep.setThreadGroup(ETLThreadGroup.newInstance(((Step) readySources
                                .get(sourceNames[ETLStep.LEFT])).getThreadGroup(ETLStep.getChannel(
                                (Element) currentStep.getConfig(), ETLStep.LEFT)), ((Step) readySources
                                .get(sourceNames[ETLStep.RIGHT])).getThreadGroup(ETLStep.getChannel(
                                (Element) currentStep.getConfig(), ETLStep.RIGHT)), ETLThreadGroup.PIPELINE_MERGE,
                                currentStep, partitions, em));
                        readySources.put(currentStep.getName(), currentStep);
                    }
                }
                else {
                    if (sourceNames.length != 1)
                        throw new KETLThreadException("Step " + currentStep.getName()
                                + " does not support multiple sources", this);

                    Step sourceStep = (Step) readySources.get(sourceNames[ETLStep.DEFAULT]);

                    if (sourceStep == null)
                        continue;

                    // if splitter then split into 2 thread groups
                    if (splitters.containsKey(currentStep.getName())) {
                        currentStep.setThreadGroups(ETLThreadGroup.newInstances(sourceStep.getThreadGroup(ETLStep
                                .getChannel((Element) currentStep.getConfig(), ETLStep.DEFAULT)), ETLStep
                                .getChannels((Element) currentStep.getConfig()), ETLThreadGroup.PIPELINE_SPLIT,
                                currentStep, partitions, em));
                        readySources.put(currentStep.getName(), currentStep);
                    }
                    else // if writer or normal transform then map straight through with single source
                    {
                        currentStep.setThreadGroup(ETLThreadGroup.newInstance(sourceStep.getThreadGroup(ETLStep
                                .getChannel((Element) currentStep.getConfig(), ETLStep.DEFAULT)), ETLThreadManager
                                .getThreadingType((Element) currentStep.getConfig()), currentStep, partitions, em));
                        readySources.put(currentStep.getName(), currentStep);
                    }
                }

            }

            // remove all ready steos from pending instantiation list
            pendingInstantiation.removeAll(readySources.values());

            if (pendingSize == pendingInstantiation.size())
                throw new KETLThreadException(
                        "Step channel mapping error, check xml for unknown sources, check the following steps for source reference errors "
                                + Arrays.toString(pendingInstantiation.toArray()), this);
        }

        // check tranforms, splitters and reader that they are assigned a destination
        checkForNonAssignedChannels(splitters.values().toArray());
        checkForNonAssignedChannels(readers.values().toArray());
        checkForNonAssignedChannels(transforms.values().toArray());

        return em;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:37:52 PM)
     * 
     * @return boolean
     * @param param com.kni.etl.ETLJob
     */
    protected boolean executeJob(ETLJob ejJob) {
        try {
            KETLJob kjJob;

            // DatabaseConnection dbConnection = null;
            ETLJobStatus jsJobStatus;

            // ETLReader erReader;
            // ETLWriter ewWriter;
            // DataItem adiRecordData[] = null;
            // int iInsertedRows = 0;
            DocumentBuilder builder = null;
            Document xmlDOM = null;

            if (this.ejCurrentJob != null) {
                System.out.println("Error: Cannot executeJob whilst job executing, job should of not been submitted");

                return false;
            }

            ejCurrentJob = ejJob;

            // Only accept KETL jobs...
            if ((ejJob instanceof KETLJob) == false) {
                ejCurrentJob = null;

                return false;
            }

            kjJob = (KETLJob) ejJob;
            jsJobStatus = kjJob.getStatus();

            // Build a DOM out of the XML string...
            try {
                builder = dmfFactory.newDocumentBuilder();

                String jobXML = (String) kjJob.getAction(true);

                if (this.aesOverrideParameters != null) {
                    for (int i = 0; i < this.aesOverrideParameters.size(); i++) {
                        String[] param = (String[]) this.aesOverrideParameters.get(i);

                        if ((param != null) && (param.length == 2)) {
                            jobXML = EngineConstants.replaceParameter(jobXML, param[0], param[1]);
                        }
                    }
                }

                xmlDOM = builder.parse(new InputSource(new StringReader(jobXML)));

                Document xmlParameterList = builder.parse(new InputSource(new StringReader("<ROOT>"
                        + this.msXMLOverride + "</ROOT>")));

                // parse XML and inherit references to external XML
                if (inheritReferencedXML(xmlDOM, xmlDOM, null, xmlParameterList) == false) {
                    jsJobStatus.setErrorCode(EngineConstants.ERROR_INHERITING_XML_CODE);

                    // CODES
                    jsJobStatus.setErrorMessage("Error inheriting Job XML, see log");

                    return false;
                }

                // fetch all parameter lists required for job in local cache
                this.ejCurrentJob.setParameterListCache(this.getParameterListsUsed(xmlDOM, new HashMap()));

                // parse XML and inherit references to external XML
                if ((replaceParameters(xmlDOM, new ArrayList())) == false) {
                    jsJobStatus.setErrorCode(EngineConstants.ERROR_REPLACING_PARAMETER_IN_XML_CODE);

                    // CODES
                    jsJobStatus.setErrorMessage("Error replacing parameter lists for Job XML, see log");

                    return false;
                }

                // disable QA jobs by setting disable attribute
                if (this.aesIgnoreQAs != null) {
                    for (int i = 0; i < this.aesIgnoreQAs.length; i++) {
                        // search for qa nodes by name and disable if in list
                        Node[] aQANodes = XMLHelper.findElementsByName(xmlDOM, QACollection.QA, ETLStep.NAME_ATTRIB,
                                aesIgnoreQAs[i]);

                        if (aQANodes != null) {
                            for (int x = 0; x < aQANodes.length; x++) {
                                if (aQANodes[x].getNodeType() == Node.ELEMENT_NODE) {
                                    Element elementNode = (Element) aQANodes[x];
                                    elementNode.setAttribute("IGNORE", "TRUE");
                                }
                            }
                        }

                        // for each qa node disable if child tag has name in list
                        NodeList qaNodeList = xmlDOM.getElementsByTagName(QACollection.QA);

                        for (int ni = 0; ni < qaNodeList.getLength(); ni++) {
                            NodeList qaNodeChildren = qaNodeList.item(ni).getChildNodes();

                            for (int nix = 0; nix < qaNodeChildren.getLength(); nix++) {
                                Node qaTypeNode = qaNodeChildren.item(nix);

                                if ((qaTypeNode != null)
                                        && (qaTypeNode.getNodeType() == Node.ELEMENT_NODE)
                                        && XMLHelper.getAttributeAsString(qaTypeNode.getAttributes(),
                                                ETLStep.NAME_ATTRIB, "_").equals(aesIgnoreQAs[i])) {
                                    Element elementNode = (Element) qaTypeNode;
                                    elementNode.setAttribute("IGNORE", "TRUE");
                                }
                            }
                        }
                    }
                }

                // printTree(document.getChildNodes().item(0), 0);
                // printTree(document.getElementsByTagName("SOURCE").item(0), 0);
            } catch (Exception e) {
                jsJobStatus.setErrorCode(EngineConstants.ERROR_READING_JOB_XML_CODE); // BRIAN: NEED TO SET UP KETL
                // JOB
                // ERROR

                // CODES
                jsJobStatus.setErrorMessage("Error reading job XML: " + e.getMessage());

                ResourcePool.LogException(e, this);

                ejCurrentJob = null;

                return false;
            }

            // Execute the step while catching all miscellaneous exceptions
            // so we can exit a little more gracefully...
            try {
                try {
                    em = compileJob((Element) xmlDOM.getElementsByTagName("ACTION").item(0));
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw (Exception) e.getCause();
                }
                em.start();

                if (this.mbCommandLine)
                    em.monitor(10, 1000);
                else
                    em.monitor(10, 100, jsJobStatus);

            } catch (KETLQAException e) {
                jsJobStatus.setErrorCode(e.getErrorCode()); // BRIAN: NEED TO SET UP KETL

                // JOB ERROR CODES
                jsJobStatus.setErrorMessage("Fatal QA error executing step '" + e.getETLStep().getName() + "'.");
                jsJobStatus.setException(e);
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, dumpExceptionCause(e));

                return false;
            } catch (KETLThreadException e) {
                jsJobStatus.setErrorCode(EngineConstants.OTHER_ERROR_EXIT_CODE); // BRIAN: NEED TO SET UP
                // KETL

                // JOB ERROR CODES
                jsJobStatus.setErrorMessage("Fatal error executing "
                        + (e.getSourceObject() instanceof ETLStep ? "step" : "") + " '"
                        + e.getSourceObject().toString() + "'.");
                jsJobStatus.setException(e);
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, dumpExceptionCause(e));

                return false;
            } catch (KETLReadException e) {
                jsJobStatus.setErrorCode(EngineConstants.OTHER_ERROR_EXIT_CODE); // BRIAN: NEED TO SET UP
                // KETL

                // JOB ERROR CODES
                jsJobStatus.setErrorMessage("Fatal error executing read step '" + e.getSourceThread().getName() + "'.");
                jsJobStatus.setException(e);
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, dumpExceptionCause(e));

                return false;
            } catch (KETLTransformException e) {
                jsJobStatus.setErrorCode(EngineConstants.OTHER_ERROR_EXIT_CODE); // BRIAN: NEED TO SET UP
                // KETL

                // JOB ERROR CODES
                jsJobStatus.setErrorMessage("Fatal error executing transform step '" + e.getSourceThread().getName()
                        + "'.");
                jsJobStatus.setException(e);
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, dumpExceptionCause(e));

                return false;
            } catch (KETLWriteException e) {
                jsJobStatus.setErrorCode(EngineConstants.OTHER_ERROR_EXIT_CODE); // BRIAN: NEED TO SET UP
                // KETL

                // JOB ERROR CODES
                jsJobStatus
                        .setErrorMessage("Fatal error executing write step '" + e.getSourceThread().getName() + "'.");
                jsJobStatus.setException(e);
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, dumpExceptionCause(e));

                return false;
            } catch (Throwable e) {

                jsJobStatus.setErrorCode(6); // BRIAN: NEED TO SET UP KETL

                // JOB ERROR CODES
                jsJobStatus.setErrorMessage("Fatal error executing - '" + e.getMessage() + "'.");
                jsJobStatus.setException(e);
                ResourcePool.LogException(e, this);
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, dumpExceptionCause(e));

                return false;
            }

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, em.finalStatus(jsJobStatus));

            return true;
        } finally {
            try{
                closeSteps();
            } finally {
                // clear job reference as job done, and new job can be excepted
                this.ejCurrentJob = null;
            }
        }

    }

    String dumpExceptionCause(Throwable pException) {

        StringBuilder res = new StringBuilder(pException.getMessage() == null ? "N/A" : pException.getMessage());
        Throwable e1 = pException.getCause();
        if (e1 != null && e1 instanceof SQLException) {
            SQLException e = ((SQLException) e1).getNextException();

            if (e != null) {
                do {
                    res.append("\n\tCaused by: " + e.getMessage());
                    if (e == e.getNextException())
                        e = null;
                    else
                        e = e.getNextException();
                } while (e != null);
            }
        }

        return res.toString();
    }

    /**
     * @return
     */
    public ETLJob getCurrentETLJob() {
        return ejCurrentJob;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLJobExecutor#getNewJob()
     */
    @Override
    protected ETLJob getNewJob() throws Exception {
        return new KETLJob();
    }

    private HashMap getParameterListsUsed(Node node, HashMap hm) {

        if (!(node.getNodeName().equalsIgnoreCase(EngineConstants.PARAMETER_LIST) || node.getNodeName()
                .equalsIgnoreCase(EngineConstants.PARAMETER))) {
            String lst = XMLHelper.getAttributeAsString(node.getAttributes(), EngineConstants.PARAMETER_LIST, null);
            if (lst != null && hm.containsKey(lst) == false)
                hm.put(lst, ParameterList.recurseParameterList(node, lst));
        }
        NodeList nl = node.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            this.getParameterListsUsed(nl.item(i), hm);
        }
        return hm;
    }

    boolean inheritReferencedXML(Document xmlDOM, Node xmlNode, String pParentParameterListName,
            Document pParameterLists) {
        NodeList nl = xmlNode.getChildNodes();
        ArrayList al = new ArrayList();

        mergeParameterLists(xmlDOM, pParameterLists, false);

        String strParameterListName = null;

        strParameterListName = XMLHelper.getAttributeAsString(xmlNode.getAttributes(), EngineConstants.PARAMETER_LIST,
                pParentParameterListName);

        // cycle through all nodes to get nodes needing inheritance
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);

            if (inheritReferencedXML(xmlDOM, n, strParameterListName, pParameterLists) == false) {
                return false;
            }

            String xmlSource = XMLHelper.getAttributeAsString(n.getAttributes(), ETLStep.XMLSOURCE_ATTRIB, null);

            if (xmlSource != null) {
                al.add(n);
            }
        }

        if (al.size() > 0) {
            for (int i = 0; i < al.size(); i++) {
                Node n = (Node) al.get(i);
                String xmlSource = XMLHelper.getAttributeAsString(n.getAttributes(), ETLStep.XMLSOURCE_ATTRIB, null);

                strParameterListName = XMLHelper.getAttributeAsString(n.getAttributes(),
                        EngineConstants.PARAMETER_LIST, pParentParameterListName);

                DocumentBuilder builder;

                try {
                    builder = dmfFactory.newDocumentBuilder();

                    Document tmpXMLDOM = builder.parse(new InputSource(new FileReader(xmlSource)));

                    String attrVal = XMLHelper.getAttributeAsString(n.getAttributes(), ETLStep.XMLSOURCENAME_ATTRIB,
                            null);

                    if (attrVal == null) {
                        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Inherited tag need an "
                                + ETLStep.XMLSOURCENAME_ATTRIB + " specified, check tag " + n);

                        return false;
                    }

                    String tagName = n.getNodeName();

                    Node xi = XMLHelper.findElementByName(tmpXMLDOM, tagName, ETLStep.NAME_ATTRIB, attrVal);

                    if (xi == null) {
                        ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Inherited tag <" + tagName + " "
                                + ETLStep.NAME_ATTRIB + "='" + attrVal + "'/> not found in " + xmlSource);

                        return false;
                    }

                    String tmpName = XMLHelper.getAttributeAsString(xi.getAttributes(), EngineConstants.PARAMETER_LIST,
                            strParameterListName);

                    // apply inheritance recursion
                    if (this.inheritReferencedXML(tmpXMLDOM, xi, tmpName, pParameterLists) == false) {
                        return false;
                    }

                    // check for parameters
                    String xmlString = XMLHelper.outputXML(xi);
                    String[] requiredParameters = EngineConstants.getParametersFromText(xmlString);

                    if ((requiredParameters != null) && (requiredParameters.length > 0)) {
                        if (strParameterListName == null) {
                            // No PARAMETER_LIST specified in source...
                            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                                    "Parameter list name required by inherited tag <" + tagName + " "
                                            + ETLStep.NAME_ATTRIB + "='" + attrVal + "'/> not found in xml tag");

                            return false;
                        }

                        for (int x = 0; x < requiredParameters.length; x++) {
                            String strParameter = requiredParameters[x];
                            String strParameterValue = null;

                            strParameterValue = XMLHelper.getParameterValueAsString(pParameterLists, tmpName,
                                    strParameter, null);

                            if (strParameterValue == null) {
                                // No PARAMETER_LIST specified in source...
                                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Parameter " + strParameter
                                        + " required by inherited tag <" + tagName + " " + ETLStep.NAME_ATTRIB + "='"
                                        + attrVal + "'/> not found in parameter list " + tmpName);

                                return false;
                            }

                            xmlString = EngineConstants.replaceParameter(xmlString, strParameter, strParameterValue);
                        }
                    }

                    tmpXMLDOM = builder.parse(new InputSource(new StringReader(xmlString)));

                    Node replacementNode = tmpXMLDOM.getFirstChild();

                    // get parent node and replace with inherited node
                    Node parentNode = n.getParentNode();

                    parentNode.replaceChild(xmlDOM.importNode(replacementNode, true), n);
                } catch (Exception e) {
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Problem inheriting code:" + e);
                    ResourcePool.LogException(e, this);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:55:23 AM)
     */
    protected boolean initialize() {
        // No need to do anything here.
        return true;
    }

    void mergeParameterLists(Document xmlDOM, Document pParameterLists, boolean pReplaceDuplicateWithChild) {
        NodeList nodes = xmlDOM.getElementsByTagName(EngineConstants.PARAMETER_LIST);

        for (int i = 0; i < nodes.getLength(); i++) {
            String tmpName = XMLHelper.getAttributeAsString(nodes.item(i).getAttributes(), ETLStep.NAME_ATTRIB, null);

            if (tmpName == null) {
                continue;
            }

            Node nf = XMLHelper.findElementByName(pParameterLists, EngineConstants.PARAMETER_LIST, ETLStep.NAME_ATTRIB,
                    tmpName);

            if (nf != null) {
                if (pReplaceDuplicateWithChild) {
                    pParameterLists.getFirstChild().replaceChild(pParameterLists.importNode(nodes.item(i), true), nf);
                }
            }
            else {
                pParameterLists.getFirstChild().appendChild(pParameterLists.importNode(nodes.item(i), true));
            }
        }
    }

    protected boolean replaceParameters(Node xmlNode, ArrayList pParameterListNames) {

        // do not recurse parameter lists
        if (xmlNode.getNodeName().equals(EngineConstants.PARAMETER_LIST))
            return true;

        if (com.kni.util.Arrays.searchArray(ETLStep.TAGS_NOT_SUPPORTING_PARAMETERS, xmlNode.getNodeName()) >= 0)
            return true;

        NodeList nl = xmlNode.getChildNodes();

        String strParameterListName = XMLHelper.getAttributeAsString(xmlNode.getAttributes(),
                EngineConstants.PARAMETER_LIST, null);

        DocumentBuilder builder;

        try {
            builder = dmfFactory.newDocumentBuilder();

            if (strParameterListName != null && pParameterListNames.contains(strParameterListName) == false) {
                pParameterListNames.add(strParameterListName);
            }

            // cycle through all nodes to get nodes needing inheritance
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);

                if (replaceParameters(n, pParameterListNames) == false) {
                    return false;
                }
            }
            String xmlString;
            if (xmlNode.getNodeType() == Node.TEXT_NODE)
                xmlString = xmlNode.getNodeValue();
            else
                xmlString = XMLHelper.outputXML(xmlNode);

            ArrayList requiredParameters = getRequiredParametersFromValidNode(xmlNode, new ArrayList());

            if (requiredParameters == null || requiredParameters.size() == 0) {
                return true;
            }

            for (int x = 0; x < requiredParameters.size(); x++) {
                String strParameter = (String) requiredParameters.get(x);
                String strParameterValue = null;

                for (int p = pParameterListNames.size() - 1; p >= 0; p--) {
                    String tmpName = (String) pParameterListNames.get(p);

                    if (tmpName != null) {
                        strParameterValue = this.ejCurrentJob.getParameterValue(tmpName, strParameter, null);
                    }

                    if (strParameterValue != null) {
                        p = -1;
                    }
                }

                if (strParameterValue == null) {
                    // No PARAMETER_LIST specified in source...
                    String pLists = "";

                    for (int p = pParameterListNames.size() - 1; p >= 0; p--) {
                        String tmpName = (String) pParameterListNames.get(p);

                        if (tmpName != null) {
                            pLists = pLists + ", " + tmpName;
                        }
                    }

                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Parameter " + strParameter
                            + " required but not found in parameter list " + pLists);

                    return false;
                }

                xmlString = EngineConstants.replaceParameter(xmlString, strParameter, strParameterValue);

                if (xmlNode.getNodeType() == Node.TEXT_NODE) {
                    xmlNode.setNodeValue(XMLHelper.decodeHex(xmlString));
                }
                else {
                    Document tmpXMLDOM;
                    tmpXMLDOM = builder.parse(new InputSource(new StringReader(xmlString)));

                    Node replacementNode = tmpXMLDOM.getFirstChild();

                    // get parent node and replace with inherited node
                    Node parentNode = xmlNode.getParentNode();

                    Document xmlDOC;

                    if (xmlNode.getNodeType() == Node.DOCUMENT_NODE) {
                        xmlDOC = (Document) xmlNode;
                    }
                    else {
                        xmlDOC = xmlNode.getOwnerDocument();
                    }

                    Node r = xmlDOC.importNode(replacementNode, true);

                    parentNode.replaceChild(r, xmlNode);
                    xmlNode = r;
                }
            }

            if (strParameterListName != null) {
                pParameterListNames.remove(strParameterListName);
            }
        } catch (Exception e) {
            ResourcePool.LogException(e, this);

            return false;
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (5/8/2002 2:52:39 PM)
     * 
     * @return boolean
     * @param jJob com.kni.etl.ETLJob
     */
    public boolean supportsJobType(ETLJob jJob) {
        // Only accept KETL jobs...
        return (jJob instanceof KETLJob);
    }

    public String toString() {
        if (this.ejCurrentJob != null) {
            return this.ejCurrentJob.getJobID();
        }

        return KETLJobExecutor.class.getName();
    }

    protected void closeSteps() {
        if (em != null) {
            em.close(this.ejCurrentJob);
        }
        // clear QA tests
        this.mqaCollections.clear();
        em = null;
    }

    private Map mqaCollections = new HashMap();

    private void registerQACollection(String name, QACollection collection) throws KETLThreadException {

        if (this.mqaCollections.put(name, collection) != null)
            throw new KETLThreadException("QA Collection " + name + " already exists, report bug", Thread
                    .currentThread());

    }

    QACollection getQACollection(String name, ETLStep step, Node xmlConfig) throws KETLThreadException {

        synchronized (this.mqaCollections) {
            QACollection res = (QACollection) this.mqaCollections.get(name);

            if (res == null) {
                res = new QACollection(step, xmlConfig);
                this.registerQACollection(name, res);
                return res;
            }

            return res;
        }

    }
}
