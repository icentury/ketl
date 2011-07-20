package com.kni.etl.ketl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.SystemConfigCache.Parameter;
import com.kni.etl.ketl.SystemConfigCache.ParameterType;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.util.XMLHelper;

public class SystemConfigDirect implements SystemConfig {

	final Map<String, Element> cache = Collections.synchronizedMap(new HashMap());

	/**
	 * Gets the step templates.
	 * 
	 * @param pClass
	 *            the class
	 * 
	 * @return the step templates
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	private final Element getStepTemplates(Class pClass) throws KETLThreadException {
		Document doc = EngineConstants.getSystemXML();

		if (doc == null) {
			// try again, stupid xml bug
			EngineConstants.clearSystemXML();
			if ((doc = EngineConstants.getSystemXML()) == null)
				throw new KETLThreadException("System.xml cannot be found or instantiated", this);
		}
		Element node = cache.get(pClass.getCanonicalName());
		if (node == null) {
			synchronized (doc) {
				node = (Element) XMLHelper.findElementByName(doc, "STEP", "CLASS", pClass.getCanonicalName());

				// bug fix, system xml gets corrupted and must be reloaded
				if (node == null) {
					EngineConstants.clearSystemXML();
					doc = EngineConstants.getSystemXML();
					cache.remove(pClass.getCanonicalName());
					node = (Element) XMLHelper.findElementByName(doc, "STEP", "CLASS", pClass.getCanonicalName());
				}

				if (node != null) {
					node = (Element) XMLHelper.getElementByName(node, "TEMPLATES", null, null);
					cache.put(pClass.getCanonicalName(), node);
				}
			}

		}

		return node;

	}

	/**
	 * Gets the step template.
	 * 
	 * @param parentClass
	 *            the parent class
	 * @param pGroup
	 *            the group
	 * @param pName
	 *            the name
	 * @param pDefaultAllowed
	 *            the default allowed
	 * 
	 * @return the step template
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public final String getStepTemplate(Class parentClass, String pGroup, String pName, boolean pDefaultAllowed) throws KETLThreadException {
		Element template = this.getStepTemplates(parentClass);

		if (template == null) {
			throw new KETLThreadException("Template missing from system file CLASS=" + this.getClass().getCanonicalName() + " GROUP=" + pGroup + " NAME=" + pName, this);
		}

		Class superCl = this.getClass().getSuperclass();

		synchronized (template) {
			// get group
			Element e = (Element) XMLHelper.getElementByName(template, "GROUP", "NAME", pGroup);

			// if group is null then try default group
			if (e == null) {

				if (pDefaultAllowed)
					e = (Element) XMLHelper.getElementByName(template, "GROUP", "NAME", "DEFAULT");

				// if still null then go to parent parent class
				if (e == null) {
					if (superCl != null && ETLWorker.class.isAssignableFrom(superCl))
						return this.getStepTemplate(superCl, pGroup, pName, pDefaultAllowed);

					throw new KETLThreadException("Template group \"" + pGroup + "\" not found", this);
				}
			}

			e = (Element) XMLHelper.getElementByName(e, "TEMPLATE", "NAME", pName);

			if (e == null) {

				// if not found in main group then go back to default
				if (pDefaultAllowed) {
					e = (Element) XMLHelper.getElementByName(template, "GROUP", "NAME", "DEFAULT");

					// go to parent
					if (e != null) {
						e = (Element) XMLHelper.getElementByName(e, "TEMPLATE", "NAME", pName);
					}
				}

				// go to parent
				if (e == null) {
					if (superCl != null && ETLWorker.class.isAssignableFrom(superCl))
						return this.getStepTemplate(superCl, pGroup, pName, pDefaultAllowed);
					throw new KETLThreadException("Template group \"" + pGroup + "\" element \"" + pName + "\" not found", this);
				}

			}

			return XMLHelper.getTextContent(e);

		}
	}

	/**
	 * Gets the required tags.
	 * 
	 * @param parentList
	 *            the parent list
	 * @param requestedClass
	 *            the requested class
	 * 
	 * @return the required tags
	 */
	public void getRequiredTags(HashSet parentList, Class requestedClass) {

		Node n = XMLHelper.findElementByName(EngineConstants.getSystemXML(), "STEP", "CLASS", requestedClass.getCanonicalName());

		if (n == null)
			return;

		NodeList nl = ((Element) n).getElementsByTagName("PARAMETERS");

		for (int i = 0; i < nl.getLength(); i++) {
			Node[] params = XMLHelper.getElementsByName(nl.item(i), "PARAMETER", "REQUIRED", "TRUE");

			if (params != null)
				for (Node element : params) {
					if (parentList.contains(params) == false)
						parentList.add(((Element) element).getAttribute("NAME"));
				}
		}

		Class cl = requestedClass.getSuperclass();
		if (cl != null && ETLWorker.class.isAssignableFrom(cl)) {
			getRequiredTags(parentList, cl);
		}

	}

	/**
	 * Gets the required tags.
	 * 
	 * @return the required tags
	 */
	public String[] getRequiredTags(Class cls, String group) {
		Exception e = null;
		for (int x = 0; x < 5; x++) {
			try {
				Node n = XMLHelper.findElementByName(EngineConstants.getSystemXML(), "STEP", "CLASS", cls.getCanonicalName());

				if (n == null) {
					EngineConstants.clearSystemXML();
					n = XMLHelper.findElementByName(EngineConstants.getSystemXML(), "STEP", "CLASS", cls.getCanonicalName());
				}

				if (n == null) {
					throw new RuntimeException("Class requires an entry in the System.xml file to be valid, please add an entry for class " + cls.getCanonicalName());
				}
				NodeList nl = ((Element) n).getElementsByTagName("PARAMETERS");

				HashSet res = new HashSet();
				for (int i = 0; i < nl.getLength(); i++) {
					Node[] params = XMLHelper.getElementsByName(nl.item(i), "PARAMETER", "REQUIRED", "TRUE");

					if (params != null)
						for (Node element : params)
							res.add(((Element) element).getAttribute("NAME"));
				}

				Class cl = cls.getSuperclass();

				if (cl != null && ETLWorker.class.isAssignableFrom(cl)) {
					getRequiredTags(res, cl);
				}

				String[] result = new String[res.size()];
				res.toArray(result);

				return result;
			} catch (Exception e1) {
				e = e1;
				ResourcePool.logException(e1);
				EngineConstants.clearSystemXML();
				ResourcePool.LogMessage(Thread.currentThread(), "Retrying system.xml in 5 seconds, attemp - " + (x + 1) + " of 5");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e2) {
					ResourcePool.logException(e2);
				}
			}
		}
		throw new RuntimeException(e);
	}

	public static SystemConfigDirect getInstance() {
		return new SystemConfigDirect();
	}

	public void refreshStepTemplates(Class className) throws KETLThreadException {
		this.getStepTemplates(className);
	}

	public Parameter getParameterOfType(Class parentClass, String pGroup, String pName, ParameterType attribute, boolean pDefaultAllowed) throws KETLThreadException {
		// TODO Auto-generated method stub
		return null;
	}
}
