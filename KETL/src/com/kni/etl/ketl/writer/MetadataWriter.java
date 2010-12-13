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
package com.kni.etl.ketl.writer;

import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title: ETLWriter
 * </p>
 * <p>
 * Description: Loads metadata parameters. {orts must be names PARAMETER_LIST,
 * PARAMETER_NAME, PARAMETER_VALUE and SUB_PARAMETER_LIST(Optional).
 * </p>
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.9
 */
public class MetadataWriter extends ETLWriter implements DefaultWriterCore {

	@Override
	protected String getVersion() {
		return "$LastChangedRevision$";
	}

	/**
	 * Instantiates a new metadata writer.
	 * 
	 * @param pXMLConfig
	 *            the XML config
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public MetadataWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLStep#getRequiredTags()
	 */
	@Override
	protected String[] getRequiredTags() {
		return null;
	}

	/** The metadata. */
	private Metadata mMetadata;

	/** The document. */
	private Document mDocument;

	/** The parameter. */
	private Element mParamList, mParameter;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.writer.ETLWriter#initialize(org.w3c.dom.Node)
	 */
	@Override
	public int initialize(Node pConfig) throws KETLThreadException {
		int res;

		if ((res = super.initialize(pConfig)) != 0)
			return res;

		this.mMetadata = ResourcePool.getMetadata();

		if (this.mMetadata == null) {
			ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "The " + this.getName() + " component requires a connection to the metadata");
			return -2;
		}

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, e.getMessage());
			return -3;
		}
		this.mDocument = builder.newDocument(); // Create from whole cloth
		this.mParamList = this.mDocument.createElement(EngineConstants.PARAMETER_LIST);
		this.mParameter = this.mDocument.createElement(EngineConstants.PARAMETER);
		this.mDocument.appendChild(this.mParamList);
		this.mParamList.appendChild(this.mParameter);

		HashSet hs = new HashSet();

		java.util.Collections.addAll(hs, new Object[] { "PARAMETER_NAME", "PARAMETER_LIST", "PARAMETER_VALUE", "SUB_PARAMETER_LIST" });

		for (int i = 0; i < this.mInPorts.length; i++) {
			if (hs.contains(this.mInPorts[i].mstrName) == false) {
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Invalid input name of " + this.mInPorts[i].mstrName + " will be ignored, it has to be one of "
						+ java.util.Arrays.toString(hs.toArray()));
			}
			this.mInputNameMap.put(this.mInPorts[i].mstrName, i);
		}
		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[],
	 * java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

		String paramName = null, paramList = null, subParamList = null, paramValue = null;

		Integer pNamePos = (Integer) this.mInputNameMap.get("PARAMETER_NAME");
		Integer pListPos = (Integer) this.mInputNameMap.get("PARAMETER_LIST");
		Integer pParamValuePos = (Integer) this.mInputNameMap.get("PARAMETER_VALUE");
		Integer pSubListPort = (Integer) this.mInputNameMap.get("SUB_PARAMETER_LIST");

		if (pNamePos != null) {
			paramName = (String) (this.mInPorts[pNamePos].isConstant() ? this.mInPorts[pNamePos].getConstantValue() : pInputRecords[this.mInPorts[pNamePos].getSourcePortIndex()]);
		} else if (pListPos != null) {
			paramList = (String) (this.mInPorts[pListPos].isConstant() ? this.mInPorts[pListPos].getConstantValue() : pInputRecords[this.mInPorts[pListPos].getSourcePortIndex()]);
		} else if (pParamValuePos != null) {
			paramValue = (String) (this.mInPorts[pParamValuePos].isConstant() ? this.mInPorts[pParamValuePos].getConstantValue() : pInputRecords[this.mInPorts[pParamValuePos]
					.getSourcePortIndex()]);
		} else if (pSubListPort != null) {
			subParamList = (String) (this.mInPorts[pSubListPort].isConstant() ? this.mInPorts[pSubListPort].getConstantValue() : pInputRecords[this.mInPorts[pSubListPort]
					.getSourcePortIndex()]);
		}

		if (paramList == null || paramName == null || paramValue == null) {
			throw new KETLWriteException("IN tags named PARAMETER_LIST,VALUE and NAME required; SUB_PARAMETER_LIST optional");
		}

		this.mParameter.setAttribute(ETLStep.NAME_ATTRIB, paramName);
		this.mParameter.setTextContent(paramValue);
		this.mParamList.setAttribute(ETLStep.NAME_ATTRIB, paramList);

		if (subParamList == null) {
			this.mParameter.removeAttribute(EngineConstants.PARAMETER_LIST);
		} else {
			int id = this.mMetadata.getParameterListID(subParamList);

			if (id < 0) {
				throw new KETLWriteException("Sub paramter list \"" + subParamList + "\" does not exist");

			} else if (id == this.mMetadata.getParameterListID(paramList)) {
				throw new KETLWriteException("Loop not allowed, parameter list calls sub parameter list with same name - \"" + subParamList + "\"");

			}

			this.mParameter.setAttribute(EngineConstants.PARAMETER_LIST, subParamList);
		}

		try {
			this.mMetadata.importParameterList(this.mParamList);
		} catch (Exception e) {
			throw new KETLWriteException(e);
		}

		return 1;
	}

	/** The input name map. */
	private final HashMap mInputNameMap = new HashMap();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
	}

}
