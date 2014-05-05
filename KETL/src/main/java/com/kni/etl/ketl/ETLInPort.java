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
package com.kni.etl.ketl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.codec.binary.Hex;
import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLInPort.
 */
public class ETLInPort extends ETLPort {

	/**
	 * Instantiates a new ETL in port.
	 * 
	 * @param esOwningStep
	 *            the es owning step
	 * @param esSrcStep
	 *            the es src step
	 */
	public ETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
		super(esOwningStep, esSrcStep);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
	 */
	@Override
	public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
		int res = super.initialize(xmlConfig);
		if (res != 0)
			return res;

		String sort = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "SORT", null);

		if (sort == null)
			this.setSort(ETLInPort.NO_SORT);
		else if (sort.equalsIgnoreCase("ASC"))
			this.setSort(ETLInPort.ASC);
		else if (sort.equalsIgnoreCase("DESC"))
			this.setSort(ETLInPort.DESC);
		else
			throw new KETLThreadException("Invalid sort value: " + sort, this);

		return 0;
	}

	/** The object name. */
	private String mObjectName;

	/**
	 * Sets the code generation reference object.
	 * 
	 * @param obj
	 *            the new code generation reference object
	 */
	public void setCodeGenerationReferenceObject(String obj) {
		this.mObjectName = obj;
	}

	/**
	 * Gets the source port index.
	 * 
	 * @return the source port index
	 */
	final public int getSourcePortIndex() {
		return this.src.getPortIndex();
	}

	private Checksum digest;
	final private byte[] NULL = "0".getBytes();
	
	public Long getHash() {
		if (digest == null)
			digest = new CRC32();
		
		digest.reset();
		for (Object o : this.mesStep.activeRecord) {
			byte[] b= o==null?NULL:o.toString().getBytes();
			digest.update(b,0,b.length);
		}
		return digest.getValue();
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLPort#getPortName()
	 */
	@Override
	public String getPortName() throws KETLThreadException {
		if (this.mstrName != null)
			return this.mstrName;

		String channel, port;

		this.mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), ETLPort.NAME_ATTRIB, null);

		if (this.isConstant() == false) {
			if (this.src == null) {
				String txt = XMLHelper.getTextContent(this.getXMLConfig());
				txt = txt.trim();

				String[] sources = txt.split("\\.");

				if (sources == null || sources.length == 1 || sources.length > 3)
					throw new KETLThreadException("IN port definition invalid: \"" + txt + "\"", this);

				channel = sources.length == 3 ? sources[1] : "DEFAULT";
				port = sources.length == 3 ? sources[2] : sources[1];
				this.src = this.mesSrcStep.setOutUsed(channel, port);
			}

			if (this.mstrName == null) {
				(this.getXMLConfig()).setAttribute("NAME", this.src.mstrName);
				this.mstrName = this.src.mstrName;
			}
		}

		if (this.mstrName == null)
			throw new KETLThreadException("In port must have a name, check step " + this.mesStep.getName() + ", XML: "
					+ XMLHelper.outputXML(this.getXMLConfig()), this);

		return this.mstrName;
	}

	/** The Constant NO_SORT. */
	final public static int NO_SORT = 0;

	/** The Constant ASC. */
	final public static int ASC = 1;

	/** The Constant DESC. */
	final public static int DESC = 2;

	/** The src. */
	ETLOutPort src;

	/** The sort. */
	private int sort = ETLInPort.NO_SORT;

	/**
	 * Sets the source port.
	 * 
	 * @param srcPort
	 *            the new source port
	 */
	public void setSourcePort(ETLOutPort srcPort) {
		this.src = srcPort;
	}

	/**
	 * Generate reference.
	 * 
	 * @return the string
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public String generateReference() throws KETLThreadException {
		if (this.isConstant())
			return "const_" + this.mstrName;

		return "((" + this.getPortClass().getCanonicalName() + ")" + this.mObjectName + "["
				+ this.mesStep.getUsedPortIndex(this) + "])";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.ETLPort#getCodeGenerationReferenceObject()
	 */
	@Override
	public String getCodeGenerationReferenceObject() {
		return this.mObjectName;
	}

	/**
	 * Sets the sort.
	 * 
	 * @param sort
	 *            the new sort
	 */
	public void setSort(int sort) {
		this.sort = sort;
	}

	/**
	 * Gets the sort.
	 * 
	 * @return the sort
	 */
	public int getSort() {
		return this.sort;
	}

}
