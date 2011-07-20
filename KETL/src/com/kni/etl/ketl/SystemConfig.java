package com.kni.etl.ketl;

import com.kni.etl.ketl.SystemConfigCache.Parameter;
import com.kni.etl.ketl.SystemConfigCache.ParameterType;
import com.kni.etl.ketl.exceptions.KETLThreadException;

public interface SystemConfig {

	public abstract String[] getRequiredTags(Class callingClass, String pGroup);

	public abstract String getStepTemplate(Class callingClass, String pGroup, String pName, boolean pDefaultAllowed) throws KETLThreadException;

	public abstract void refreshStepTemplates(Class className) throws KETLThreadException;

	public abstract Parameter getParameterOfType(Class parentClass, String pGroup, String pName, ParameterType attribute, boolean pDefaultAllowed) throws KETLThreadException;

}