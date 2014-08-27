package com.kni.etl.ketl.transformation;

import java.sql.Connection;
import java.util.Map;

import net.minidev.json.JSONObject;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface UDFConfiguration extends SharedResource {

  String getAttribute(String arg0);

  Map<String, JSONObject> getValueMapping(String arg0) throws KETLTransformException;

  String getParameter(String arg0);

  Connection getConnection(String paramList) throws KETLTransformException;

  void releaseConnection(Connection con);

  String getTempDir();

}
