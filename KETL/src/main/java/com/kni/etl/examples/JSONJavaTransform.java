package com.kni.etl.examples;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.Format;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.reader.JDBCReader;
import com.kni.etl.ketl.transformation.UserDefinedTransform;
import com.kni.etl.stringtools.FastSimpleDateFormat;

public class JSONJavaTransform extends UserDefinedTransform {

  private static final String JSON_MAP = "JSONMap";

  public JSONJavaTransform() {
    super();
  }

  final class JSONMapper {
    public String name;
    public Class<?> cls;
    public JsonPath jpath;
    public Format format;
    public boolean required = false;
    public ParsePosition position;
    public String field;

    public Object value(JSONObject jObj) throws KETLTransformException {
      Object res = null;
      try {
        res = this.jpath == null ? jObj.get(field) : jpath.read(jObj.toString());
      } catch (PathNotFoundException e) {
        res = null;
      }
      if (this.required && res == null) {
        throw new KETLTransformException("Missing required field " + name + " - " + jObj.toString());
      }
      if (res == null)
        return res;
      return cast(res, this.cls, format, position);
    }
  }

  private List<JSONMapper> maps = new ArrayList<JSONMapper>();
  private Input jsonField;

  @Override
  public void instantiate(List<Input> inputs) throws KETLTransformException {

    String jsonField = this.getAttribute("JSONFIELD");

    for (Input in : inputs) {
      if (in.name().equalsIgnoreCase(jsonField))
        this.jsonField = in;
    }

    List<JSONMapper> tmpMaps = (List<JSONMapper>) this.getSharedResource(JSON_MAP);

    // if the map has been created then no need to reconnect
    if (tmpMaps == null) {

      String mapAttr = this.getAttribute("EVENT");
      String tableName = this.getAttribute("TABLENAME");


      this.fieldMap = this.getValueMapping(mapAttr);
      Connection con = this.getConnection(this.getAttribute("PARAMETER_LIST"));

      try {
        PreparedStatement mStmt = con.prepareStatement("select * from " + tableName + " where 1=0");
        ResultSet rs = mStmt.executeQuery();
        ResultSetMetaData rm = rs.getMetaData();
        int cols = rm.getColumnCount();
        JDBCItemHelper jdbcMapper = new JDBCItemHelper();

        for (int i = 1; i <= cols; i++) {
          JSONMapper map = new JSONMapper();
          map.name = rm.getColumnName(i);
          map.cls =
              jdbcMapper.getJavaClass(rm.getColumnType(i), JDBCReader.getColumnDisplaySize(rm, i),
                  JDBCReader.getPrecision(rm, i), JDBCReader.getScale(rm, i));
          JSONObject mapConfig = this.fieldMap.get(map.name);


          if (mapConfig != null) {
            String required = (String) mapConfig.get("required");
            map.required = required != null && Boolean.parseBoolean(required);
            if (mapConfig.containsKey("jpath"))
              map.jpath = JsonPath.compile((String) mapConfig.get("jpath"));
            else if (mapConfig.containsKey("field"))
              map.field = (String) mapConfig.get("field");
            else
              map.field = map.name;

            if (mapConfig.containsKey("format")) {
              map.format = new FastSimpleDateFormat((String) mapConfig.get("format"));
              map.position = new ParsePosition(0);
            }
          }
          this.maps.add(map);
        }

        // Close open resources
        if (mStmt != null) {
          mStmt.close();
        }


      } catch (Exception e) {
        throw new KETLTransformException(e);
      } finally {
        if (con == null)
          return;
        ResourcePool.releaseConnection(con);
      }

      this.setSharedResource(JSON_MAP, this.maps);
    } else {
      this.maps.addAll(tmpMaps);
    }

    for (JSONMapper map : maps) {
      this.addOut(map.name, map.cls);
    }

  }

  private Map<String, JSONObject> fieldMap;

  @Override
  public void transform() throws InterruptedException, KETLTransformException, KETLQAException {
    for (Object[] r : this) {
      JSONObject jObj = (JSONObject) JSONValue.parse((String) r[this.jsonField.index()]);
      Object[] res = new Object[maps.size()];
      int i = 0;
      for (JSONMapper map : maps) {
        res[i++] = map.value(jObj);
      }
      emit(res);
    }
  }

}
