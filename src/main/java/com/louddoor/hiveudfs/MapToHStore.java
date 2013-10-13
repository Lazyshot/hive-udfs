package com.louddoor.hiveudfs;

import java.util.Map;
import java.lang.StringBuilder;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

@Description(name="map_to_hstore", value="_FUNC_(map) - Returns hstore style string from map")
public class MapToHStore extends GenericUDF {

	private MapObjectInspector mapOI;

	@Override
	public String getDisplayString(String[] children) {
		return "map_to_hstore()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 1) {
			throw new UDFArgumentException("map_to_hstore only takes 1 argument: Map<String, String>");
		}

		ObjectInspector map = arguments[0];

		if (!(map instanceof MapObjectInspector)) {
			throw new UDFArgumentException("argument must be a map of <string, string>");
		}

		this.mapOI = (MapObjectInspector) map;

		if (!(this.mapOI.getMapKeyObjectInspector() instanceof StringObjectInspector) ||
			!(this.mapOI.getMapValueObjectInspector() instanceof StringObjectInspector))
			throw new UDFArgumentException("argument must have both key and value of types string");

		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	@Override
	public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException
	{
		Map<Object, Object> map = (Map<Object, Object>) this.mapOI.getMap(arguments[0].get());
		StringBuilder sb = new StringBuilder();

		if (map == null) {
			return null;
		}

		for (Map.Entry<Object, Object> ent : map.entrySet()) {
			String key = (String) ent.getKey().toString();
			String value = (String) ent.getValue().toString();

			sb.append("\"" + key.replace("\"", "") + "\"=>\"" + value.replace("\"", "") + "\"").append(",");
		}

		return StringUtils.stripEnd(sb.toString(), ",");
	}

}