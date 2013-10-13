package com.louddoor.hiveudfs;

import java.util.Map;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

@Description(name="filter_map", value="_FUNC_(map, list) - Returns a map with only the keys specified. v0.0.2")
public class FilterMap extends GenericUDF {

	private MapObjectInspector mapOI;
	private ListObjectInspector listOI;

	@Override
	public String getDisplayString(String[] children) {
		return "filter_map()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentException("filter_map only takes 2 argument: map and list");
		}

		ObjectInspector map = arguments[0];
		ObjectInspector list = arguments[1];

		if (!(map instanceof MapObjectInspector)) {
			throw new UDFArgumentException("argument 1 must be a map: " + map.getClass().toString());
		}

		if (!(list instanceof ListObjectInspector)) {
			throw new UDFArgumentException("argument 2 must be a list: " + list.getClass().toString());
		}

		this.mapOI = (MapObjectInspector) map;
		this.listOI = (ListObjectInspector) list;

		ObjectInspector keyOI = this.mapOI.getMapKeyObjectInspector();
		ObjectInspector valOI = this.mapOI.getMapValueObjectInspector();

		return ObjectInspectorFactory.getStandardMapObjectInspector(
				keyOI,
				valOI
			);
	}

	@Override
	public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException
	{
		Map<Object, Object> map = (Map<Object, Object>) this.mapOI.getMap(arguments[0].get());
		List<Object> list = (List<Object>) this.listOI.getList(arguments[1].get());

		if (map == null) {
			return null;
		}

		if (list == null) {
			return null;
		}

		for (Object key : map.keySet()) {
			if (!list.contains(key)) {
				map.remove(key);
			}
		}

		return map;
	}

}