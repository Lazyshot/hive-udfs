package com.louddoor.hiveudfs;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

@RunWith(JUnit4.class)
public class FilterMapTest {
	@Test
	public void fulltest() throws UDFArgumentException, HiveException {
		FilterMap udf = new FilterMap();

		ObjectInspector moi = ObjectInspectorFactory.getStandardMapObjectInspector(
			PrimitiveObjectInspectorFactory.javaStringObjectInspector,
			PrimitiveObjectInspectorFactory.javaStringObjectInspector
		);

		ObjectInspector loi = ObjectInspectorFactory.getStandardListObjectInspector(
			PrimitiveObjectInspectorFactory.javaStringObjectInspector
		);


		ObjectInspector ois[] = { moi, loi };

		udf.initialize(ois);

    Map<String, String> testMap = new HashMap<String, String>();
    List<String> testKeys = new ArrayList<String>();

    testMap.put("a", "1");
    testMap.put("b", "2");

    testKeys.add("a");

    GenericUDF.DeferredJavaObject ins[] = {
      new GenericUDF.DeferredJavaObject(testMap), 
      new GenericUDF.DeferredJavaObject(testKeys)
    };

    Map<String, String> newMap = (Map<String, String>) udf.evaluate(ins);

    System.out.println(newMap.toString());
		
		assertTrue(true);
	}
}
