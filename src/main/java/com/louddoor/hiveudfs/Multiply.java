package com.louddoor.hiveudfs;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

@Description(name="multiply",value="_FUNC_(val) - Multiples the values of a list")
public class Multiply extends GenericUDF {
	private Converter[] converters;
	private ObjectInspector[] argumentOIs;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

		if (arguments.length != 1) {
			throw new UDFArgumentLengthException(
					"The function MULTIPLY(array(obj1, obj2,...)) needs one argument.");
		}

		switch(arguments[0].getCategory()) {
		case LIST:
			if(((ListObjectInspector)(arguments[0])).getListElementObjectInspector()
					.getCategory().equals(Category.PRIMITIVE)) {
				break;
			}
		default:
			throw new UDFArgumentTypeException(0, "Argument 1"
					+ " of function MULTIPLY must be " + Constants.LIST_TYPE_NAME
					+ "<" + Category.PRIMITIVE + ">, but " + arguments[0].getTypeName()
					+ " was found.");
		}

		ObjectInspector elementObjectInspector =
			((ListObjectInspector)(arguments[0])).getListElementObjectInspector();
		argumentOIs = arguments;
		converters = new Converter[arguments.length];
		ObjectInspector returnOI = returnOIResolver.get();
		if (returnOI == null) {
			returnOI = elementObjectInspector;
		}
		converters[0] = ObjectInspectorConverters.getConverter(elementObjectInspector, returnOI);

		return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Double evaluate(DeferredObject[] args) {
		Double d = 1d;

		if (args.length == 0)
			return null;

		Object array;
		
		try {
			array = args[0].get();
			ListObjectInspector arrayOI = (ListObjectInspector) argumentOIs[0];
			List<Object> retArray = (List<Object>) arrayOI.getList(array);
			
			for (int i = 0; i < retArray.size(); i++) {
				converters[0].convert(retArray.get(i)); 
			}
		} catch (HiveException e) {
			e.printStackTrace();
		}

		return d;
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 1);
		return "multiply(" + children[0] + ")";
	}
}