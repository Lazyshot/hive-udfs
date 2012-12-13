package com.louddoor.hiveudfs;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="array_intersect", value="_FUNC_(arg,arg) - Returns map of all the key/value pairs")
public class ArrayIntersect extends UDF {
	
	@SuppressWarnings("rawtypes")
	public List evaluate(final List first, final List second)
	{
		List result = new ArrayList();
		
		result = ListUtils.intersection(first, second);
		
		return result;
	}
	
}