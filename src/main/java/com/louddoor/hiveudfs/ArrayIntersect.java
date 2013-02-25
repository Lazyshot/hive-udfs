package com.louddoor.hiveudfs;

import java.util.List;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="array_intersect", value="_FUNC_(int,int) - Returns map of all the key/value pairs")
public class ArrayIntersect extends UDF {
	@SuppressWarnings("unchecked")
	public List<String> evaluate(final List<String> first, final List<String> second)
	{		
		return ListUtils.intersection(first, second);
	}
	
}