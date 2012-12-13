package com.louddoor.hiveudfs;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.pdk.HivePdkUnitTests;
import org.apache.hive.pdk.HivePdkUnitTest;

@Description(name="array_intersect", value="_FUNC_(arg,arg) - Returns map of all the key/value pairs")
@HivePdkUnitTests(setup = "create table dual_data (i int);"
		+ "insert overwrite table dual_data select 'Facebook' from onerow limit 1;",
		cleanup = "drop table if exists dual_data;",
		cases = {
				@HivePdkUnitTest(
					query = "SELECT array_intersect(array(1,2,3,4), array(2,4)) FROM onerow;",
					result = ""),
				@HivePdkUnitTest(
					query = "SELECT array_intersect(array(1,2,3), array(4,5)) FROM onerow;",
					result = "")
			}
		)
public class ArrayIntersect extends UDF {
	
	@SuppressWarnings("rawtypes")
	public List evaluate(final List first, final List second)
	{
		List result = new ArrayList();
		
		result = ListUtils.intersection(first, second);
		
		return result;
	}
	
}