package com.louddoor.hiveudfs;

import java.util.List;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.pdk.HivePdkUnitTests;
import org.apache.hive.pdk.HivePdkUnitTest;

@Description(name="array_intersect", value="_FUNC_(int,int) - Returns map of all the key/value pairs")
@HivePdkUnitTests(setup = "create table dual_data (i int);"
		+ "insert overwrite table dual_data select 'Facebook' from onerow limit 1;",
		cleanup = "drop table if exists dual_data;",
		cases = {
				@HivePdkUnitTest(
						query = "SELECT ld_array_intersect(array('1','2','3','4'), array('2','4')) FROM onerow;",
						result = "[\"2\",\"4\"]"),
				@HivePdkUnitTest(
						query = "SELECT ld_array_intersect(array('1','2','3'), array('4','5')) FROM onerow;",
						result = "[]")
			}
		)
public class ArrayIntersect extends UDF {
	@SuppressWarnings("unchecked")
	public List<String> evaluate(final List<String> first, final List<String> second)
	{		
		return ListUtils.intersection(first, second);
	}
	
}