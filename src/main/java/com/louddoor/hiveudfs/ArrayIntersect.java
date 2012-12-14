package com.louddoor.hiveudfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
						result = ""),
				@HivePdkUnitTest(
						query = "SELECT ld_array_intersect(array('1','2','3'), array('4','5')) FROM onerow;",
						result = ""),
				@HivePdkUnitTest(
						query = "SELECT ld_array_intersect(array(1,2,3,4), array(2,4)) FROM onerow;",
						result = ""),
				@HivePdkUnitTest(
						query = "SELECT ld_array_intersect(array(1,2,3), array(4,5)) FROM onerow;",
						result = "")
			}
		)
public class ArrayIntersect extends UDF {
	private List<String> result = new ArrayList<String>();
	
	public String[] evaluate(final String[] first, final String[] second)
	{
		Set<String> canAdd = new HashSet<String>(Arrays.asList(first));
		result.clear();
		
		for (String b : second) {
			if (canAdd.contains(b)) {
				canAdd.remove(b);
				result.add(b);
			}
		}
		
		return result.toArray(new String[0]);
	}
	
	public Integer[] evaluate(final Integer[] first, final Integer[] second)
	{
		Set<Integer> canAdd = new HashSet<Integer>(Arrays.asList(first));
		result.clear();
		
		for (String b : second) {
			if (canAdd.contains(b)) {
				canAdd.remove(b);
				result.add(b);
			}
		}
		
		return result.toArray(new String[0]);
	}
	
}