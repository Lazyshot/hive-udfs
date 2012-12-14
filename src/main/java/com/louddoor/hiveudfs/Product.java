package com.louddoor.hiveudfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

@Description(name="product", value="_FUNC_(str) - Returns product of all grouped values")
@HivePdkUnitTests(setup = "create table dual_data (i int);"
		+ "insert overwrite table dual_data select 5 from onerow limit 1;",
		cleanup = "drop table if exists dual_data;",
		cases = {
				@HivePdkUnitTest(
					query = "SELECT ld_product(i) FROM dual_data;",
					result = "5")
			}
		)
public class Product extends UDAF {
	
	public static class ProdState {
		private boolean empty;
		private double mProd;
	}
	
	public static class ProductEvaluator implements UDAFEvaluator {
		
		ProdState state;

		public ProductEvaluator() {
			super();
			state = new ProdState();
			init();
		}
		
		public void init() {
			state.mProd = 1;
			state.empty = true;
		}
		
		public boolean iterate(Double o) {
			if (o != null) {
				state.mProd *= o;
				state.empty = false;
			}
			return true;
		}
		
		public ProdState terminatePartial() {
			return state.empty == true ? null : state;
		}
		
		public boolean merge(ProdState o) {
			if (o != null) {
				state.empty = false;
				state.mProd *= o.mProd;
			}
			return true;
		}
		
		public Double terminate() {
			return state.empty == true ? null : Double.valueOf(state.mProd);			
		}
	}
	
	private Product() {}
	
}