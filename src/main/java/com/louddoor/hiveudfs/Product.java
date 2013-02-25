package com.louddoor.hiveudfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@Description(name="product", value="_FUNC_(str) - Returns product of all grouped values")
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