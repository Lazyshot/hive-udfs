package com.louddoor.hiveudfs;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class Product extends UDAF {
	
	public static class ProdState {
		private long mCount;
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
			state.mCount = 0;
		}
		
		public boolean iterate(Double o) {
			if (o != null) {
				state.mProd *= o;
				state.mCount++;
			}
			return true;
		}
		
		public ProdState terminatePartial() {
			return state.mCount == 0 ? null : state;
		}
		
		public boolean merge(ProdState o) {
			if (o != null) {
				state.mCount += o.mCount;
				state.mProd *= o.mProd;
			}
			return true;
		}
		
		public Double terminate() {
			return state.mCount == 0 ? null : Double.valueOf(state.mProd);			
		}
	}
	
	private Product() {}
	
}