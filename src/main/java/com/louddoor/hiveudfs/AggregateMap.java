package com.louddoor.hiveudfs;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@Description(name="agg_map", value="_FUNC_(arg,arg) - Returns map of all the key/value pairs")
public class AggregateMap extends UDAF {
	
	public static class MapState {
		private boolean empty;
		private HashMap<Object, Object> mapRes;
	}
	
	public static class ProductEvaluator implements UDAFEvaluator {
		
		MapState state;

		public ProductEvaluator() {
			super();
			state = new MapState();
			init();
		}
		
		public void init() {
			state.mapRes = new HashMap<Object, Object>();
			state.empty = true;
		}
		
		public boolean iterate(Object key, Object val) {
			if (key != null && val != null) {
				state.mapRes.put(key, val);
				state.empty = false;
			}
			return true;
		}
		
		public MapState terminatePartial() {
			return state.empty == true ? null : state;
		}
		
		public boolean merge(MapState s) {
			if (s != null) {
				state.empty = false;
				state.mapRes.putAll(s.mapRes);
			}
			return true;
		}
		
		public HashMap<Object, Object> terminate() {
			return state.empty == true ? null : state.mapRes;
		}
	}
	
	private AggregateMap() {}
	
}