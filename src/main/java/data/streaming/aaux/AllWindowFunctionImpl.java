package data.streaming.aaux;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import data.streaming.utils.Utils;

public class AllWindowFunctionImpl implements AllWindowFunction<String, String, TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void apply(TimeWindow window, Iterable<String> values, Collector<String> out) throws Exception {
		
		for(String i: values) {
			if(Utils.esValido(i)) {
				out.collect(i);
			}
			
		}
		
		
	}



}
