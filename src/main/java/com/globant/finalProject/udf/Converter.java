package com.globant.finalProject.udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Converter extends EvalFunc<Double> {
	
	private String magnitude = "B";
	
	public Converter(){
		
	}
	
	public Converter(String convertTo) {
		magnitude = convertTo.toUpperCase();
	}

	@Override
	public Double exec(Tuple input) throws IOException {
		
		if(input == null || input.size() == 0)
			return null;
		
		try{
			Double value = (Double)input.get(0);			
			switch (magnitude) {
			case "B":				
				break;
			case "KB":
				value = value/1024;				
				break;
			case "MB":
				value = value/1048576;	
			case "GB":
				value = value/1073741824;
			default:
				break;
			}
			
			return value;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row ", e);
		}
		
		
	}

}
