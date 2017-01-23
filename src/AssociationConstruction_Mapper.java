import java.io.IOException;

import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class AssociationConstruction_Mapper extends Mapper<Object, Text, Text, Text> {
		
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String file = value.toString();    
        	String [] items;
        	int support;
        	
    		support = Integer.parseInt(file.split("\t")[1]);
    		file = file.split("\t")[0];
    		items = file.split(",");
    		
    		String [][] combinations = new String [items.length][items.length-1];
        	String [] missing = new String [items.length];
        	
        	for (int j=0; j<items.length; j++) {
        		missing [j] = items [j];
        		int k = 0;
        		for(int l=0; l < items.length; l++) {
            	   if(l != j) combinations[j][k++] = items[l];
            	}        		
        	}
        	
        	for (int j = 0; j<combinations.length; j++) {
        		String [] combination = combinations [j];
        		String combinationString = arrayToString (combination);
        		outputKey.set(combinationString);
        		
        		String returnValue = missing [j] + ":" + support;
        		outputValue.set(returnValue);
        		
        		context.write(outputKey, outputValue);
        	}
        	
        	String combinationString = arrayToString (items);
    		outputKey.set(combinationString);
    		
    		String returnValue = Apriori_Main.EMPTY_SYMBOL + ":" + support;
    		outputValue.set(returnValue);
    		
    		context.write(outputKey, outputValue);
    	}
        	        	
        
		private String arrayToString(String[] combination) {
			String combinationString = "";
    		for (int i =0; i<combination.length-1; i++) {
    			combinationString += combination[i] + ",";
    		}
    		if (combination.length!=0) combinationString += combination[combination.length-1];
    		
    		return combinationString;
		}
}