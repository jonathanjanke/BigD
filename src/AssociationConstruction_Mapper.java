import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsAboutPage;

public class AssociationConstruction_Mapper extends Mapper<Object, Text, Text, Text> {
		
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String file = value.toString();
        	String [] itemsets = file.split("\n");
    
        	String [] items;
        	int support;
        	
        	for (int i = 0; i<itemsets.length; i++) {
        		support = Integer.parseInt(itemsets[i].split("\t")[1]);
        		itemsets[i] = itemsets[i].split("\t")[0];
        		items = itemsets[i].split(",");
        		
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
        	
        	
        }

		private String arrayToString(String[] combination) {
			String combinationString = "";
    		for (int i =0; i<combination.length-1; i++) {
    			combinationString += combination[i] + ",";
    		}
    		if (combination.length!=0) combinationString += combination[combination.length-1];
    		
    		return combinationString;
		}

		private static String [][] combine (String [] elements, int combinationSize) {
    		ArrayList<String> elList = new ArrayList<String>();
    		for (String el : elements) {
    			elList.add(el);
    		}
//    		int combinationNumber = 1;
//    		for (int i=0; i<combinationSize; i++) {
//    			combinationNumber *= elements.length - i;
//    		}
    		ArrayList<String[]> temp= combine(elList, combinationSize);
    		String [][] combinations = new String [temp.size()][];
    		for (int i=0; i<temp.size(); i++) {
    			combinations [i] = temp.get(i);
    		}
    		
    		if (combinationSize > 0) return combinations;
    		else return null;
    	}
    	
    	private static ArrayList<String[]> combine (ArrayList<String> elements, int combinationSize) {
    		ArrayList<String []> combinations = new ArrayList<String []>();
    		//recursion base
    		if ((elements.size() == 1)||combinationSize == 1||elements.size()<combinationSize) {
    			for (String el : elements) {
    				String [] s = {el};
    				combinations.add(s);
    			}
    			return combinations;
    		}
    		//recursion loop
    		else {
    			for (int i=0; i<=elements.size()-combinationSize; i++) {
    				//get ith element from elements and set it to array
    				String [] s = {elements.get(i)};
    				
    				// call function on remaining elements;
    				ArrayList<String> tempElements = new ArrayList <String>();
    				for (int k = 0; k<elements.size(); k++) {
    					tempElements.add(elements.get(k));
    				}
    				for (int j=0; j<=i; j++) {
    					tempElements.remove(0);
    				}
    				
    				ArrayList<String []> temp = combine (tempElements, combinationSize-1);
    				for (String [] t : temp) combinations.add(concat(s, t));
    			}
    			return combinations;
    		}
    	}
    	
    	//from the internet: http://stackoverflow.com/questions/80476/how-can-i-concatenate-two-arrays-in-java
    	public static String[] concat(String[] a, String[] b) {
    	   int aLen = a.length;
    	   int bLen = b.length;
    	   String[] c= new String[aLen+bLen];
    	   System.arraycopy(a, 0, c, 0, aLen);
    	   System.arraycopy(b, 0, c, aLen, bLen);
    	   return c;
    	}
}