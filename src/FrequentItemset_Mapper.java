import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsAboutPage;
import org.junit.experimental.theories.Theories;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class FrequentItemset_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable result = new IntWritable(1);
        private Text word = new Text();
        private static int numberCombinations;
        String [][] basketCombinations;
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String file = value.toString();
      	   	file = file.replace("\t", "");
      	   	numberCombinations = Apriori_Main.NUMBER_COMBINATIONS;
      	   	
      	   	String [] baskets  = file.split("\n");
   		 	
				  for (String basket : baskets) {
					  String [] elementsInBasket = basket.split(",");
					  Arrays.sort(elementsInBasket);
					  ArrayList <String> reducedElements = this.reduceElementsInBasket(elementsInBasket);
					  if (elementsInBasket.length >=numberCombinations) {
						  basketCombinations = combineToArray (reducedElements, numberCombinations);
						  
						  for (String [] element : basketCombinations) {
							  String curr = "";
							  for (int i=0; i<element.length-1; i++) {
								  curr += element [i] + ",";
							  }
							  curr += element [element.length-1];
							  word.set(curr);
							  result.set(1);
						      context.write(word, result);			      
						  }
						  
//						  if (numberCombinations==1) {
//							  HashMap <String, Integer> hashMap = hashMapToArray (reducedElements, numberCombinations+1);
//							  Set<String> keys = hashMap.keySet();
//							  for (String currentKey: keys) {
//								  word.set(currentKey);
//								  result.set(hashMap.get(currentKey) + "");
//								  context.write(word, result);
//							  }
//						  }
					  }
				  }
        }
        
        private HashMap<String, Integer> hashMapToArray(ArrayList<String> elements, int combinationSize) {
        	HashMap<String, Integer> temp = new HashMap <String, Integer>();
        	int size = elements.size() - 1;

        	for (int i=0; i<elements.size(); i++) {
				//get ith element from elements and set it to array
				String key = elements.get(i);
				
//				// call function on remaining elements;
//				ArrayList<String> tempElements = new ArrayList <String>();
//				for (int k = 0; k<elements.size(); k++) {
//					tempElements.add(elements.get(k));
//				}
//				for (int j=0; j<=i; j++) {
//					tempElements.remove(0);
//				}
//				
//				int hashValue = hash (tempElements, combinationSize-1);
				temp.put(key, size);
			}
    		
    		if (combinationSize > 0) return temp;
    		else return null;
		}

		private int hash(ArrayList<String> elements, int combinationSize) {
			System.out.print(elements.size());
			return elements.size();
		}

		//TODO: get rid of this method
        private ArrayList<String> reduceElementsInBasket (String [] elementsInBasket) {
        	ArrayList<String> reducedElements = new ArrayList<String>();
        	for (String element : elementsInBasket) {
//        		if (itemset.size()>0) {
//        			if (itemset.contains(element)) reducedElements.add(element);
//        			// if (itemset.containsKey(element)) 
//        		} else if (numberCombinations==1) {
        			reducedElements.add(element);
//        		}	
        	}
        	return reducedElements;
        }
    	
    	private static String [][] combineToArray (ArrayList<String> elements, int combinationSize) {

    		ArrayList<String[]> temp = combine(elements, combinationSize);
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
    		}
    		//recursion step
    		else {
    			for (int i=0; i<=elements.size()-combinationSize; i++) {
    				//get ith element from elements and set it to array
    				String [] s = {elements.get(i)};
    				
//    				if (numberCombinations == 2) {
//    					if (Apriori_Main.hashMap.containsKey(elements.get(i).split(",")[0])) {
//    						if (Apriori_Main.hashMap.get(elements.get(i)) < Apriori_Main.SUPPORT_THRESHOLD) {
//    							break;
//    						}
//    					}
//    				}
    				
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
    		}
    		return combinations;
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