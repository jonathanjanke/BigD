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

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Apriori_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String file = value.toString();
      	   	int numberCombinations = Apriori_Main.currentNumberCombinations;
      	   	String [] baskets  = file.split("\n");
   		 	HashSet<String> hs = new HashSet<String> ();
   		 	
   		 	 /*
		     for (String basket : baskets) {
			 	String [] elementsInBasket = basket.split(",");
			  
				for (String element : elementsInBasket) {
					hs.add(element);
				}
			  }
			  String [][] combinationsInBasket = this.combine(hs.toArray(new String[0]), numberCombinations);
			  */
   		 	
			  for (String basket : baskets) {
				  
				  String [] elementsInBasket = basket.split(",");
				  if (elementsInBasket.length >=numberCombinations) {
					  Arrays.sort(elementsInBasket);
					  String [][] basketCombinations = combine (elementsInBasket, numberCombinations);
					  
					  for (String [] element : basketCombinations) {
						  String curr = "";
						  for (int i=0; i<element.length-1; i++) {
							  curr += element [i] + ", ";
						  }
						  curr += element [element.length-1];
						  word.set(curr);
					      context.write(word, one);			      
					  }
				  }
			  }                   
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