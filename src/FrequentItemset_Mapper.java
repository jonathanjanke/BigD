import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class FrequentItemset_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable result = new IntWritable(1);
        private Text word = new Text();
        private static int numberCombinations;
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String file = value.toString();
      	   	numberCombinations = Apriori_Main.NUMBER_COMBINATIONS;
      	   	
      	   	String [] baskets  = file.split("\n");
      	   	if (numberCombinations == 1) Apriori_Main.DYNAMIC_NUMBER_LINES += baskets.length;
		    for (String basket : baskets) {
			  basket = basket.split("\t")[0];
			  String [] elementStringsInBasket = basket.split(",");
			  int [] elementsInBasket = new int [elementStringsInBasket.length];
			  if (numberCombinations ==1) {
				  for (int i = 0; i<elementStringsInBasket.length; i++) {
					  if (!Apriori_Main.inverseItemMap.containsKey(elementStringsInBasket[i])) {
						  int keyInt = Apriori_Main.itemMap.size()+1;
						  Apriori_Main.itemMap.put(keyInt, elementStringsInBasket[i]);
						  Apriori_Main.inverseItemMap.put(elementStringsInBasket[i], keyInt);
						  elementsInBasket[i] = keyInt;
					  } else {
						  elementsInBasket[i] = Apriori_Main.inverseItemMap.get(elementStringsInBasket[i]);
					  }
				  }
//				  if (numberCombinations==1) {
//					  HashMap<Integer, String> basketCombinations = hashValues(elementsInBasket, 2);
//				  }
			   } else {
				  for (int i = 0; i<elementStringsInBasket.length; i++) {
					  elementsInBasket[i] = Integer.parseInt(elementStringsInBasket[i]);
				  }
			   }
			   Arrays.sort(elementsInBasket);
			   //ArrayList <String> reducedElements = this.reduceElementsInBasket(elementsInBasket);
			   int  [][] basketCombinations = combineToArray (elementsInBasket, numberCombinations);
			  
			   for (int [] element : basketCombinations) {
				  String curr = "";
				  for (int i=0; i<element.length; i++) {
					  curr += element [i];
					  if (i != element.length-1) curr += ",";
				  }
				  word.set(curr);
			      context.write(word, result);			      
			   }
			}
        }
        
        private HashMap<String, Integer> hashValues(ArrayList<String> elements, int combinationSize) {
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
    	
    	private static int [][] combineToArray (int [] elements, int combinationSize) {

    		ArrayList<int[]> temp = combine(elements, combinationSize);
    		int [][] combinations = new int [temp.size()][];
    		for (int i=0; i<temp.size(); i++) {
    			combinations [i] = temp.get(i);
    		}
    		
    		if (combinationSize > 0) return combinations;
    		else return null;
    	}
    	
    	private static ArrayList<int[]> combine (int [] elements, int combinationSize) {
    		ArrayList<int []> combinations = new ArrayList<int []>();
    		//recursion base
    		if ((elements.length == 1)||combinationSize == 1||elements.length<combinationSize) {
    			for (int el : elements) {
    				int [] s = {el};
    				combinations.add(s);
    			}
    		}
    		//recursion step
    		else {
    			for (int i=0; i<=elements.length-combinationSize; i++) {
    				//get ith element from elements and set it to array
    				int [] s = {elements[i]};
    				
//    				if (numberCombinations == 2) {
//    					if (Apriori_Main.hashMap.containsKey(elements.get(i).split(",")[0])) {
//    						if (Apriori_Main.hashMap.get(elements.get(i)) < Apriori_Main.SUPPORT_THRESHOLD) {
//    							break;
//    						}
//    					}
//    				}
    				
    				// call function on remaining elements;
    				int [] tempElements = new int [elements.length-i-1];
    				for (int k = 0; k<tempElements.length; k++) {
    					tempElements[k] = elements[k+i+1];
    				}
    				
    				ArrayList<int []> temp = combine (tempElements, combinationSize-1);
    				for (int [] t : temp) combinations.add(concat(s, t));
    			}
    		}
    		return combinations;
    	}
    	

    	
    	//from the internet: http://stackoverflow.com/questions/80476/how-can-i-concatenate-two-arrays-in-java
    	
    	public static int[] concat(int[] a, int[] b) {
     	   int aLen = a.length;
     	   int bLen = b.length;
     	   int[] c= new int[aLen+bLen];
     	   System.arraycopy(a, 0, c, 0, aLen);
     	   System.arraycopy(b, 0, c, aLen, bLen);
     	   return c;
     	}
    	
    	
}