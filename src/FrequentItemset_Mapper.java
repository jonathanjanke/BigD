import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class FrequentItemset_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable result = new IntWritable(1);
        private Text word = new Text();
        private static int numberCombinations;
        private static int bucketNumber = Apriori_Main.HASH_BUCKET_NUMBER;
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String wholebasket = value.toString();
      	   	numberCombinations = Apriori_Main.NUMBER_COMBINATIONS;
      	   
      	   	if (numberCombinations == 1) Apriori_Main.DYNAMIC_NUMBER_LINES ++;
		    
			  String basket = wholebasket.split("\t")[0];
			  int resultNumber = 1;
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
////					  int [] basketCombinations = hashValues(elementsInBasket, 2, 50);
//					  int [] hashValues = this.hashTwinValues(elementsInBasket);
//					  for (int i=0; i<hashValues.length; i++) {
//						  word.set(-i + "");
//						  result.set(hashValues[i]);
//						  context.write(word, result);
//					  }
//				  }
			   } else {
				  resultNumber = Integer.parseInt(wholebasket.split("\t")[1]);
				  for (int i = 0; i<elementStringsInBasket.length; i++) {
					  elementsInBasket[i] = Integer.parseInt(elementStringsInBasket[i]);
				  }
			   }
			   Arrays.sort(elementsInBasket);
			   //ArrayList <String> reducedElements = this.reduceElementsInBasket(elementsInBasket);
			   //int  [][] basketCombinations = combineToArray (elementsInBasket, numberCombinations);
			  
			   int [][] basketCombinations = combineToArray(elementsInBasket, numberCombinations);
			   
			   for (int [] element : basketCombinations) {
				  String curr = "";
				  for (int i=0; i<element.length; i++) {
					  curr += element [i];
					  if (i != element.length-1) curr += ",";
				  }
				  word.set(curr);
				  result.set(resultNumber);
			      context.write(word, result);			      
			   }
			}
        
        private int [] hashTwinValues (int [] elements) {
        	int [] hashValues = new int [bucketNumber];
        	for (int i=0; i<elements.length; i++) {
        		for (int j=0; j<elements.length-1; j++) {
        			hashValues [calculateTwinHash(elements[i], elements[j])]++;
        		}
        	}
        	return hashValues;
        }
        
        private static int calculateTwinHash (int v1, int v2) {
        	return (v1+v2)%bucketNumber;
        }
       
    	
    	private static int [][] combineToArray (int [] elements, int combinationSize) {

    		ArrayList<int[]> temp = combine(elements, combinationSize);
    		if (numberCombinations > 2 && Apriori_Main.CREATE_ITEMSET) {
				int [] combination;
				int j = 0;
//				System.out.print(temp.size() + " - ");
				while (j<temp.size()) {
					combination = temp.get(j);
					ArrayList<Integer> tempo = new ArrayList<Integer>();
					for (int comb : combination) {
						tempo.add(comb);
					}

					if (!Apriori_Main.frequentSets.contains(tempo)) {
						temp.remove(j);
					} else {
						j++;
					}

				}
//				System.out.println(temp.size());
			}
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