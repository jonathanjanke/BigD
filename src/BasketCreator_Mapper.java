import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class BasketCreator_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable result = new IntWritable(1);
        private Text word = new Text();
        private HashSet <Integer> singleItemsets;
        HashMap<String, Integer> itemMap;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String wholebasket = value.toString();
      	   	singleItemsets = Apriori_Main.singleItemsets;
      	   	itemMap = Apriori_Main.inverseItemMap;
			  String basket = wholebasket.split("\t")[0];
			  String [] elementsInBasket = basket.split(",");
			  if (elementsInBasket.length>=Apriori_Main.NUMBER_COMBINATIONS+1) {
				  int [] elementIntegerInBasket = new int [elementsInBasket.length];
				  
	
				  if (Apriori_Main.NUMBER_COMBINATIONS==1) {
					  for (int i=0; i<elementsInBasket.length; i++) {
						  elementIntegerInBasket[i] = itemMap.get(elementsInBasket[i]);
						  //elementIntegerInBasket[i] = getKeyByValue(itemMap, elementsInBasket[i]);
						  //elementIntegerInBasket[i] = getIndex(itemMap, elementsInBasket[i]);
					  }
				  } else {
					  int basketCount = Integer.parseInt(wholebasket.split("\t")[1]);
					  for (int i=0; i<elementsInBasket.length; i++) {
						  elementIntegerInBasket[i] = Integer.parseInt(elementsInBasket[i]);
					  }
					  result.set(basketCount);
				  }
				  
				  int [] reducedElements = this.reduceElementsInBasket(elementIntegerInBasket);						  						 
				
				  String curr = "";
				  if (reducedElements.length>0) {
					  for (int i=0; i<reducedElements.length; i++) {
						  curr += reducedElements[i];
						  if (i != reducedElements.length-1) {
							  curr += ",";
						  }
					  }
					  word.set(curr);
					  context.write(word, result);				  
				  }
	          }
		  }

		private int [] reduceElementsInBasket (int [] elementsInBasket) {
        	ArrayList<Integer> reducedElements = new ArrayList<Integer>();
        	for (int element : elementsInBasket) {
        		if (singleItemsets.contains(element)) {
        			reducedElements.add(element);
        		}
        	}
        	int [] result = new int [reducedElements.size()];
        	for (int i=0; i<reducedElements.size(); i++) {
        		result[i] = reducedElements.get(i);
        	}
        	Arrays.sort(result);
        	return result;
        }
				
}