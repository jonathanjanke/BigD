import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsAboutPage;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class BasketCreator_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private HashSet <Integer> singleItemsets;
        HashMap<String, Integer> itemMap;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String file = value.toString();
      	   	singleItemsets = Apriori_Main.singleItemsets;
      	   	itemMap = Apriori_Main.inverseItemMap;
      	   	String [] baskets  = file.split("\n");
      	   	   		 	
				  for (String basket : baskets) {
					  
					  basket = basket.split("\t")[0];
					  String [] elementsInBasket = basket.split(",");
					  if (elementsInBasket.length>Apriori_Main.NUMBER_COMBINATIONS+1) {
						  int [] elementIntegerInBasket = new int [elementsInBasket.length];
						  

						  if (Apriori_Main.NUMBER_COMBINATIONS==1) {
							  for (int i=0; i<elementsInBasket.length; i++) {
								  elementIntegerInBasket[i] = itemMap.get(elementsInBasket[i]);
								  //elementIntegerInBasket[i] = getKeyByValue(itemMap, elementsInBasket[i]);
								  //elementIntegerInBasket[i] = getIndex(itemMap, elementsInBasket[i]);
							  }
						  } else {
							  for (int i=0; i<elementsInBasket.length; i++) {
								  elementIntegerInBasket[i] = Integer.parseInt(elementsInBasket[i]);
							  }
						  }
						  
						  ArrayList <Integer> reducedElements = this.reduceElementsInBasket(elementIntegerInBasket);						  						 
						
						  String curr = "";
						  if (reducedElements.size()>0) {
							  for (int i=0; i<reducedElements.size(); i++) {
								  curr += reducedElements.get(i);
								  if (i != reducedElements.size()-1) {
									  curr += ",";
								  }
							  }
							  word.set(curr);
							  context.write(word, one);				  
						  }
		              }
				  }
      	   	}

		private ArrayList<Integer> reduceElementsInBasket (int [] elementsInBasket) {
        	ArrayList<Integer> reducedElements = new ArrayList<Integer>();
        	for (int element : elementsInBasket) {
        		if (singleItemsets.contains(element)) {
        			reducedElements.add(element);
        		}
        	}
        	return reducedElements;
        }
				
}