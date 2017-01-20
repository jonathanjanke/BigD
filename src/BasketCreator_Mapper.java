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

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class BasketCreator_Mapper extends Mapper<Object, Text, Text, Text> {
		
        private final static Text one = new Text("1");
        private Text word = new Text();
        private HashSet <String> singleItemsets;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	   	String file = value.toString();
      	   	singleItemsets = Apriori_Main.singleItemsets;
      	   	String [] baskets  = file.split("\n");
   		 	
      	   	if (singleItemsets.size()>0) {
				  for (String basket : baskets) {
					  basket = basket.split("\t")[0];
					  String [] elementsInBasket = basket.split(",");
					  ArrayList <String> reducedElements = this.reduceElementsInBasket(elementsInBasket);						  						 
					  String curr = "";
					  if (reducedElements.size()>0) {
						  String replacedElement;
						  for (int i=0; i<reducedElements.size()-1; i++) {
							  if (Apriori_Main.NUMBER_COMBINATIONS==1) replacedElement = "" + replaceElement(reducedElements.get(i));
							  else replacedElement = reducedElements.get(i);
							  curr += replacedElement + ",";
						  }
						  if (Apriori_Main.NUMBER_COMBINATIONS==1) replacedElement = "" + replaceElement(reducedElements.get(reducedElements.size()-1));
						  else replacedElement = reducedElements.get(reducedElements.size()-1);
						  curr += replacedElement;
						  word.set(curr);
						  context.write(word, one);				  
					  }
				  }
      	   	}
        }
        
        private int replaceElement(String string) {
			int i = getIndex(this.singleItemsets, string);
			return i;
		}

		private ArrayList<String> reduceElementsInBasket (String [] elementsInBasket) {
        	ArrayList<String> reducedElements = new ArrayList<String>();
        	for (String element : elementsInBasket) {
        		if (singleItemsets.contains(element)) reducedElements.add(element);
        	}
        	return reducedElements;
        }
		
		public static int getIndex(Set<? extends String> set, String value) {
			   int result = 0;
			   for (String entry:set) {
			     if (entry.equals(value)) return result;
			     result++;
			   }
			   return -1;
		}
}