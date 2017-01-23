import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentItemset_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private int support;
        private HashMap <String, Integer> itemMap;
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	if (Apriori_Main.CALCULATE) {
    			Apriori_Main.SUPPORT_THRESHOLD = (int)Math.ceil(Apriori_Main.DYNAMIC_NUMBER_LINES * Apriori_Main.RELATIVE_SUPPORT_THRESSHOLD);
    			Apriori_Main.CALCULATE = false;
    		}
        	if (Apriori_Main.CREATE_HASHSET) {
        		Apriori_Main.frequentItems = new ArrayList<ArrayList<Integer>>();
        		Apriori_Main.CREATE_HASHSET = false;
        	}
        	support = Apriori_Main.SUPPORT_THRESHOLD;
//        	if (Apriori_Main.NUMBER_COMBINATIONS == 1) {
//        		int keyInt = Integer.parseInt(key.toString());
//        		if (keyInt<0) {
//        			for (IntWritable val : values) {
//        				sum += val.get();
//        			}
//        			if (sum>= support) {
//        				Apriori_Main.hashedItems.add(keyInt*(-1));
//        			}
//        			return;
//        		}
//        	}
        		
            for (IntWritable val : values) {
            	sum += val.get();
            }
        	
        	if (sum >= support) {
            	result.set(sum);
            	String [] tempItems = key.toString().split(",");
            	itemMap = Apriori_Main.inverseItemMap;
            	String reducedItem = "";
//            	if (Apriori_Main.NUMBER_COMBINATIONS == 1) {
//	            	for (int i= 0; i<tempItems.length; i++) {
//	            		int reduce = itemMap.get(tempItems[i]);
//	           			reducedItem += reduce;
//	            		Apriori_Main.singleItemsets.add(reduce);
//	         			if (i != tempItems.length-1) {
//            				reducedItem+= ",";
//            			}
//	            	}
//	            	key.set(reducedItem);
//            	}
//            	else
            	ArrayList<Integer> items = new ArrayList<Integer>();
            		for (int i =0; i<tempItems.length; i++) {
            			int item = Integer.parseInt(tempItems[i]);
            			Apriori_Main.singleItemsets.add(item);
            			items.add(item);
            		}
            		int h = items.size();
            		Apriori_Main.frequentItems.add(items);
            	        	
            	context.write(key, result);
            }
        }
        
}