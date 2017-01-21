import java.io.IOException;
import java.util.ArrayList;
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
        private int s = Apriori_Main.SUPPORT_THRESHOLD;
        private HashSet <String> itemMap;
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
            	sum += val.get();
            }
        	
        	if (sum >= s) {
            	result.set(sum);
            	String [] tempItems = key.toString().split(",");
            	itemMap = (HashSet<String>)Apriori_Main.itemMap.clone();
            	String reducedItem = "";
            	if (Apriori_Main.NUMBER_COMBINATIONS == 1) {
	            	for (int i= 0; i<tempItems.length; i++) {
	            		int reduce = getIndex(itemMap, tempItems[i]);
	           			reducedItem += getIndex(itemMap, tempItems[i]);
	            		Apriori_Main.singleItemsets.add(getIndex(itemMap, tempItems[i])+"");
	         			if (i != tempItems.length-1) {
            				reducedItem+= ",";
            			}
	            	}
	            	key.set(reducedItem);
            	}
            	else for (String item : tempItems) Apriori_Main.singleItemsets.add(item);
            	        	
            	context.write(key, result);
            }
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