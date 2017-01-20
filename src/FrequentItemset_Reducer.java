import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentItemset_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private int s = Apriori_Main.SUPPORT_THRESHOLD;
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (IntWritable val : values) {
            	counter ++;
            }
        	
        	if (counter >= s) {
            	result.set(counter);
            	String [] tempItems = key.toString().split(",");
            	
            	String reducedItem = "";
            	if (Apriori_Main.NUMBER_COMBINATIONS == 1) {
	            	for (int i= 0; i<tempItems.length; i++) {
	            		Apriori_Main.singleItemsets.add(tempItems [i]);
	           			reducedItem += Apriori_Main.singleItemsets.size()-1;
	         			if (i != tempItems.length-1) {
            				reducedItem+= ",";
            			}
	            	}
	            	key.set(reducedItem);
            	}
            	else for (String item : tempItems) Apriori_Main.singleItemsets.add(item);
            	//Apriori_Main.itemsets.put(key.toString(), counter);
//            	
            	context.write(key, result);
            }
        }
        

}