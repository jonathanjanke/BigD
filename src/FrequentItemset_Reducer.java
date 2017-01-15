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
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Text val : values) {
            	counter ++;
            }
        	
        	if (counter >= s) {
            	result.set(counter);
            	String [] tempItems = key.toString().split(",");
            	for (String item : tempItems) Apriori_Main.singleItemsets.add(item);
            	//Apriori_Main.itemsets.put(key.toString(), counter);
            	context.write(key, result);
            }
        }
        

}