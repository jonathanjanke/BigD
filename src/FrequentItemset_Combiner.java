import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentItemset_Combiner extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        private int s = Apriori_Main.SUPPORT_THRESHOLD;
        boolean write = false;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            
//            System.out.println("FrequentItemset_Combiner");
            	for (Text val : values) {
            		String value = val.toString();
            		if (val.toString().equals("")) {
            			context.write(key, val);
            		} else {
            			write = true;
            			counter = counter + Integer.parseInt(val.toString());
            		}
            	}
            	if (write) {
            		String kkkk = key.toString();
            		Apriori_Main.hashMap.put(key.toString(), counter);
            	}
        	
        	
        }
        

}