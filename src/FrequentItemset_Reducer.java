import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentItemset_Reducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        private int s = Apriori_Main.SUPPORT_THRESHOLD;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Text val : values) {
            	counter ++;
            }
        	
        	if (counter >= s) {
            	result.set(counter+"");
            	context.write(key, result);
            }
        }
        

}