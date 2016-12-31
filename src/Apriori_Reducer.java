import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Apriori_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private int s = Apriori_Main.SUPPORT_THRESHOLD;
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int counter = 0;
                for (IntWritable val : values) {
                	counter ++;
                }
                if (counter >= s) {
                	result.set(counter);
                	context.write(key, result);
                }
        }
}