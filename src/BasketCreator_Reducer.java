import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BasketCreator_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private static IntWritable returnValue = new IntWritable(1);
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            if (key.toString().split(",").length>=Apriori_Main.NUMBER_COMBINATIONS+1) {
            	for (IntWritable val : values) {
            		count += val.get();
            	}
            	returnValue.set(count);
        		context.write(key, returnValue);
        	}
        }
}