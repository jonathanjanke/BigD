import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BasketCreator_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private static IntWritable returnValue = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
            	if (key.toString().split(",").length>Apriori_Main.NUMBER_COMBINATIONS+1) {
            		context.write(key, returnValue);
            	}
            }
        }
        

}