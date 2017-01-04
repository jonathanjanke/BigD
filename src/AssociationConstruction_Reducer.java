import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AssociationConstruction_Reducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        private int s = Apriori_Main.SUPPORT_THRESHOLD;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	System.out.println("Reduced 2 löft");
                HashMap<String, Integer> supports = new HashMap<String, Integer>();
                int support;
                for (Text val : values) {
                	String [] a = val.toString().split(":");
                	if (a.length == 1) {
                		support = Integer.parseInt(a[0]);
                		supports.put(" ", support);
                	}
                	else {
                		supports.put(a[0], Integer.parseInt(a[1]));
                	}
                }
        }
        
}