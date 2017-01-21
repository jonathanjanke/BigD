import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/***
 * 
 * @author Jonathan
 *
 *	currently not working because of attempt to convert numbers back to Strings - problem to be solved...
 *
 */

public class AssociationConstruction_Combiner extends Reducer<Text,Text,Text,Text> {
        private Text resultValue = new Text();
        private Text resultKey = new Text();
        private double confidence = Apriori_Main.CONFIDENCE;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                HashMap<String, Integer> supports = new HashMap<String, Integer>();
                int support = 1;
                String keyString = key.toString();
                String valString;
                Object [] itemMapElements = Apriori_Main.itemMap.toArray();
                
                if (!keyString.equals("")) {
                	String [] keys = keyString.split(",");
                	keyString = "";
                	for (int i = 0; i<keys.length; i++) {
                		if (keys[i].matches("\\d+")) keyString += (String)itemMapElements[Integer.parseInt(keys[i])];
                		else keyString += keys[i];
                		if (i<keys.length-1) keyString += ",";
                	}
                	for (Text val : values) {
                		valString = val.toString();
                    	String [] a = valString.split(":");
                    	if (a[0].matches("\\d+")) a[0] = (String)itemMapElements[Integer.parseInt(a[0])];
                    	valString = a[0] + ":" + a[1];
                    	val.set(valString);
                    	key.set(keyString);
                    	context.write(key, val);
                    }
                } else {
                	for (Text val : values) {
                		valString = val.toString();
                    	String [] a = valString.split(":");
                    	if (a[0].matches("\\d+")) a[0] = (String)itemMapElements[Integer.parseInt(a[0])];
                    	valString = a[0] + ":" + a[1];
                    	val.set(valString);
                    	
                		context.write(key, val);
                	}
                }           
        }
}