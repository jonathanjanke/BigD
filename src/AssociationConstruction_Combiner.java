import java.io.IOException;
import java.util.HashMap;

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
        private HashMap <Integer, String> itemMap = Apriori_Main.itemMap;
        private double confidence = Apriori_Main.CONFIDENCE;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                HashMap<String, Integer> supports = new HashMap<String, Integer>();
                int support = 1;
                
                String keyString = key.toString();
                String valString;
                //Object [] itemMapElements = Apriori_Main.itemMap.toArray();
                if (!keyString.equals("")) {
                	String [] keys = keyString.split(",");
                	keyString = "";
                	for (int i = 0; i<keys.length; i++) {
                		if (keys[i].matches("\\d+")) keyString += itemMap.get(Integer.parseInt(keys[i]));
                		else keyString += keys[i];
                		if (i<keys.length-1) keyString += ",";
                	}
                	for (Text val : values) {
                		valString = val.toString();
                    	String [] a = valString.split(":");
                    	if (a[0].matches("\\d+")) a[0] = itemMap.get(Integer.parseInt(a[0]));
                    	valString = a[0] + ":" + a[1];
                    	resultValue.set(valString);
                    	resultKey.set(keyString);
                    	context.write(resultKey, resultValue);
                    }
                } else {
                	for (Text val : values) {
                		valString = val.toString();
                    	String [] a = valString.split(":");
                    	if (a[0].matches("\\d+")) a[0] = itemMap.get(Integer.parseInt(a[0]));
                    	valString = a[0] + ":" + a[1];
                    	resultValue.set(valString);                    	
                		context.write(key, resultValue);
                	}
                }           
        }
}