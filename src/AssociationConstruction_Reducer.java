import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/***
 * 
 * @author Jonathan, Rene, Marnik
 *
 *	currently not working because of attempt to convert numbers back to Strings - problem to be solved...
 *
 */

public class AssociationConstruction_Reducer extends Reducer<Text,Text,Text,Text> {
        private Text resultValue = new Text();
        private Text resultKey = new Text();
        private double confidence = Apriori_Main.CONFIDENCE;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                HashMap<String, Integer> supports = new HashMap<String, Integer>();
                int support = 1;
                String keyString = key.toString();              
                for (Text val : values) {
                	String [] a = val.toString().split(":");
                	
                	if (a[0].equals(Apriori_Main.EMPTY_SYMBOL)) {
                		support = Integer.parseInt(a[1]);
                	} else {
                		supports.put(a[0], Integer.parseInt(a[1]));
                	}
                	
                }
                
                if (keyString.equals("")) {
                	support = Apriori_Main.DYNAMIC_NUMBER_LINES;
                }
                
                Iterator it = supports.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    double currentSupport = (int)pair.getValue();
                    double currentConfidence = currentSupport/support;
                    if  (currentConfidence >= confidence) {
	                    resultKey.set("{" + key.toString() + "} => " + (String)pair.getKey());
	                    resultValue.set(currentConfidence + "");
	                    
	                    context.write(resultKey, resultValue);
                    }
                    it.remove(); // avoids a ConcurrentModificationException
                }
        }
        
}