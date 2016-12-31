 import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;  

public class Apriori_Main {
	
	/*
	 * Ideas:
	 * - ignore baskets with certain lengths (e. g. threshold 2, basket length 1)
	 * 
	 * 
	 */
	
	
	public static final int NUMBER_COMBINATIONS = 4;
	public static final int SUPPORT_THRESHOLD = 10; //support threshold in absolute numbers
	public static final int CONFIDENCE = 3; //confidence in absolute numbers
	public static final double INTEREST = 0.8;
	
	public static int currentNumberCombinations;

	public static void main(String[] args) throws Exception {
		Configuration conf;
		Job job;
		double startTime = System.currentTimeMillis();
		for (currentNumberCombinations=1; currentNumberCombinations<=NUMBER_COMBINATIONS; currentNumberCombinations++) {
			conf = new Configuration();
			conf.set("recursion.depth", currentNumberCombinations + "");

			job = new Job(conf);
			job.setJobName("apriori " + currentNumberCombinations);
       
			job.setJarByClass(Apriori_Main.class);
			job.setMapperClass(Apriori_Mapper.class);
			//job.setCombinerClass(wordCountCombiner.class);
			job.setReducerClass(Apriori_Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path("data/" + (currentNumberCombinations-1)));
			FileOutputFormat.setOutputPath(job, new Path("data/" + currentNumberCombinations));
			if (job.waitForCompletion(true)) {
				double endTime = System.currentTimeMillis();
				double executionTime = (endTime - startTime) / 60000;
				System.out.println("Runtime: " + executionTime);
				System.exit(0);
			} else {
				System.out.println("Runtime Error");
			}
		}
	}
}

