import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;  

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

public class Apriori_Main {
	
	/*
	 * Ideas:
	 * - ignore baskets with certain lengths (e. g. threshold 2, basket length 1)
	 * 
	 * 
	 */
	
	
	public static final int NUMBER_COMBINATIONS = 10;
	public static final int SUPPORT_THRESHOLD = 3; //support threshold in absolute numbers
	public static final double CONFIDENCE = 0.5; //confidence in relative numbers
	public static final double INTEREST = 0.8;
	
	public static int currentNumberCombinations = 2;

	public static void main(String[] args) throws Exception {
		Configuration conf;
		Configuration conf2;
		Job supportJob;
		Job associationJob;
		double startTime = System.currentTimeMillis();
//		for (;currentNumberCombinations<=NUMBER_COMBINATIONS; currentNumberCombinations++) {
		conf = new Configuration();
		
		conf2 = new Configuration();
		//conf.set("recursion.depth", currentNumberCombinations + "");

		supportJob=Job.getInstance(conf);
		
		supportJob.setJobName("support " + currentNumberCombinations);
		supportJob.setJarByClass(Apriori_Main.class);
		supportJob.setMapperClass(FrequentItemset_Mapper.class);
		supportJob.setReducerClass(FrequentItemset_Reducer.class);
		supportJob.setOutputKeyClass(Text.class);
		supportJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(supportJob, new Path("data/" + 0 + "/input"));
		FileOutputFormat.setOutputPath(supportJob, new Path("data/" + 0 + "/temp"));
			
//			FileInputFormat.addInputPath(job, new Path("data/" + (currentNumberCombinations-1)));
//			FileOutputFormat.setOutputPath(job, new Path("data/" + currentNumberCombinations));
			if (supportJob.waitForCompletion(true)) {

				
				associationJob=Job.getInstance(conf2);
				
				associationJob.setJobName("association " + currentNumberCombinations);
				associationJob.setJarByClass(Apriori_Main.class);
				associationJob.setMapperClass(AssociationConstruction_Mapper.class);
				associationJob.setReducerClass(AssociationConstruction_Reducer.class);
				associationJob.setOutputKeyClass(Text.class);
				associationJob.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(associationJob, new Path("data/" + 0 + "/temp"));
				FileOutputFormat.setOutputPath(associationJob, new Path("data/" + 0 + "/output"));
				
				System.out.println("Association start");
				
				if (associationJob.waitForCompletion(true)) {
				
					double endTime = System.currentTimeMillis();
					double executionTime = (endTime - startTime) / 60000;
					System.out.println("Runtime: " + executionTime);
					System.exit(0);
				} else {
					System.out.println("Runtime Error in association job");
				}
			} else {
				System.out.println("Runtime Error in support job");
			}
//		}
	}
}

