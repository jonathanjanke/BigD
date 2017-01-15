import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.*;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

public class Apriori_Main extends Configured implements Tool {
	
	/*
	 * Ideas:
	 * - ignore baskets with certain lengths (e. g. threshold 2, basket length 1)
	 * 
	 * 
	 */
	
	public static int NUMBER_COMBINATIONS = 1;
	public static final int TOTAL_NUMBER_COMBINATIONS = 10;
	public static double RELATIVE_SUPPORT_THRESSHOLD = 0;
	public static int SUPPORT_THRESHOLD = 10; //support threshold in absolute numbers
	public static final double CONFIDENCE = 0.5; //confidence in relative numbers
//	public static final double INTEREST = 0.8;
	public static final Object EMPTY_SYMBOL = "{X}";
	public static int NUMBER_LINES = 9835;
	//public static HashMap <String, Integer> itemsets;
	public static HashSet <String> singleItemsets;
	public static HashMap <String, Integer> hashMap;

	public static void main(String[] args) throws Exception {
		if (RELATIVE_SUPPORT_THRESSHOLD>0) {
			SUPPORT_THRESHOLD = (int)(NUMBER_LINES * RELATIVE_SUPPORT_THRESSHOLD);
		}
		
		if (args.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(0);
		}
		double startTime = System.currentTimeMillis();
	    if (ToolRunner.run(new Configuration(), new Apriori_Main(), args)==0) {
	    	double endTime = System.currentTimeMillis();
			double executionTime = (endTime - startTime) / 60000;
			System.out.println("Run succesful");
			System.out.println("Runtime: " + executionTime);
	    } else {
		    System.out.println("Run not succesful");
	    }
	    System.exit(0);
	}

	@Override
	public int run(String[] args) throws Exception {
			/*
		   * Job 1
		   */
		  Configuration conf = getConf();
		  FileSystem fs = FileSystem.get(conf);
	  
		  Path inputPath = new Path(args[0]);;
		  Path firstOutputPath;
		  
		  singleItemsets = new HashSet<String>();
		  inputPath.getFileSystem(new Configuration()).delete(new Path("data/"), true);

		  while ((NUMBER_COMBINATIONS<=TOTAL_NUMBER_COMBINATIONS && this.singleItemsets.size()>0) || NUMBER_COMBINATIONS==1) {
			  System.out.println("Combination: " + NUMBER_COMBINATIONS);	
			  
			  hashMap = new HashMap<String, Integer>();
			  
			  Job job = new Job(conf, "Job1");
			  job.setJarByClass(Apriori_Main.class);

			  firstOutputPath = new Path("data/" + NUMBER_COMBINATIONS + "/2_frequent_itemsets");

			  job.setMapperClass(FrequentItemset_Mapper.class);
			  

			  
			  job.setReducerClass(FrequentItemset_Reducer.class);

			  job.setOutputKeyClass(Text.class);
			  job.setOutputValueClass(IntWritable.class);

			  job.setInputFormatClass(TextInputFormat.class);
			  job.setOutputFormatClass(TextOutputFormat.class);
			  
			  if (this.NUMBER_COMBINATIONS!=1) {
				  inputPath = new Path("data/" + (NUMBER_COMBINATIONS) + "/1_input");
			  } else {
//				  job.setCombinerClass(FrequentItemset_Combiner.class);
			  }
			  
			  //itemsets = new HashMap<String, Integer>();
			  singleItemsets = new HashSet<String>();
			  
			  TextInputFormat.addInputPath(job, inputPath);
			  TextOutputFormat.setOutputPath(job, firstOutputPath);
			  
			  if (!job.waitForCompletion(true)) {
				  System.out.println("First Job Failed");
				  return 1;
			  } else {
				  //itemsetList.add(itemsets);
				  System.out.println(singleItemsets.toString());
				  
				  /*
				   * Job 2
				   */
				  
				  Job job2 = new Job(conf, "Job 2");
				  job2.setJarByClass(Apriori_Main.class);
		
				  job2.setMapperClass(BasketCreator_Mapper.class);
				  job2.setReducerClass(BasketCreator_Reducer.class);
		
				  job2.setOutputKeyClass(Text.class);
				  job2.setOutputValueClass(Text.class);
		
				  job2.setInputFormatClass(TextInputFormat.class);
				  job2.setOutputFormatClass(TextOutputFormat.class);
				  Path itemsetOutputPath = new Path("data/" + (NUMBER_COMBINATIONS+1) + "/1_input");
				  
				  TextInputFormat.addInputPath(job2, inputPath);
				  TextOutputFormat.setOutputPath(job2, itemsetOutputPath);
				  
				  if (!job2.waitForCompletion(true)) {
					  System.out.println("Second Job Failed");
					  return 1;
				  }
			  }
			  NUMBER_COMBINATIONS++;
		  }
		  
		  Path secondInputPath = new Path(args[1] + "/Frequent_Itemsets");
		  Path outputPath = new Path(args[1] + "/Association_Rules");

		  // File to write
		  File output = new File(args[1] + "/Frequent_Itemsets/combinedItemsets.txt");

		  // Read the file as string
		  String outputString = "";
		  File tempDest;
		  for (int i =1; i < NUMBER_COMBINATIONS; i++) {
			  tempDest = new File("data/" + i + "/2_frequent_itemsets/part-r-00000");
			  outputString += FileUtils.readFileToString(tempDest);
		  }

		  // Write the file
		  FileUtils.write(output, outputString);
		  
		  /*
		   * Job 3
		   */
		  
		  Job job3 = new Job(conf, "Job 3");
		  job3.setJarByClass(Apriori_Main.class);

		  job3.setMapperClass(AssociationConstruction_Mapper.class);
		  job3.setReducerClass(AssociationConstruction_Reducer.class);

		  job3.setOutputKeyClass(Text.class);
		  job3.setOutputValueClass(Text.class);

		  job3.setInputFormatClass(TextInputFormat.class);
		  job3.setOutputFormatClass(TextOutputFormat.class);
  	
		  outputPath.getFileSystem(new Configuration()).delete(outputPath, true);
		  
		  TextInputFormat.addInputPath(job3, secondInputPath);
		  TextOutputFormat.setOutputPath(job3, outputPath);
		  
		  if (!job3.waitForCompletion(true)) {
			  System.out.println("Third Job Failed");
			  return 1;
		  }
		  
		  return 0;
	}
}

