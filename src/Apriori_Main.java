import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
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
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

public class Apriori_Main extends Configured implements Tool {
	
	/*
	 * Ideas:
	 * - ignore baskets with certain lengths (e. g. threshold 2, basket length 1)
	 * 
	 * 
	 */
	
	
	public static final int NUMBER_COMBINATIONS = 4;
	public static final int SUPPORT_THRESHOLD = 10; //support threshold in absolute numbers
	public static final double CONFIDENCE = 0.5; //confidence in relative numbers
	public static final double INTEREST = 0.8;
	public static final Object EMPTY_SYMBOL = "{X}";
	
	public static int currentNumberCombinations = 3;

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			   System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			   System.exit(0);
			  }
			  if (ToolRunner.run(new Configuration(), new Apriori_Main(), args)==0) {
				  System.out.println("Run succesful");
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
		  Job job = new Job(conf, "Job1");
		  job.setJarByClass(Apriori_Main.class);

		  job.setMapperClass(FrequentItemset_Mapper.class);
		  job.setReducerClass(FrequentItemset_Reducer.class);

		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);

		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);

		  TextInputFormat.addInputPath(job, new Path("data/" + 0 + "/input"));
		  TextOutputFormat.setOutputPath(job, new Path("data/" + 0 + "/temp"));

		  job.waitForCompletion(true);

		  /*
		   * Job 2
		   */
		  
		  Job job2 = new Job(conf, "Job 2");
		  job2.setJarByClass(Apriori_Main.class);

		  job2.setMapperClass(AssociationConstruction_Mapper.class);
		  job2.setReducerClass(AssociationConstruction_Reducer.class);

		  job2.setOutputKeyClass(Text.class);
		  job2.setOutputValueClass(Text.class);

		  job2.setInputFormatClass(TextInputFormat.class);
		  job2.setOutputFormatClass(TextOutputFormat.class);

		  TextInputFormat.addInputPath(job2, new Path("data/" + 0 + "/temp"));
		  TextOutputFormat.setOutputPath(job2, new Path("data/" + 0 + "/output"));
		  
		  return job2.waitForCompletion(true) ? 0 : 1;
	}
}

