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

import java.awt.event.ItemListener;
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
	
	public static final int TOTAL_NUMBER_COMBINATIONS = 15;
	public static double RELATIVE_SUPPORT_THRESSHOLD = 0;
	public static int SUPPORT_THRESHOLD = 100; //support threshold in absolute numbers
	public static final double CONFIDENCE = 0.5; //confidence in relative numbers
	
	
	public static final String EMPTY_SYMBOL = "{X}";
	public static final int HASH_BUCKET_NUMBER = 100;
	
	
	public static HashSet <Integer> singleItemsets;
	public static HashMap <Integer, String> itemMap;
	public static HashMap <String, Integer> inverseItemMap;
	public static HashSet <Integer> hashedItems;
	public static ArrayList <ArrayList<Integer>> frequentItems;
	public static ArrayList <ArrayList<Integer>> frequentSets;
	
	public static int DYNAMIC_NUMBER_LINES = 0;
	public static int NUMBER_COMBINATIONS = 1;
	public static boolean CREATE_HASHSET = false;
	public static boolean CREATE_ITEMSET = true;
	public static boolean CALCULATE = true;
	
	public static void main(String[] args) throws Exception {
		if (RELATIVE_SUPPORT_THRESSHOLD<= 0) CALCULATE = false;
		if (args.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(0);
		}
		double startTime = System.currentTimeMillis();
	    if (ToolRunner.run(new Configuration(), new Apriori_Main(), args)==0) {
	    	double endTime = System.currentTimeMillis();
			double executionTime = (endTime - startTime) / 1000;
			System.out.println("Run succesful - Association Rules created.");
			System.out.println("Runtime: " + executionTime + " seconds");
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
	  
		  Path inputPath = new Path(args[0]);
		  Path firstOutputPath;
		  
		  singleItemsets = new HashSet<Integer>();
		  frequentItems = new ArrayList<ArrayList<Integer>>(1);
		  inputPath.getFileSystem(new Configuration()).delete(new Path("data/"), true);

		  while ((NUMBER_COMBINATIONS<=TOTAL_NUMBER_COMBINATIONS && Apriori_Main.singleItemsets.size()>0 && Apriori_Main.frequentItems.size()>0) || NUMBER_COMBINATIONS==1) {
			  hashedItems = new HashSet<Integer>();
			  
			  Job frequentItemsetJob = new Job(conf, "Job1");
			  frequentItemsetJob.setJarByClass(Apriori_Main.class);

			  firstOutputPath = new Path("data/" + NUMBER_COMBINATIONS + "/2_frequent_itemsets");

			  frequentItemsetJob.setMapperClass(FrequentItemset_Mapper.class);
			  frequentItemsetJob.setCombinerClass(FrequentItemset_Combiner.class);	  
			  frequentItemsetJob.setReducerClass(FrequentItemset_Reducer.class);

			  frequentItemsetJob.setOutputKeyClass(Text.class);
			  frequentItemsetJob.setOutputValueClass(IntWritable.class);

			  frequentItemsetJob.setInputFormatClass(TextInputFormat.class);
			  frequentItemsetJob.setOutputFormatClass(TextOutputFormat.class);
			  
			  if (Apriori_Main.NUMBER_COMBINATIONS!=1) {
				  //if (Apriori_Main.NUMBER_COMBINATIONS == 2) {
					  inputPath = new Path("data/" + (NUMBER_COMBINATIONS) + "/1_input");
				  //}
				  Apriori_Main.CREATE_HASHSET = true;
			  } else {
				  //job.setCombinerClass(FrequentItemset_Combiner.class);
				  itemMap = new HashMap<Integer, String>();
				  inverseItemMap = new HashMap<String, Integer>();
			  }
			  
			  //itemsets = new HashMap<String, Integer>();
			  singleItemsets = new HashSet<Integer>();
			  
			  TextInputFormat.addInputPath(frequentItemsetJob, inputPath);
			  TextOutputFormat.setOutputPath(frequentItemsetJob, firstOutputPath);
			  
			  if (!frequentItemsetJob.waitForCompletion(true)) {
				  System.out.println("First Job Failed");
				  return 1;
			  } else {
				  if (Apriori_Main.singleItemsets.size()==0) {
					  System.out.println("All frequent itemsets successfully created.");
					  break;
				  };
				  if (CREATE_ITEMSET) Apriori_Main.frequentSets = createSets();
				  
//				  System.out.println(singleItemsets.toString());
//				  System.out.println(singleItemsets.size());
				  /*
				   * Job 2
				   */
				  
				  Job inputBasketCreatorJob = new Job(conf, "Job 2");
				  inputBasketCreatorJob.setJarByClass(Apriori_Main.class);
		
				  inputBasketCreatorJob.setMapperClass(BasketCreator_Mapper.class);
				  inputBasketCreatorJob.setReducerClass(BasketCreator_Reducer.class);
		
				  inputBasketCreatorJob.setOutputKeyClass(Text.class);
				  inputBasketCreatorJob.setOutputValueClass(IntWritable.class);
		
				  inputBasketCreatorJob.setInputFormatClass(TextInputFormat.class);
				  inputBasketCreatorJob.setOutputFormatClass(TextOutputFormat.class);
				  Path itemsetOutputPath = new Path("data/" + (NUMBER_COMBINATIONS+1) + "/1_input");
				  
				  TextInputFormat.addInputPath(inputBasketCreatorJob, inputPath);
				  TextOutputFormat.setOutputPath(inputBasketCreatorJob, itemsetOutputPath);
				  
				  if (!inputBasketCreatorJob.waitForCompletion(true)) {
					  System.out.println("Second Job Failed");
					  return 1;
				  } else {
					  System.out.println("Frequent " + NUMBER_COMBINATIONS + "-Itemsets successfully created.");
					  NUMBER_COMBINATIONS++;			  
				  }
			  }
		  }
		  
		  Path tempOutput = new Path("data/tempFreq/combinedItemsets.txt");
		  Path secondInputPath = new Path(args[1] + "/Frequent_Itemsets");
		  Path outputPath = new Path(args[1] + "/Association_Rules");

		  // File to write
		  File output = new File("data/tempFreq/combinedItemsets.txt");

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
		  
		  Job associationRuleConstructionJob = new Job(conf, "Job 3");
		  associationRuleConstructionJob.setJarByClass(Apriori_Main.class);

		  associationRuleConstructionJob.setMapperClass(AssociationConstruction_Mapper.class);
		  associationRuleConstructionJob.setCombinerClass(AssociationConstruction_Combiner.class);
		  associationRuleConstructionJob.setReducerClass(AssociationConstruction_Reducer.class);

		  associationRuleConstructionJob.setOutputKeyClass(Text.class);
		  associationRuleConstructionJob.setOutputValueClass(Text.class);

		  associationRuleConstructionJob.setInputFormatClass(TextInputFormat.class);
		  associationRuleConstructionJob.setOutputFormatClass(TextOutputFormat.class);
  	
		  outputPath.getFileSystem(new Configuration()).delete(outputPath, true);
		  
		  TextInputFormat.addInputPath(associationRuleConstructionJob, tempOutput);
		  TextOutputFormat.setOutputPath(associationRuleConstructionJob, outputPath);
		  
		  if (!associationRuleConstructionJob.waitForCompletion(true)) {
			  System.out.println("Third Job Failed");
			  return 1;
		  }
		  
		  return 0;
	}

	private ArrayList<ArrayList<Integer>> createSets() {
		ArrayList<ArrayList<Integer>> itemsets = Apriori_Main.frequentItems;
		int currentSizeOfItemsets = itemsets.get(0).size();
    		
    	HashMap<String, int[]> itemsetCandidates = new HashMap<String, int[]>();
    	
        // compare each pair (n-2)-combinations
        for(int i=0; i<itemsets.size(); i++) {
            for(int j=i+1; j<itemsets.size(); j++) {
                ArrayList<Integer> firstCombination = itemsets.get(i);
                ArrayList<Integer> secondCombination = itemsets.get(j);

                //string of the first n-2 tokens from both combinations
                int [] newCandandidate = new int[currentSizeOfItemsets+1];
                for(int k=0; k<newCandandidate.length-1; k++) {
                	newCandandidate[k] = firstCombination.get(k);
                }
                    
                int numberDifferent = 0;
                // find missing value
                for(int m=0; m<secondCombination.size(); m++) {
                	boolean found = false;
                	// is secondCombination[m] in firstCombination?
                    for(int n=0; n<firstCombination.size(); n++) {
                    	if (firstCombination.get(n)==secondCombination.get(m)) { 
                    		found = true;
                    		break;
                    	}
                	}
                	if (!found){ // secondCombination[m] is not in firstCombination
                		numberDifferent++;
                		if (numberDifferent>1) break;
                		// missing value at the end of newCandidate
                		newCandandidate[newCandandidate.length -1] = secondCombination.get(m);
                	}
            	
            	}
                          
                if (numberDifferent==1) {
                	// Arrays.toString to reuse equals and hashcode of String
                	Arrays.sort(newCandandidate);
                	itemsetCandidates.put(Arrays.toString(newCandandidate),newCandandidate);
                }
            }
        }
        
        //set the new itemsets
        itemsets = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> combinations;
        for (int [] element : itemsetCandidates.values()) {
        	combinations = combinations(element, element.length-1);
        	if (Apriori_Main.frequentItems.containsAll(combinations)) {
        		ArrayList<Integer> itemlist = new ArrayList<Integer>();
            	for (int el : element) {
            		itemlist.add(el);
            	}
            	itemsets.add(itemlist);
        	}
        }
        System.out.println(itemsets.toString());
        return itemsets;
	}
	
	public static ArrayList<ArrayList<Integer>> combinations (int[] set, int k) {
		int [][] tempCombinations = combineToArray (set, k);
		ArrayList<ArrayList<Integer>> finalCombinations = new ArrayList<ArrayList<Integer>>();
		for (int [] temp1 : tempCombinations) {
			ArrayList<Integer> tempList = new ArrayList<Integer>();
			for (int temp2 : temp1) {
				tempList.add(temp2);
			}
			finalCombinations.add(tempList);
		}
		return finalCombinations;
	}
	
	private static int[][] combineToArray (int [] elements, int combinationSize) {

		ArrayList<int[]> temp = combine(elements, combinationSize);
		if (NUMBER_COMBINATIONS > 2 && Apriori_Main.CREATE_ITEMSET) {
			int [] combination;
			int j = 0;
//			System.out.print(temp.size() + " - ");
			while (j<temp.size()) {
				combination = temp.get(j);
				ArrayList<Integer> tempo = new ArrayList<Integer>();
				for (int comb : combination) {
					tempo.add(comb);
				}

				if (!Apriori_Main.frequentSets.contains(tempo)) {
					temp.remove(j);
				} else {
					j++;
				}

			}
//			System.out.println(temp.size());
		}
		int [][] combinations = new int [temp.size()][];
		for (int i=0; i<temp.size(); i++) {
			combinations [i] = temp.get(i);
		}
		
		if (combinationSize > 0) return combinations;
		else return null;
	}
	
	
	private static ArrayList<int[]> combine (int [] elements, int combinationSize) {
		ArrayList<int []> combinations = new ArrayList<int []>();
		//recursion base
		if ((elements.length == 1)||combinationSize == 1||elements.length<combinationSize) {
			for (int el : elements) {
				int [] s = {el};
				combinations.add(s);
			}
		}
		//recursion step
		else {
			for (int i=0; i<=elements.length-combinationSize; i++) {
				//get ith element from elements and set it to array
				int [] s = {elements[i]};
				
				// call function on remaining elements;
				int [] tempElements = new int [elements.length-i-1];
				for (int k = 0; k<tempElements.length; k++) {
					tempElements[k] = elements[k+i+1];
				}
				
				ArrayList<int []> temp = combine (tempElements, combinationSize-1);
				for (int [] t : temp) combinations.add(concat(s, t));
			}
		}
		return combinations;
	}
	

	
	//from the internet: http://stackoverflow.com/questions/80476/how-can-i-concatenate-two-arrays-in-java
	
	public static int[] concat(int[] a, int[] b) {
 	   int aLen = a.length;
 	   int bLen = b.length;
 	   int[] c= new int[aLen+bLen];
 	   System.arraycopy(a, 0, c, 0, aLen);
 	   System.arraycopy(b, 0, c, aLen, bLen);
 	   return c;
 	}
	
	
}

