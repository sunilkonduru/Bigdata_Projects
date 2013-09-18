import java.io.BufferedReader;
import org.apache.hadoop.mapreduce.*;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CorpusCalculator extends Configured implements Tool {

	/**
	 * Just emit <word,position of the word in sentence> I/P of Mapper: <0,fox
	 * hello world fox!!> <24, hello> O/p of Mapper: <fox,1> <hello,2> <world,3>
	 * <fox,4> <hello,1> <rabbit,2>
	 */

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable location;
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			int locationOfWord = 0;
			while (itr.hasMoreTokens()) {
				locationOfWord += 1;
				word.set(itr.nextToken());
				location = new IntWritable(locationOfWord);
				output.collect(word, location);
			}
		}
	}

	/**
	 * Input that this function receives is <Text,Iterator<IntWritable>> Output
	 * would be <Text, Text> Input: <fox,1,5,1,4> <eats,2> <rabbit,1,3>
	 * 
	 * Output: <<fox,1>,2> Representation: fox$$1 2 <<fox,4>,1> <<fox,5>,1>
	 * <<eats,2>,1> <rabbit,1>,1> <rabbit,3>,1>
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			/**
			 * for given sentence <fox 1,5,1,4> My HashMap will contain (1,2) 1
			 * is the position of word fox in the sentences and 2 is the count
			 */
			HashMap<Integer, Integer> hmap = new HashMap<Integer, Integer>();
			List<IntWritable> cache=new ArrayList<IntWritable>();
			while (values.hasNext()) {
				int positionOfKey = values.next().get();
				if (hmap.containsKey(positionOfKey)) {
					int noOfWordsAtPosition = hmap.get(positionOfKey);
					noOfWordsAtPosition = noOfWordsAtPosition + 1;
					hmap.put(positionOfKey, noOfWordsAtPosition);
				} else {
					hmap.put(positionOfKey, 1);
					cache.add(new IntWritable(positionOfKey));
				}
			}
			String keyTempValue;
			Text newText=new Text();
			for(IntWritable tempValue:cache)
			{
				keyTempValue="";
				keyTempValue=key.toString();
				keyTempValue=keyTempValue+"$$"+tempValue;
				newText.set(keyTempValue);
				int temp=tempValue.get();
				int findValueInHmap= hmap.get(temp);
				output.collect(newText,new IntWritable(findValueInHmap));
			}
			
	
//			int sum = 0;
//			while (values.hasNext()) {
//				sum += values.next().get();
//			}
//			output.collect(key, new IntWritable(sum));
		}
	}
	
	/**
	 * 
	 * 
	 *
	 */

	  public static class MapClass1 extends MapReduceBase
	    implements Mapper<LongWritable, Text, IntWritable, Text> {

	    
	    private Text word = new Text();
       
	    public void map(LongWritable key, Text value,
	                    OutputCollector<IntWritable, Text> output,
	                    Reporter reporter) throws IOException {
	      /**
	       * Get the output from the previous mapper and split that into 3 strings
	       * 
	       * Example Input:
	       * 
	       * fox$$1   5
	       * help$$1  4
	       * 
	       * fox is present in 5 sentences as first character.
	       * 
	       * prevMapperSplitKey[0]=fox; prevMapperSplitTkey[1]=1; prevMapperValue=5
	       *  
	       */
	      String line = value.toString();
	      //StringTokenizer itr = new StringTokenizer(line,"\\s+");
	      String[] splittedValues=line.split("\\s");
	      String prevMapperKey="";
	      String prevMapperValue="";
	      String[] prevMapperSplitKey;
	      String finalValueOfMapPhase="";
	      int tempValue=0;
	      HashMap<Integer,Integer> hmap=new HashMap<Integer,Integer>();
	      if (splittedValues.length>1) {
	          prevMapperKey=splittedValues[0];;
	          prevMapperValue=splittedValues[1];
	    	  prevMapperSplitKey=prevMapperKey.split("\\$\\$");
	    	  tempValue=Integer.parseInt(prevMapperSplitKey[1]);
	          finalValueOfMapPhase=prevMapperSplitKey[0]+"$$"+prevMapperValue;
	    	  output.collect(new IntWritable(tempValue), new Text(finalValueOfMapPhase));
	      }
	    }
	  }

	  /**
	   * A reducer class that just emits the sum of the input values.
	   * 
	   * I/P: <1,fox$$2, eats$$4>
	   * O/P: <fox$$1,2/6> <eat, 4/6>
	   * 
	   * Iterate through the list and count the number 2+4+...and store it one variable. 
	   * 
	   * Use the above variable as denominator and form fox$$1, 2/final variable value that we get.
	   */
	  public static class Reduce1 extends MapReduceBase
	    implements Reducer<IntWritable, Text, Text, Text> {

	    public void reduce(IntWritable key, Iterator<Text> values,
	                       OutputCollector<Text, Text> output,
	                       Reporter reporter) throws IOException {
	    	
	    	List<String> cache=new ArrayList<String>();
	    	String valueOfTheList="";
	    	String[] splitMappersValue;
	    	int totalCountOfSentences=0;
	    	Text tempVar=new Text();
	    	while(values.hasNext())
	    	{
	    	  valueOfTheList=values.next().toString();
	    	  splitMappersValue=valueOfTheList.split("\\$\\$");
	    	  totalCountOfSentences+=Integer.parseInt(splitMappersValue[1]);
	    	  cache.add(valueOfTheList);
	    	}
	    	
	    	String tempKey="";
	    	
	    	double probSingleWordAtPos;
	    	String probString="";
	    	float value1;
	    	int value2;
	    	String splitAgainM;
	    	for(String s: cache)
	    	{
	    	 probString="";
	    	 splitMappersValue= s.split("\\$\\$");
	    	 value1=Float.parseFloat(splitMappersValue[1]);
	    	 value2=totalCountOfSentences;
	    	 probSingleWordAtPos=value1/value2;
	    	 probString+=probSingleWordAtPos;
	    	 tempKey=splitMappersValue[0]+"$$"+key;
	         output.collect(new Text(tempKey), new Text(probString));   	 
	    		
	    	}
	    	
	    	
	    	
//	      int sum = 0;
//	      while (values.hasNext()) {
//	        sum += values.next().get();
//	      }
//	      output.collect(key, new IntWritable(sum));
	    }
	  }
	  /**
       * Cache the output from the previous reducer into each of the datanodes machines
       * 
       * Example Input:
       * 
       * put these in a hashmap:
       * ----------------------
       * fox$$1   9.41287348621843e12
       * help$$1  4.112339848703986e13
       * 
       * Read each file line by line that would be my value.
       * 
       * read each word of the line and concatenate with its position say fox$$1
       *  and check in Hmap the probability of the word at that position. Multiply each
       *  words of the sentence and compute the total sentence's probability.
       * 
       *  emit(probability, sentence).
       */

	  public static class MapClass2 extends MapReduceBase
	    implements Mapper<LongWritable, Text, FloatWritable, Text> {

	    
	     private Text word = new Text();
	     private HashMap<String,Float> loadFile=new HashMap<String,Float>();

		public void configure(JobConf job) {
			System.out.println("inside configure()");
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

				if (cacheFiles != null && cacheFiles.length > 0) {
					System.out.println("Inside setup(): "
							+ cacheFiles[0].toString());

					String line;

					BufferedReader cacheReader = new BufferedReader(
							new FileReader(cacheFiles[0].toString()));
					try {
						String[] splitInputSentence;
						float probability;
						while ((line = cacheReader.readLine()) != null) {
						splitInputSentence=	line.split("\\s");
						probability =Float.parseFloat(splitInputSentence[1].toString());
						loadFile.put(splitInputSentence[0],new Float(probability));
						}
					} finally {
						cacheReader.close();
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistribtuedCache: " + e);
			}
		}

	    public void map(LongWritable key, Text value,
	                    OutputCollector<FloatWritable, Text> output,
	                    Reporter reporter) throws IOException {
	     
	      String line = value.toString();
	      String temp="";
	      String lineSplits[]=line.split("\\s");
	      int lineLength=0;
	      Float computedProabability=1.0f;
	      String tempHashKey="";
	      int index=0;
	      while(lineLength!=lineSplits.length)
	      {   
	    	  tempHashKey="";
	    	  index=lineLength+1;
	    	  tempHashKey+=lineSplits[lineLength]+"$$"+index;
	    	  computedProabability*=loadFile.get(tempHashKey);
	    	  lineLength++;
	      }
	      
	      output.collect(new FloatWritable(computedProabability), value);
	      
	     }
	  }

	  /**
	   * A reducer class that just emits the sum of the input values.
	   * 
	   * I/P: <1,fox$$2, eats$$4>
	   * O/P: <fox$$1,2/6> <eat, 4/6>
	   * 
	   * Iterate through the list and count the number 2+4+...and store it one variable. 
	   * 
	   * Use the above variable as denominator and form fox$$1, 2/final variable value that we get.
	   */
	  
	  public static class Reduce2 extends MapReduceBase
	    implements Reducer<FloatWritable, Text, Text, Text> {

		private int i=0;
	    public void reduce(FloatWritable key, Iterator<Text> values,
	                       OutputCollector<Text, Text> output,
	                       Reporter reporter) throws IOException {
	         
	    	for(;i<3;i++)
	    	{
	    		int count=0;
	    		while(values.hasNext())
	    		{
	    			output.collect(new Text(""),new Text(values.next()) );
	    			if(count==2)break;
	    			count++;
	    		}	
	    	}
	    	
	    	
	    	
//	      int sum = 0;
//	      while (values.hasNext()) {
//	        sum += values.next().get();
//	      }
//	      output.collect(key, new IntWritable(sum));
	    }
	    
	    
	  }

/**
	 * The main driver for map/reduce program. Invoke this method to submit the
	 * map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	  
	  
	  
	
	public int run(String[] args) throws Exception {

		final String inputPathForMapper1="/home/Output/";
		final String outputPathOfReducer1 = "/home/hduser1/Output/A59.txt";
		final String outputPathofReducer2="/home/hduser1/Output/A60.txt";
		final String outputPathofReducer3="/home/hduser1/Output/A61.txt";
		JobConf conf = new JobConf(getConf(), CorpusCalculator.class);
		conf.setJobName("CorpusCalculator");

		// the keys are words (strings) , the values are counts (String)
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);

		conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));

		FileInputFormat.setInputPaths(conf, new Path(inputPathForMapper1));
		FileOutputFormat.setOutputPath(conf, new Path(outputPathOfReducer1));

		JobClient.runJob(conf);
		// can I write another JobClient.runJob(conf1)?
		
		JobConf conf1 = new JobConf(getConf(), CorpusCalculator.class);
		conf1.setJobName("CorpusCalculator1");

		// the keys are words (strings) , the values are counts (String)
		
		conf1.setMapOutputKeyClass(IntWritable.class);
		conf1.setMapOutputValueClass(Text.class);
		
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		conf1.setMapperClass(MapClass1.class);
		conf1.setReducerClass(Reduce1.class);
         
		conf1.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf1.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));

		FileInputFormat.setInputPaths(conf1, new Path(outputPathOfReducer1));
		FileOutputFormat.setOutputPath(conf1, new Path(outputPathofReducer2));

		JobClient.runJob(conf1);
		
		//Job 3 
		
		JobConf conf2 = new JobConf(getConf(), CorpusCalculator.class);
		conf2.setJobName("CorpusCalculator2");
       
		//Distributed Caching of the file emitted by the reducer2 is done here
		conf2.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
		conf2.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));
		
		//cacheFile(conf2, new Path(outputPathofReducer2));
		
		conf2.setNumReduceTasks(1);
		//conf2.setOutputKeyComparatorClass()
		
		conf2.setMapOutputKeyClass(FloatWritable.class);
		conf2.setMapOutputValueClass(Text.class);
		
		
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(MapClass2.class);
		conf2.setReducerClass(Reduce2.class);
         
		

		FileInputFormat.setInputPaths(conf2, new Path(inputPathForMapper1));
		FileOutputFormat.setOutputPath(conf2, new Path(outputPathofReducer3));
        
		DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/home/hduser1/Output/A65.txt"), conf2);
		JobClient.runJob(conf2);
		// can I write another JobClient.runJob(conf1)?
		
		
	
		
		

		return 0;
	}
	public void cacheFile(JobConf conf,Path hdfsFile) throws IOException {
		 DistributedCache.addCacheFile(hdfsFile.toUri(), conf);
		 System.out.println(hdfsFile.toUri().toString());
		 
		 }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CorpusCalculator(),
				args);
		System.exit(res);
	}

}
