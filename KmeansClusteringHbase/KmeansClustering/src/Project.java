import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Project {

	public static class ReadDataMap extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException {

			Configuration config = context.getConfiguration();

			// set the table details
			String tableName = config.get("TableName", "");

			String areaFamily = "Area";
			String propertyFamily = "Property";

			String[] columnValues = value.toString().split("\\s+");

			byte[] rowkey = DigestUtils.md5(value.toString());
			Put row = new Put(rowkey);

			// temporary counters for getting qualifiers
			int temp = 1;
			int temp1 = 1;

			HTable table = new HTable(config, tableName);

			// writing them to hbase
			for (String columnValue : columnValues) {
				if (temp == 1 || temp == 5 || temp == 6) {
					row.add(Bytes.toBytes(areaFamily),
							Bytes.toBytes("X" + "" + temp),
							Bytes.toBytes(columnValue));
					temp++;
				}

				else if (temp == 2 || temp == 3 || temp == 4 || temp == 7
						|| temp == 8) {
					row.add(Bytes.toBytes(propertyFamily),
							Bytes.toBytes("X" + "" + temp),
							Bytes.toBytes(columnValue));
					temp++;
				}

				else if (temp == 9 || temp == 10) {
					row.add(Bytes.toBytes(areaFamily),
							Bytes.toBytes("y" + "" + temp1),
							Bytes.toBytes(columnValue));
					temp++;
					temp1++;
				}

			}
			table.put(row);
			table.flushCommits();

		}

	}

	public static class MyMapper extends TableMapper<Text, Text> {

		ArrayList<ArrayList<Double>> centroids = new ArrayList<ArrayList<Double>>();

		// add all the cenroids to the ArrayList for easy computation of
		// Centroids from Text file
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			
			Configuration conf = context.getConfiguration();
			
			String fileName=conf.get("centroidsFileName");
			if(fileName.contains("sunilkumarkonduru"))
			 
				fileName+="/part-r-00000";
			//System.out.println("CentroidsFileName"+fileName);
			try{
                Path pt=new Path(fileName);
                FileSystem fs = FileSystem.get(conf);
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                ArrayList<Double> temp;
                while (line != null){
                      
                	   temp=new ArrayList<Double>();
                	   String[] stringValues=line.split("\\s+");
                	   for(String value:stringValues)
                	   {
                		   temp.add(Double.parseDouble(value));
                	   }
                       line=br.readLine();
                       centroids.add(temp);
                }
        }catch(Exception e){
        }
			
//			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
//			try {
//				String line;
//				BufferedReader cacheReader = new BufferedReader(new FileReader(
//						localFiles[0].toString()));
//				try {
//					String[] splitInputSentence;
//					ArrayList<Double> temp;
//					while ((line = cacheReader.readLine()) != null) {
//						temp = new ArrayList<Double>();
//						splitInputSentence = line.split("\\s+");
//						System.out.println("Distributed Cache Values:"+line);
//						for (String value : splitInputSentence) {
//							temp.add(Double.parseDouble(value));
//							
//						}
//						centroids.add(temp);
//					}
//				} finally {
//					cacheReader.close();
//				}
//			} catch (IOException e) {
//				System.err.println("Exception reading DistribtuedCache:" + e);
//			}
		}

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {

			ArrayList<Double> doubleList = new ArrayList<Double>();
			for (int i = 1; i <= 10; i++) {
				if (i == 1 || i == 5 || i == 6) {
					Double i1 = Double.parseDouble(new String(value.getValue(
							Bytes.toBytes("Area"), Bytes.toBytes("X" + i))));
					//System.out.println("Double i1"+i1);
					doubleList.add(i1);
				} else if (i == 2 || i == 3 || i == 4 || i == 7 || i == 8) {
					Double i1 = Double
							.parseDouble(new String(value.getValue(
									Bytes.toBytes("Property"),
									Bytes.toBytes("X" + i))));
					doubleList.add(i1);
				} else if (i == 9) {
					Double i1 = Double.parseDouble(new String(value.getValue(
							Bytes.toBytes("Area"), Bytes.toBytes("y1"))));
					doubleList.add(i1);
				} else if (i == 10) {
					Double i1 = Double.parseDouble(new String(value.getValue(
							Bytes.toBytes("Area"), Bytes.toBytes("y2"))));
					doubleList.add(i1);
				}
			}

			double min = Double.MAX_VALUE;
			int bestCentroid=0;
			String tempCheck="";
			for(Double temp: doubleList)
			{
				tempCheck+=temp+" ";
			}
			//System.out.println("Input: "+tempCheck);
			//fSystem.out.println("Centroids size:"+centroids.size());
			for (int j = 0; j < centroids.size(); j++) {
				
				ArrayList<Double> tempArrayList = centroids.get(j);
				double sum = 0;
				for (int i = 0; i < 9; i++) {
					//System.out.println("tempArrayList.get(i):"+ tempArrayList.get(i));
					//System.out.println("doubleList.get(i):"+ doubleList.get(i));
					sum += Math.abs(tempArrayList.get(i) - doubleList.get(i));
				}
				//System.out.println("sum:"+j+"val:"+sum);
				//System.out.println(min);
				if (sum < min) {
					min = sum;
					bestCentroid = j;
				}
			}

			// get the Text Value of Output Key
			ArrayList<Double> bestCentroidValues = centroids.get(bestCentroid);
			String outputKey = "";
			for (int i = 0; i < bestCentroidValues.size(); i++) {
				outputKey += bestCentroidValues.get(i) + " ";
			}

			// get the Text Value of Output value with a delimiter

			
			String outputValue = "";
			for (int i = 0; i < doubleList.size(); i++) {
				outputValue += doubleList.get(i) + " ";
			}

			//System.out.println("outputKey: "+outputKey);
			//System.out.println("outputValue:"+outputValue);
			context.write(new Text(outputKey), new Text(outputValue));
		}
	}

	public static class MyTableReducer extends Reducer<Text, Text, Text,Text> {

		public static enum counter {

			CONVERGECOUNTER
		}

		List<String> finalListAvg = new ArrayList<String>();
		double errorSum=0.0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<ArrayList<Double>> newAverages = new ArrayList<ArrayList<Double>>();
			ArrayList<Double> temp;
			for (Text value : values) {
				String rowValue = value.toString();
				String[] splitValues = rowValue.split("\\s+");
				temp = new ArrayList<Double>();
				for (String splitValue : splitValues) {
					temp.add(Double.parseDouble(splitValue));
				}
				newAverages.add(temp);
			}

			ArrayList<Double> tempList1 = newAverages.get(0);
			ArrayList<Double> finalAverages = new ArrayList<Double>();

			//System.out.println("TempList size"+ tempList1.size());
			for (int i = 0; i < tempList1.size(); i++) {
				double sum = 0;
				for (int j = 0; j < newAverages.size(); j++) {
					ArrayList<Double> tempList = newAverages.get(j);
					sum += tempList.get(i);
				}
				sum = sum / newAverages.size();
				finalAverages.add(sum);
			}
			String finalAverage = "";
			for (int i = 0; i < finalAverages.size(); i++) {
				finalAverage += finalAverages.get(i) + " ";
			}

			finalListAvg.add(finalAverage);
			
			
			String keyString = key.toString();
			
			//check for convergence
			String[] keyStringSplit=keyString.split("\\s+");
			String[] finalAverageSplit=finalAverage.split("\\s+");
			
			
			for (int i = 0; i < keyStringSplit.length; i++) {
				Double dKeyStringSplit = Double.parseDouble(keyStringSplit[i]);
				Double dfinalAverageSplit = Double
						.parseDouble(finalAverageSplit[i]);
				errorSum += Math.abs(dKeyStringSplit - dfinalAverageSplit);
				
			}

			//System.out.println("errorSum:"+errorSum);
		if(errorSum>0.0)
				context.getCounter(counter.CONVERGECOUNTER).increment(1);	
		
		}


		// clean up method
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
//			Configuration conf = context.getConfiguration();
//			Path outPath = new Path(conf.get("cPath"));
//			FileSystem fs = FileSystem.get(conf);
//			fs.delete(outPath, true);
//
//			FSDataOutputStream out = fs.create(outPath);
//			for (String str : finalListAvg) {
//				System.out.println("Centroids In Reduce: "+ str);
//
//				out.writeBytes(str);
//				// out.writeUTF(str);
//
//				out.write(System.getProperty("line.separator").getBytes());
//			}
//			out.close(); 
			
			for(int i=0;i<finalListAvg.size();i++)
			{
				context.write(new Text(finalListAvg.get(i)),new Text(""));
				//System.out.println("centroids:"+ finalListAvg.get(i));
			}

		}
	}
	
	 public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		        
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    	
		    	context.write(new Text("1"), value);
		        
			}
	}
		  
		        
		 public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		    public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		        
		    	for(Text value:values)
		    	{
		    		context.write(value, new Text(""));
		    	}
		    			    	    	
		    }
		 }


	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		final String inputHdfsPath = args[0];
		final String tableName = args[1];
		final String outputHdfsPath = args[2];
		final String kValue = args[3];
		final String inputToKCentroids = args[4];
		final String destFile = "/sunilFile/KRandomClusters" + Math.random()
				* 500 + "";
		final String outputHdfsPath1 = "/sunilFile/KRandomClusters1" + Math.random()
				* 500 + "";
		
		Configuration config = HBaseConfiguration.create();

		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor desc = new HTableDescriptor(args[1]);

		config.setStrings("TableName", tableName);
		config.setStrings("cPath", inputToKCentroids);

		config.addResource(new Path(args[5]));

		// add Column Families
		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("Area")));
		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("Property")));

		// Create Table
		admin.createTable(desc);

		// configure for Map Reduce Job
		Job createTableJob = new Job(config, "insertData");

		createTableJob.setInputFormatClass(TextInputFormat.class);
		// createTableJob.setOutputFormatClass(TableOutputFormat.class);

		createTableJob.setJarByClass(Project.class);
		createTableJob.setMapperClass(ReadDataMap.class);
		createTableJob.setOutputKeyClass(ImmutableBytesWritable.class);
		createTableJob.setOutputValueClass(Put.class);
		createTableJob.setNumReduceTasks(0);
        
		

		// Distributed Caching of the file before putting input
		// hdfs://localhost:54310/KRandomClusters
		
		
		//Create table for input to K centroids 
		
		FileInputFormat.addInputPath(createTableJob, new Path(inputHdfsPath));
		FileOutputFormat
				.setOutputPath(createTableJob, new Path(outputHdfsPath1));
		createTableJob.waitForCompletion(true);

		
		long counter=1;
		int i=1;
		String fileName=inputToKCentroids;
		while(counter>0)
		{
		//System.out.println("fileName:"+fileName);
		config.setStrings("centroidsFileName", fileName);
		String value=config.get("centroidsFileName");
		//System.out.println("main method filename:"+value);
		//DistributedCache.addCacheFile(new Path(fileName).toUri(),config);	
		fileName = "/sunilkumarkonduru"+54321 + Math.random() * 500 + "";
		Job kMeansJob = new Job(config, "Kmeans");
		kMeansJob.setJarByClass(Project.class);
		kMeansJob.setMapperClass(MyMapper.class);
		kMeansJob.setOutputKeyClass(Text.class);
		kMeansJob.setOutputValueClass(Text.class);
		kMeansJob.setReducerClass(MyTableReducer.class);
		//kMeansJob.setJarByClass(MyMapper.class);

		Scan scan = new Scan();
		scan.setCaching(1);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(tableName, scan, MyMapper.class,
				Text.class, Text.class, kMeansJob);

//		TableMapReduceUtil.initTableReducerJob(tableName, // output table
//				MyTableReducer.class, // reducer class
//				kMeansJob);
//		kMeansJob.setOutputFormatClass(NullOutputFormat.class);

		//kMeansJob.setReducerClass(MyTableReducer.class);
		kMeansJob.getConfiguration().set(TableInputFormat.INPUT_TABLE,
				tableName);
		
		FileOutputFormat.setOutputPath(kMeansJob, new Path(fileName));
		kMeansJob.waitForCompletion(true);
		counter = kMeansJob.getCounters().findCounter(MyTableReducer.counter.CONVERGECOUNTER).getValue();
		}
		//write fileName to hdfs
	 Job job = new Job(config, "PrintValues");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map1.class);
    job.setReducerClass(Reduce1.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(1);
    job.setJarByClass(Project.class);
    
    FileInputFormat.addInputPath(job, new Path(fileName));
    FileOutputFormat.setOutputPath(job, new Path(outputHdfsPath));
        
    job.waitForCompletion(true);
    

	}
}