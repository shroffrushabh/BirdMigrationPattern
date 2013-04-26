package com.project;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {
	
	static int NUM_SEEDS = 10;
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Path[] localFiles;
		
		// Creating the speciesAndCenters which stores the species,month as key and a list of centers
		// as an arraylist
		HashMap<String,ArrayList<PointP>> speciesAndCenters;
		
		// Stores the name of the of all the species with the position of the species in 1stRow as the 
		// key and the name of the species as the value
		HashMap<Integer,String> speciesHash;
				
		public void setup(Context context) throws IOException {
			try {

			    Configuration conf = context.getConfiguration();
			    
		        // Create a FileSystem object pointing to my bucket on S3
			    FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/usr/rushabh"),context.getConfiguration());
			    
			    BufferedReader br;
			    String line;
			    
			    // Read the seeds file from seeds file from S3  
				FSDataInputStream cache = fs.open(new Path("hdfs://localhost:9000/usr/rushabh/seeds.v2"));
					    
				Vector<String> v=new Vector();
				FSDataInputStream src;
		
				try {
					while((line = cache.readLine()) != null ){
						v.add(line.trim());
				    }
				 } catch (IOException e) {
					        // TODO Auto-generated catch block
					 e.printStackTrace();
				 }
		
				 String str[]= v.toArray(new String[0]);
				 speciesAndCenters = new HashMap();
					    
				 for(int i=0;i<str.length;i++){
				   	String [] temp = str[i].split(" ");
				   	String speciesAndMonth = temp[0].trim() + "," + Integer.parseInt(temp[1]);
				   	PointP newCenter = new PointP();
				   	newCenter.x=Float.parseFloat(temp[2]);
				  	newCenter.y=Float.parseFloat(temp[3]);
				   	newCenter.ClusterNum=Integer.parseInt(temp[4]);
				   	//newCenter.clusterized=Integer.parseInt(temp[5].trim());
				   	if(speciesAndCenters.get(speciesAndMonth) == null){
				   		ArrayList centers = new ArrayList();
				  		centers.add(newCenter);
				  		
				  		// Adding the (species,month) as the key to the hashmap and a new arraylist 
				  		// having one center as the value
						speciesAndCenters.put(speciesAndMonth, centers);
					}
				   	// if (species,month) is present in the hashmap then just append the point object
				   	// to the arraylist 
					else speciesAndCenters.get(speciesAndMonth).add(newCenter);
				}
				cache = fs.open(new Path("hdfs://localhost:9000/usr/rushabh/1stRow"));
			    speciesHash = new HashMap();
				while((line = cache.readLine()) != null ){
					String [] temp = line.split(",");
					for(int i=0;i<temp.length;i++)
						// the key is the position of the position of the species in the array 
						// the value is the name of the species
						speciesHash.put(i,temp[i]);
				 }

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context)
						throws IOException, InterruptedException {
			String[] record = value.toString().split(",");

			String LATITUDE=record[1];
			String LONGITUDE=record[2];
			String YEAR=record[3];
			String MONTH=record[4];
			String DAY=record[5];
			String TIME=record[6];
			String COUNTY=record[7];
			String STATE_PROVINCE=record[8];
			
			PointP v = new PointP();
			v.x=Float.parseFloat(LATITUDE);
			v.y=Float.parseFloat(LONGITUDE);
			
			double nearestDistance = Double.MAX_VALUE;
	

			for(int i=0;i<speciesHash.size();i++){
				String particularSpecies = speciesHash.get(i);
				
				// This check is performed to see is the number of sightings is greater than 0.
				// There are possible missing values in the dataset(X,?) which are not considered.
				// The Buteo_lineatus is for checking if the species is the Red Shouldered Hawk
				if( !record[i+16].equals("0") && !record[i+16].equals("X") && !record[i+16].equals("?") 
						&& particularSpecies.equalsIgnoreCase("Buteo_lineatus")){ 
					ArrayList<PointP> speciesCenters = speciesAndCenters.get(/*particularSpecies.trim()+*/ 
											particularSpecies.trim()+"," + Integer.parseInt(MONTH));
					
					// Finding the center to which this record is closest to from the speciesAndCenters hash
					if(speciesCenters != null){
						PointP nearest = null;
						for (PointP c:speciesCenters) {
						 double dist = distanceMeasure(c,v);
						 if (nearest == null) {
						  	nearest = c;
						 	nearestDistance = dist;
						 } else {
						 if (nearestDistance > dist) {
						 		nearest = c;
						 		nearestDistance = dist;
						 		}
						 	}
						 }
						
						// emitting the name of the name of the species, cluster's x, cluster's y, 
						// Month, cluster's number and the number of sightings and the record 
						// itself as the value
						context.write(new Text(particularSpecies + ","
										+ nearest.x + "," + nearest.y + ","+ Integer.parseInt(MONTH) + "," + nearest.ClusterNum
																), new Text(record[i+16] + "," + value.toString()));
					}
			}		
		}
	}
		// Euclidean distance calculation
		public static double distanceMeasure(PointP a,PointP b){	
			return Math.sqrt(Math.pow((a.x - b.x),2) + Math.pow((a.y - b.y),2));
		}
	}
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
			private Text result = new Text();
			
			public void reduce(Text key, Iterable<Text> values, Context context)
						throws IOException, InterruptedException {
				String [] keyValues = key.toString().split(",");

				float sumLat=Float.parseFloat(keyValues[1]);
				float sumLon=Float.parseFloat(keyValues[2]);
				int len=1;
				
				// computing the mean of list of all the sightings
				for (Text val : values) {
					String [] record = val.toString().split(",");
					int iter = Integer.parseInt(record[0]);
					String lat = record[2];
					String lon = record[3];
					sumLat += Float.parseFloat(lat)*iter;
					sumLon += Float.parseFloat(lon)*iter;
					len+=iter;
				}
				// emitting the species name, month as the key and the mean lat, mean lon, cluster number and the size
				// of the cluster as the value
				context.write(new Text(keyValues[0]+" "+keyValues[3]), 
						new Text((float)sumLat/len+" "+(float)sumLon/len+" "+keyValues[4]+ " " + len ));
				
			}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
		System.out.println("Started!!!");
		Job job = new Job(conf, "Migration Pattern");
		job.setJarByClass(KMeans.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/usr/rushabh/sample"));
		Boolean repeat = false;
		int iter = 1;
		String iteration="";
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/usr/rushabh/oop0"));
		
		while (job.waitForCompletion(true)) {
			System.out.println("Iteration:"+ iter);
			repeat = false;
	    	FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/usr/rushabh"),conf);

			HashMap result = null;

			try {
					iteration=iter+"";
					
					// Reading all the output files
					FileStatus[] status = fs.listStatus(new Path("hdfs://localhost:9000/usr/rushabh/oop0"));
					String newline=null;
					result = new HashMap();
					repeat = false;
			        
					for(int i=0;i<status.length;i++){
						FSDataInputStream inOfop = fs.open(status[i].getPath());						
						while((newline = inOfop.readLine()) != null ){
				        	String [] temp = newline.split("\\s+");
				        	String species = temp[0];
				        	String month = temp[1];
				        	String clusterNum = temp[4];
				        	String lat = temp[2];
				        	String lon = temp[3];
				        	String size = temp[5];
				        	// storing the output of the reducer, with the species name and month as the key
				        	// and the lat, lon and cluster size as the value
				        	result.put(species+","+month+","+clusterNum,lat+","+lon+","+size);	
				        }
					}
					
					// Reading the previous seed file and checking with the hashmap that we created above 
					// to figure if the seed points have converged or not
			        FSDataInputStream inOfCache = fs.open(new Path("hdfs://localhost:9000/usr/rushabh/seeds.v2"));
			        while((newline = inOfCache.readLine()) != null ){
			        	String [] temp = newline.split("\\s+");
			        	String species = temp[0];
			        	String month = temp[1];
			        	String clusterNum = temp[4];
			        	String lat = temp[2];
			        	String lon = temp[3];
 			        	if(result.get(species+","+month+","+clusterNum) != null){
	 			        	String latlonOfResult = (String) result.get(species+","+month+","+clusterNum);
	 			        	String [] latlon = latlonOfResult.split(",");
	 			        	if(Math.abs(Float.parseFloat(latlon[0])*10000-Float.parseFloat(lat)*10000) < 10 && 
	 			        			Math.abs(Float.parseFloat(latlon[1])*10000-Float.parseFloat(lon)*10000) < 10){
	 			        			System.out.println("Something has converged...");
	 			        	}
	 			        	else {
	 			        		// if they havent converged we break the task and repeat the MR job
	 			        		System.out.println("Lat diff:"+Math.abs(Float.parseFloat(latlon[0])*10000-Float.parseFloat(lat)*10000));
	 			        		System.out.println("Lon diff:"+Math.abs(Float.parseFloat(latlon[1])*10000-Float.parseFloat(lon)*10000));

	 			        		repeat=true;
	 			        		break;
	 			        	}
 			        	}
			        }
			        
			        if(repeat){
					    FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000/usr/rushabh"),conf);
						iter++;
					    iteration=iter+"";
						  
					    Path path = new Path("hdfs://localhost:9000/usr/rushabh/seeds.v2");
						
					    FSDataOutputStream out = fs.create(path);
					    
					    Iterator it = result.entrySet().iterator();

					    // we use the output hashmap we have created before and overwrite the seeds file
					    // the new values from the hashmap
					    while (it.hasNext()) {
					        Map.Entry pairs = (Map.Entry)it.next();
					        String [] temp1 = ((String)pairs.getKey()).split(",");
					        String [] temp2 = ((String)pairs.getValue()).split(",");
					        String species = temp1[0];
				        	String month = temp1[1];
				        	String clusterNum = temp1[2];
				        	String lat = temp2[0];
				        	String lon = temp2[1];
				        	String size = temp2[2];
				        	
				        	out.write((species + " " + month  + " " + lat + " "
				        					+ lon + " " + clusterNum + " "+ size + "\n").getBytes());
					        
					        it.remove(); 
					    }

					    job = restart(iteration);
					    fileSystem.delete(new Path("hdfs://localhost:9000/usr/rushabh/oop0"), true);
					    fileSystem.close();

					    out.close();
					}
					else {
						System.out.println("Converged!!!");
						break;
					}
				
		    } catch (IOException e) {
		        // TODO Auto-generated catch block
		        e.printStackTrace();
		    }

		}
	}	
		
	public static Job restart(String iteration) throws URISyntaxException, IOException{
		Configuration conf = new Configuration();		

		Job job = new Job(conf, "Migration Pattern");
		job.setJarByClass(KMeans2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/usr/rushabh/sample"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/usr/rushabh/oop0"));
		return job;
	}
}
