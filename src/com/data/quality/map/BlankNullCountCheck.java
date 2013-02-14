package com.data.quality.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlankNullCountCheck extends Configured implements Tool {
	
	private static final String inputPath = "/home/maverick/hadoopinput/cdi_1M.txt";
	//private static final String inputPath2 = "/home/maverick/hadoopinput/test.jpdafd";
	
	private static final String outputPath ="/home/maverick/agilencr/output";
	private static final String tempPath = "/home/maverick/agilencr/temp";
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        Job job1 = new Job(conf);
        job1.setJarByClass(BlankNullCountCheck.class);       
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setMapperClass(AllColumnsMapper.class);
        
        job1.setReducerClass(AllColumnsReducer.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
       
        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        boolean succ = job1.waitForCompletion(true);
        if (! succ) {
          System.out.println("Job1 failed, exiting");
          return -1;
        }
        return 0;
        
    }

    public static void main(String[] args) throws Exception {
    	String path [] = new String [3];
    	path[0] = inputPath;
    	path[1] = outputPath;
    	path[2] = tempPath;
    	
        int res = ToolRunner.run(new BlankNullCountCheck(), path);
        
        System.exit(res);
    }
}