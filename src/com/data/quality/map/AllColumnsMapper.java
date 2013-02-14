package com.data.quality.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AllColumnsMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private final static int maxColumns = 12;

	public Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.trim();
		StringTokenizer tokenizer = new StringTokenizer(line, "\n");
		int numberofRecords = 0;
		
		while (tokenizer.hasMoreTokens()) {
			numberofRecords = numberofRecords +1;
			
			String token = tokenizer.nextToken();
			int currentIndex = 1;
			int columnCount = 1;
			int foundAtIndex = 0;
			for (; currentIndex < token.length();) {				
				foundAtIndex = token.indexOf(";", currentIndex);			
				if (foundAtIndex - currentIndex == 0
						|| token.substring(currentIndex, foundAtIndex).trim()
								.equals("")) {
				
					currentIndex = foundAtIndex + 1;
					String columnNumber = "column"
							+ String.valueOf(columnCount) + "nf";					
					columnCount++;
					context.write(new Text(columnNumber), one);				
					if (columnCount == maxColumns) {
						if (token.endsWith(";")) {
							
							currentIndex = token.length() + 1;
							 columnNumber = "column"
									+ String.valueOf(columnCount) + "nf";
							
							context.write(new Text(columnNumber), one);
							
						} else {						
								currentIndex = token.length() + 1;
								 columnNumber = "column"
										+ String.valueOf(columnCount) + "fd";							
								context.write(new Text(columnNumber), one);												
						}
					}
				} else {
					currentIndex = foundAtIndex + 1;
					String columnValueFound = "column"
							+ String.valueOf(columnCount) + "fd";
					columnCount++;
					context.write(new Text(columnValueFound), one);
					if (columnCount == maxColumns) {
						if (token.endsWith(";")) {
							currentIndex = token.length() + 1;
							String columnNumber = "column"
									+ String.valueOf(columnCount) + "nf";
							context.write(new Text(columnNumber), one);
							

						} else {						
								currentIndex = token.length() + 1;
								String columnNumber = "column"
										+ String.valueOf(columnCount) + "fd";								
								context.write(new Text(columnNumber), one);													
						}
					}
				}
			}
		}
		String totalRecords = "recordCount";
		context.write(new Text(totalRecords), one);
	}
}