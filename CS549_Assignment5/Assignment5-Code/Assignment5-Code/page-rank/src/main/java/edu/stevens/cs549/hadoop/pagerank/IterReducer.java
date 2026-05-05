package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		double d = PageRankDriver.DECAY; // Decay factor
		double rank = 0.0; // stores the decay factor in a variable rank
		String adjList = "";

		for (Text value : values) {
			String valStr = value.toString();
			if (valStr.startsWith("@")) {
				adjList = valStr.substring(1); // remove '@'
			} else {
				rank += Double.parseDouble(valStr);
			}
		}

		rank = d * rank + (1 - d);

		context.write(
				new Text(key.toString() + " [" + rank + "]"),
				new Text(adjList)
		);
	}
}
