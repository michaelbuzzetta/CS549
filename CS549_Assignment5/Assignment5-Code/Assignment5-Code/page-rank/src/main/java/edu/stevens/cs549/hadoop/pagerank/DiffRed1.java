package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/* 
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */

		int i = 0;
		for (Text val : values) {
			if (i >= 2) break;
			ranks[i] = Double.parseDouble(val.toString());
			i++;
		}

		if (i != 2) return;


		double diff = Math.abs(ranks[0] - ranks[1]);

		context.write(new Text(String.valueOf(diff)), new Text(""));

	}
}
