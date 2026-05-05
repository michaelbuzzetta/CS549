package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TextPair ensures that we have values with tag "0" first, followed by tag "1"
		 * So we know that first value is the name and second value is the rank
		 */
		String k = key.toString(); // Converts the key to a String
		
		// TODO values should have the vertex name and the page rank (in that order).
		// Emit (vertex name, pagerank) or (vertex id, vertex name, pagerank)
		// Ignore if the values do not include both vertex name and page rank

		String vertexName = null;
		String pageRank = null;

		int i = 0;
		for (Text val : values) {
			if (i == 0) {
				vertexName = val.toString();
			} else if (i == 1) {
				pageRank = val.toString();
				break;
			}
			i++;
		}
		if (vertexName != null && pageRank != null) {
			context.write(new Text(vertexName), new Text(pageRank));
		}

	}
}
