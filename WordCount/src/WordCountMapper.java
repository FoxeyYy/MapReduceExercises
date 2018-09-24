import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		final IntWritable one = new IntWritable(1);
		Text word = new Text();

		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken()
					.replaceAll("^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "")
					.toLowerCase();
			if (token.isEmpty()) {
				continue;
			}
			
			word.set(token);
			context.write(word, one);
			
		}

	}
}
