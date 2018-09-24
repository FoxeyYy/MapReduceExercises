import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstCharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		final IntWritable one = new IntWritable(1);
		Text word = new Text();

		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			
			String token = itr.nextToken()
					.replaceAll("^[^a-zA-Z]+|[^a-zA-Z]+$", "")
					.toLowerCase();
			
			if (token.isEmpty()) {
				continue;
			}
			
			word.set(String.valueOf(token.charAt(0)));
			context.write(word, one);
			
		}

	}
}
