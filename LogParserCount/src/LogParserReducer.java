import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogParserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final int MIN_ACCESS = 10;

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable val : values) {
			sum += val.get();
		}

		if (sum > MIN_ACCESS) {
			IntWritable result = new IntWritable(sum);
			context.write(key, result);
		}

	}

}