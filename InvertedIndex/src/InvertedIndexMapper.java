import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text fileName;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		String file = ((FileSplit) context.getInputSplit()).getPath().getName();
		fileName = new Text(file);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			
			String token = itr.nextToken()
					.replaceAll("^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "")
					.toLowerCase();
			
			if (token.isEmpty()) {
				continue;
			}
			
			Text word = new Text(token);
			context.write(word, fileName);
			
		}

	}
}
