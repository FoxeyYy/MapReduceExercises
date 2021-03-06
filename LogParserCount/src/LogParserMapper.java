import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogParserMapper extends Mapper<Text, LogRecord, Text, IntWritable> {
	
	@Override
	public void map(Text key, LogRecord value, Context context)
			throws IOException, InterruptedException {

		context.write(key, new IntWritable(1));

	}

}
