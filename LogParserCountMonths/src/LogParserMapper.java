import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogParserMapper extends Mapper<Text, LogRecord, Text, IntWritable> {
	
	@Override
	public void map(Text key, LogRecord value, Context context)
			throws IOException, InterruptedException {
		
		String[] date = value.getDate().split("/");
		
		Text month = new Text(date[1]);
		int year = Integer.valueOf(date[2]);

		if (year == 2010) {
			context.write(month, new IntWritable(1));
		}

	}

}
