import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogParserMapper extends Mapper<Text, LogRecord, Text, IntWritable> {
	
	@Override
	public void map(Text key, LogRecord value, Context context)
			throws IOException, InterruptedException {
		
		String[] date = value.getDate().split("/");
		
		int year = Integer.valueOf(date[2]);
		
		String[] time = value.getTime().split(":");
		Text hour = new Text(time[0]);

		if (year == 2010) {
			context.write(hour, new IntWritable(1));
		}

	}

}
