import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogParserMapper extends Mapper<LongWritable, Text, Text, LogRecord> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer linesItr = new StringTokenizer(value.toString(), "\n");
		while (linesItr.hasMoreTokens()) {		

			String line = linesItr.nextToken();
			
			String ip = line.substring(0, line.indexOf(" "));
			
			int indexOfFirstBracket = line.indexOf('[');
			String dateAndTime = line.substring(indexOfFirstBracket + 1, 
					line.indexOf(']', indexOfFirstBracket));//line[3 + offset];
			
			int timeSeparatorIndex = dateAndTime.indexOf(':');
			String date = dateAndTime.subSequence(0, timeSeparatorIndex).toString();
			String time = dateAndTime.subSequence(timeSeparatorIndex + 1, dateAndTime.length())
					.toString()
					.split("\\s")[0];

			int begginingIndexOfQuotes = line.indexOf('\"', indexOfFirstBracket);
			int begginingIndexOfURL = line.indexOf('/', begginingIndexOfQuotes);
			int lastIndexOfQuotes = line.lastIndexOf('\"');
			int lastIndexOfURL = line.lastIndexOf(' ', lastIndexOfQuotes);

			String url = line.substring(begginingIndexOfURL, lastIndexOfURL);

			short statusCode = Short.valueOf(line.substring(lastIndexOfQuotes + 2, line.lastIndexOf(' ')));
			
			incrementStatusCounters(context, statusCode);
			
			if (statusCode / 100 == 2) {
				LogRecord record = new LogRecord(ip, date, time, statusCode);
				context.write(new Text(url), record);
			}
			
		}

	}

	private void incrementStatusCounters(Context context, short statusCode) {
		switch(statusCode / 100) {
		case 2:
			context.getCounter(Counters.GROUP, Counters.OK).increment(1);
			break;
		case 3: 
			context.getCounter(Counters.GROUP, Counters.REDIRECT).increment(1);
			break;
		case 4:
			context.getCounter(Counters.GROUP, Counters.ERROR).increment(1);
			break;
		default:
			break;
		}
	}
}
