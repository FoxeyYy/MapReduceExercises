import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BakeryCustomersTotalBillPricesMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer linesItr = new StringTokenizer(value.toString(), "\n");
		
		while (linesItr.hasMoreTokens()) {
			String[] line = linesItr.nextToken().split("[\t\\s]");
			context.write(new IntWritable(Integer.valueOf(line[0])), new Text("A," + line[1]));
		}
		

	}

}
