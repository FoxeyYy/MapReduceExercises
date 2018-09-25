import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BakeryCustomersTotalBillTotalBillPerReceiptMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer linesItr = new StringTokenizer(value.toString(), "\n");

		if (key.get() == 0 && linesItr.hasMoreTokens()) {
			linesItr.nextToken();	// Skip csv header
		}
		
		while (linesItr.hasMoreTokens()) {
			String[] line = linesItr.nextToken().split("\t");
			context.write(new IntWritable(Integer.valueOf(line[0])), new Text("B," + line[1]));
		}
		

	}

}
