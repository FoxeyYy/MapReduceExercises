import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BakeryCustomersTotalBillPricesMapper extends Mapper<Text, Text, IntWritable, Text> {
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
				
		context.write(new IntWritable(Integer.valueOf(key.toString())), new Text("A," + value));

	}

}
