import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BakeryCustomersTotalBillReceiptBillReducer extends Reducer<IntWritable, Text, IntWritable, FloatWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int customerId = 0;
		float bill = 0.0f;

		for (Text line : values) {
			String[] elements = line.toString().split(",");
			if (elements[0].equals("A")) {
				bill += Float.valueOf(elements[1]);
			} else {
				customerId = Integer.valueOf(elements[1]);
			}
		}

		context.write(new IntWritable(customerId), new FloatWritable(bill));
		
	}

}