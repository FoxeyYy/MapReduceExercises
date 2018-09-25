import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BakeryCustomersTotalBillReceiptFinalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String name = "";
		float totalBill = 0f;

		for (Text line : values) {
			String[] elements = line.toString().split(",");
			if (elements[0].equals("A")) {
				name = elements[1] + "," + elements[2];
			} else {
				totalBill += Float.valueOf(elements[1]);
			}
		}

		String output = name + "," + totalBill;
		context.write(key, new Text(output));
		
	}

}