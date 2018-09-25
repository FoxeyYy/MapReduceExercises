import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BakeryProductsTotalReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		String metadata = "";

		for (Text line : values) {
			String[] elements = line.toString().split(",");
			if (elements[0].equals("A")) {
				metadata = elements[1] + elements[2];
			} else {
				count += Integer.valueOf(elements[1]);
			}
		}

		String output = metadata + "," + count;
		context.write(key, new Text(output));
		
	}

}