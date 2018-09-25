import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BakeryClientsMultiShoppingReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> datesCount = new HashMap<>();
		String metadata = "";

		for (Text line : values) {
			String[] elements = line.toString().split(",");
			if (elements[0].equals("A")) {
				metadata = elements[1] + "," + elements[2];
			} else {
				Integer count = datesCount.get(elements[1]);
				if (count == null) {
					count = 0;
				}
				
				datesCount.put(elements[1], count + 1);
			}
		}

		StringBuilder datesListBuilder = new StringBuilder();
		for (String date: datesCount.keySet()) {
			if (datesCount.get(date) > 1) {
				datesListBuilder.append(date);
			}
		}
		
		String datesList = datesListBuilder.toString();
		
		if (!datesList.isEmpty()) {
			context.write(key, new Text(metadata + "," + datesList));
		}
		
	}

}