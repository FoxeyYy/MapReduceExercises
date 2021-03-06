import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendshipsCounterReducer extends Reducer<FriendsKey, IntWritable, FriendsKey, IntWritable> {

	@Override
	public void reduce(FriendsKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;

		for (IntWritable val: values) {
			if (val.get() == 0) {
				return;
			} else {
				count++;
			}
		}

		context.write(key, new IntWritable(count));
		
	}

}