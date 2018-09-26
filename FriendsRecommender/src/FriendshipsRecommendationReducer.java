import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendshipsRecommendationReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private static final int MAX_SUGGESTIONS = 10;

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		PriorityQueue<Friend> maxHeap = new PriorityQueue<>(MAX_SUGGESTIONS, new Friend.FriendComparator());

		for (Text val: values) {
			String[] parts = val.toString().split(",");
			Friend friend = new Friend(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
			
			maxHeap.add(friend);
		}

		int count = 0;
		StringBuilder outputBuilder = new StringBuilder();
		
		while(count < MAX_SUGGESTIONS) {
			if (maxHeap.isEmpty()) {
				break;
			}
			
			Friend friend = maxHeap.poll();
			outputBuilder.append(friend.getId() + ":" + friend.getFriendshipsCount() +  ", ");
			count++;
		}
		
		outputBuilder.setLength(outputBuilder.length() - 2);
		
		context.write(key, new Text(outputBuilder.toString()));

	}

}