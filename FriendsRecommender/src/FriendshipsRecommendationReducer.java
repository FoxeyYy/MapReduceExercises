import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendshipsRecommendationReducer extends Reducer<IntWritable, Friend, IntWritable, Text> {
	
	private static final int MAX_SUGGESTIONS = 10;

	@Override
	public void reduce(IntWritable key, Iterable<Friend> values, Context context)
			throws IOException, InterruptedException {

		PriorityQueue<Friend> maxHeap = new PriorityQueue<>(MAX_SUGGESTIONS, new Friend.FriendComparator());

		for (Friend val: values) {
			maxHeap.add(new Friend(val));
		}

		int count = 0;
		StringBuilder outputBuilder = new StringBuilder();
		
		while(count < MAX_SUGGESTIONS) {
			if (maxHeap.isEmpty()) {
				break;
			}
			
			Friend friend = maxHeap.poll();
			outputBuilder.append(friend.getId() +  ", ");
			count++;
		}
		
		outputBuilder.setLength(outputBuilder.length() - 2);
		
		context.write(key, new Text(outputBuilder.toString()));

	}

}