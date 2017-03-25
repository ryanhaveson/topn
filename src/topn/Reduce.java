package topn;
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	private PriorityQueue<User> userQueue = new PriorityQueue<>();
	
	@Override
	public void reduce(final NullWritable key,
						final Iterable<Text> values, 
						final Context context )
			throws IOException, InterruptedException{

		for(Text value : values ) {
			
			String line = value.toString();
			String[] data = line.split("\t");

			Integer followers = Integer.parseInt(data[1]);
			User user = userQueue.peek();
			if(userQueue.size() < 3 || followers > user.getFollowers()) {
				userQueue.add(new User(followers, new Text(value)));
				
				if (userQueue.size() > 3) {
					userQueue.poll();
				}
			}

			
		}
		
		for(User u : userQueue) {
			context.write(NullWritable.get(), u.getRecord());
		}
	}

}



