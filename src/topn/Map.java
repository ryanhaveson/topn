package topn;
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, NullWritable, Text>{

	private PriorityQueue<User> userQueue = new PriorityQueue<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
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
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(User u : userQueue) {
			context.write(NullWritable.get(), u.getRecord());
		}
	}
}
