package topn;

import org.apache.hadoop.io.Text;

public class User implements Comparable<User> {
	private int _followers;
	private Text _record;
	
	public User( int followers, Text record) {
		_followers = followers;
		_record = record;
	}

	public int getFollowers() { return _followers; }
	public Text getRecord() { return _record;}
	
	@Override 
	public int compareTo(User user) {
		return _followers - user._followers;
	}
}
