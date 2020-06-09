package manager;

import java.util.Date;

/**
 * 
 * @author Fabian Lehmann
 *
 */
public class Pair {
	
	private String commit;
	private Date date;
	public Pair(String commit, Date date) {
		this.commit = commit;
		this.date = date;
	}
	public String getCommit() {
		return commit;
	}
	public Date getDate() {
		return date;
	}
	@Override
	public String toString() {
		return commit + " " + date;
	}

}
