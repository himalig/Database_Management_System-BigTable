package BigT;


import global.MID;

public class Version {

	public int storageType;
	public MID mid;
	public int timestamp;
	
	public Version(int storageType, MID mid, int timestamp) {
		super();
		this.storageType = storageType;
		this.mid = mid;
		this.timestamp=timestamp;
	}
}
