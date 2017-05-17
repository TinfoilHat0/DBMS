
class RangeBf {
	int maxDepth = 32; 
	long maxRange = (long) Math.pow(2, maxDepth);
	int nExpected = 4* (int) Math.pow(10, 5);
	BloomFilter[] bfList = new BloomFilter[maxDepth + 1];
	
	public RangeBf(double pr) {
		double adjustedPr = 1 - Math.pow(1-pr, 1/64.0);
		for(int i=maxDepth; i>-1; i--){
			int width;
			if (Math.pow(2, i) > nExpected)
				width = nExpected;
			else
				width = (int) Math.pow(2, i);
			bfList[i] = new BloomFilter((float)adjustedPr, width);
		}	
	}
	
	public void insertValue(long key) {
		//Start from leaves, proceed to the root
		for (int i=maxDepth; i>-1; i--){
			bfList[i].add(key);
			key = (long) Math.ceil(key/2.0);
		}
	}
	
	public boolean existsInRange(long l, long r) {
		//Partition to dyadic intervals and query the corresponding bloom filter
		long lower = l;
		long upper = lower;
		while(upper < r){
			long x = lower-1;
			long len = 1;
			while(x>0 && x%2==0){
				len *= 2;
				x /= 2;
			}
			while (lower+len-1 > r)
				len /= 2.0;
			upper = lower+len-1;
			long intervalLength =  upper - lower + 1;
			int bfIndex = maxDepth - (int) (Math.log(intervalLength)/Math.log(2));
			long queryIndex = upper/intervalLength; //query by index at that depth 
			if (bfList[bfIndex].doesContain(queryIndex))
				return true;
			lower = upper + 1;
		}
		return false;
	}
	
	
					
}
