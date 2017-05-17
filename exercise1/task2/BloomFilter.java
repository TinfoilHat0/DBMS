import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;

public class BloomFilter {
	private int arraySize;
	private int hashSize;
	private int nExpected;
	private BitSet array;
	private int[] rnd1;
	private int[] rnd2;

	public BloomFilter(float falsePos, int nExpected) {
		this.nExpected = nExpected;
		this.arraySize = (int) Math.ceil(((-nExpected * Math.log(falsePos)) / Math.pow(Math.log(2.0), 2)));
		this.hashSize = (int) Math.ceil(Math.log(2.0) * ((double) arraySize / nExpected));
		this.array = new BitSet(arraySize);
		this.rnd1 = new int[hashSize];
		this.rnd2 = new int[hashSize];
		for (int i = 0; i < hashSize; i++) {
			this.rnd1[i] = ThreadLocalRandom.current().nextInt(1,
					Integer.MAX_VALUE);
			this.rnd2[i] = ThreadLocalRandom.current().nextInt(1,
					Integer.MAX_VALUE);
		}
	}
	
	public void add(long key) {
		for (int i = 0; i < hashSize; i++) 
			this.array.set(hash(i, key));
	}
	
	public boolean doesContain(long key) {
		for (int i = 0; i < hashSize; i++) {
			if (!this.array.get(hash(i, key)))
				return false;
		}
		return true;
	}
	
	private int hash(int i, long key) {
		return (int) (Math.abs(((long)(key*rnd1[i] + rnd2[i]) % Integer.MAX_VALUE) % arraySize));
	}
}
