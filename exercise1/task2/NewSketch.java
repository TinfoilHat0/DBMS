


public class NewSketch {
	private int bloomSize;
	private int hashSize;
	private int cmColumnSize;
	private int cmRowSize;
	private BloomFilter bf;
	private CountMin cm;
	private int nExpected =  (int) (4*Math.pow(10, 5));

	public NewSketch(int availableSpace, float pr1, float epsilon,
			float pr2) throws InsufficentMemoryException {
		this.bloomSize = (int) ((-nExpected*Math.log(pr1))/Math.pow(Math.log(2.0), 2));
		this.cmColumnSize = (int) Math.ceil(Math.E/epsilon);
		this.cmRowSize = (int) Math.ceil(Math.log((double) 1/(1-pr2)));
		long reqMemory = cmRowSize*cmColumnSize*32 + bloomSize; //required memory in bits
		if (reqMemory > availableSpace*8)
			throw new InsufficentMemoryException();
		this.bf = new BloomFilter(pr1, nExpected);
		this.cm = new CountMin(1-pr2, epsilon, nExpected);
	}
	
	public void addArrival(long key) {
		bf.add(key);
		cm.add(key);
	}

	public int getFreqEstimation(long key) {
		//First check with BF to see if key has seen, if yes get frequency with CM
		if (!bf.doesContain(key))
			return 0;
		return cm.getFrequency(key);
	}
	
}


