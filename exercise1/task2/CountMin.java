import java.util.concurrent.ThreadLocalRandom;


public class CountMin {
	private int nExpected;
	private int columnSize;
	private int rowSize;
	private int[][] matrix;
	private int[] rnd1;
	private int[] rnd2;
	
	public CountMin(float errorProb, float errorEpsilon, int nExpected){
		this.nExpected = nExpected;
		this.columnSize = (int) Math.ceil(Math.E/errorEpsilon);
		this.rowSize = (int) Math.ceil(Math.log((double) 1/(1-errorProb)));
		this.matrix = new int[rowSize][columnSize];
		this.rnd1 = new int[rowSize];
		this.rnd2 = new int[rowSize];
		for (int i = 0; i < rowSize; i++) {
			this.rnd1[i] = ThreadLocalRandom.current().nextInt(1,
					Integer.MAX_VALUE);
			this.rnd2[i] = ThreadLocalRandom.current().nextInt(1,
					Integer.MAX_VALUE);
		}
	}
	
	public void add(long key){
		for(int i=0; i<rowSize; i++)
			matrix[i][hash(i, key)] += 1;
	}
	public int getFrequency(long key){
		int min = Integer.MAX_VALUE;
		for(int i=0; i<rowSize;i++){
			if (matrix[i][hash(i, key)] < min)
				min = matrix[i][hash(i, key)];
		}
		return min;
		
	}
	private int hash(int i, long key) {
		return (int) (Math.abs(((long)(key*rnd1[i] + rnd2[i]) % Integer.MAX_VALUE) % columnSize));
	}

}
