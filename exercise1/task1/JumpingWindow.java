import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;



public class JumpingWindow {
	private short nDistinctKey = 256;
	private int subWindowSize;
	private int windowSize;
	private int[] subWindowCtr = new int[nDistinctKey];
	Queue<int[]> q = new LinkedList<int[]>();
	private int qLimit;
	public int streamCtr = 0;

	public JumpingWindow(int wSize, float epsilon){
		this.windowSize = wSize;
		this.subWindowSize = (int) Math.ceil(2*wSize*epsilon);
		this.qLimit = (int) Math.ceil((double) windowSize/subWindowSize);
		this.subWindowCtr = new int[nDistinctKey];
	}

	public void insertEvent(int srcIP){
		subWindowCtr[srcIP] += 1;
		streamCtr += 1;
		//Once a subwindow is filled, reset the counter and move to the next one
		if (streamCtr == subWindowSize){
			streamCtr = 0;
			//If q is full, remove first
			if (q.size() == qLimit)
				q.remove(); //Garbage collector handles that
			q.add(Arrays.copyOf(subWindowCtr, nDistinctKey));
			Arrays.fill(subWindowCtr, 0);
		}		
	}

	public int getFreqEstimation(int srcIP, int w1Size){
		if (w1Size == 0 || w1Size > windowSize)
			w1Size = windowSize;
		int freqEst = 0;
		//Add fully covered subwindows
		if(q.size() > 0){
			int ctr = 0;
			int nFullyCovered = (int) Math.floor((double) w1Size/subWindowSize);
			for(int[] window : q){
				if (ctr < nFullyCovered){
					ctr += 1;
					freqEst += window[srcIP];
					//System.out.println(window[srcIP]);
				}
				else if (ctr == nFullyCovered){
					//If there's a partially covered subwindow, add half of its value
					if ((int) Math.ceil((double) w1Size/subWindowSize) > nFullyCovered)
						freqEst += window[srcIP]/2;
					break;
				}
			}
		}
		else //the case which the first subwindow is partially covered and q is empty.
			//In this case, we have to read from counter to achieve the desired probability error
			freqEst += subWindowCtr[srcIP]/2;

		return freqEst;
	}

	public int getFreqEstimation(int srcIP){
		return getFreqEstimation(srcIP, windowSize);
	}


}





