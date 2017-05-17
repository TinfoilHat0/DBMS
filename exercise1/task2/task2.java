import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Scanner;


public class task2 {
	public static void main(String[] args) throws Exception {
		
		//1. Get parameters and initialize the NewSketch 
		Scanner scan = new Scanner(System.in);
		int availableSpace = scan.nextInt();
		float pr1 = scan.nextFloat();
		float epsilon = scan.nextFloat();
		float pr2 = scan.nextFloat();
		scan.close();
		NewSketch freqEstimator = new NewSketch(availableSpace, pr1, epsilon, pr2);
		
		//2. Create a file for writing the answers
		FileWriter fileWriter = new FileWriter(new File("out2.txt"));
		
		//3. Open both the query and stream file to read
		FileInputStream fstream = new FileInputStream("task2_queries.txt");
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		FileInputStream fstream2 = new FileInputStream("file1.tsv");
		DataInputStream in2 = new DataInputStream(fstream2);
		BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
		
		//4.Get the query details and insert&process the stream accordingly
		String str;
		int ctr = 0;
		int lines = 0;
		while ((str = br.readLine()) != null){
			String[] parsed = str.split("\t");
			int queryTime = Integer.parseInt(parsed[0]);
			long queryIP = task3.ipToLong(parsed[1]);
			//5. Inserting values from file1 up to the queryTime
			while(ctr < queryTime){
				String str2 = br2.readLine();
				long ip = task3.ipToLong(str2.split("\t")[0]);
				freqEstimator.addArrival(ip);
				ctr += 1;
			}
			int est = freqEstimator.getFreqEstimation(queryIP);
			if (lines > 0)
				fileWriter.write(",");
			fileWriter.write(Integer.toString(est));
			lines += 1;
		}
		//6. Close files
		in.close();
		in2.close();
		fileWriter.close();
	}

}

