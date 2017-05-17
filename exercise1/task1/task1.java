import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Scanner;


public class task1 {
	public static void main(String args[]) throws IOException
	{
		
		
		//1. Get parameters and initialize the JumpingWindow 
		Scanner scan = new Scanner(System.in);
		int w = scan.nextInt();
		float epsilon = scan.nextFloat();
		scan.close();
		JumpingWindow jw = new JumpingWindow(w, epsilon);

		//2. Create a file for writing the answers
		FileWriter fileWriter = new FileWriter(new File("out1.txt"));

		//3. Open both the query and stream file to read
		FileInputStream fstream = new FileInputStream("task1_queries.txt");
		FileInputStream fstream2 = new FileInputStream("file1.tsv");
		DataInputStream in = new DataInputStream(fstream);
		DataInputStream in2 = new DataInputStream(fstream2);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));

		//4.Get the query details and insert&process the stream accordingly
		String str;
		int ctr = 0;
		int lines = 0;
		while ((str = br.readLine()) != null)   {
			String[] parsed = str.split("\t");
			int queryTime = Integer.parseInt(parsed[0]);
			int queryIP = Integer.parseInt(parsed[1].split("\\.")[0]);
			int queryWSize = Integer.parseInt(parsed[2]);
			//5. Inserting values from file1 up to the queryTime
			while(ctr < queryTime){
				String str2 = br2.readLine();
				int ip = Integer.parseInt(str2.split("\t")[0].split("\\.")[0]);
				jw.insertEvent(ip);
				ctr += 1;
			}
			int est = jw.getFreqEstimation(queryIP, queryWSize);
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







