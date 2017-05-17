import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;


public class task3 {
	public static long ipToLong(String ipString){
		String[] ip = ipString.split("\\.");
		return (long) (Integer.parseInt(ip[0])*256*256*256 +
				Integer.parseInt(ip[1])*256*256 + 
				Integer.parseInt(ip[2])*256 + 
				Integer.parseInt(ip[3]));
	}
	public static void main(String args[]) throws IOException, InsufficentMemoryException
	{
				
		//1. Get parameters and initialize the RangeBf
		Scanner scan = new Scanner(System.in);
		float pr = scan.nextFloat();
		scan.close();
		RangeBf rangeBf= new RangeBf(pr);
		
		//2. Create the file for writing the answers
		FileWriter fileWriter = new FileWriter(new File("out3.txt"));

		//3. Open both the query&string file to read
		FileInputStream fstream = new FileInputStream("task3_queries.txt");
		FileInputStream fstream2 = new FileInputStream("file1.tsv");
		DataInputStream in = new DataInputStream(fstream);
		DataInputStream in2 = new DataInputStream(fstream2);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));

		//4.Insert the whole stream (takes about 4 min)
		String str;
		while ((str = br2.readLine()) != null) 
			rangeBf.insertValue(ipToLong(str.split("\t")[0]));
		
		//5. Query
		int lines = 0;
		while((str = br.readLine()) != null) {
			String[] parsed = str.split("\t");
			long l = ipToLong(parsed[0]);
			long r =  ipToLong(parsed[1]);
			boolean exists = rangeBf.existsInRange(l, r);
			if (lines > 0)
				fileWriter.write(",");
			fileWriter.write(Boolean.toString(exists));
			lines += 1;	
		}
		//6. Close files
		in.close();
		in2.close();
		fileWriter.close();
	}
	
	
}