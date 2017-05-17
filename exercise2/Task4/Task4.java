package q4;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Task4 implements Serializable{
	int cCustKeyInd = 0;
	int oCustKeyInd = 1;
	int oCommentInd = 8;
	int partitionSize = 13; // An arbitrary prime number
	String customerFilePath;
	String orderFilePath;
	String outputFilePath;
	JavaPairRDD<Integer, Integer> cCustKey;
	JavaPairRDD<Integer, String> oCustKeyComments;
	HashPartitioner partitioner;

	public Task4(String customerFilePath, String orderFilePath, String outputFilePath) {
		this.customerFilePath = customerFilePath;
		this.orderFilePath = orderFilePath;
		this.outputFilePath = outputFilePath;
		partitioner = new HashPartitioner(partitionSize);

	}

	/**
	 * Fetches the required columns from the databases
	 * 
	 * @return
	 * @throws IOException
	 */
	public void loadColumns(JavaSparkContext sc) throws IOException {
		//Get columns that participate in join to RDDs, partition them using the same HashPartitioner
		cCustKey = sc.textFile(customerFilePath).mapToPair(x -> {
			String[] parsed = x.split(Pattern.quote("|"));
			return new Tuple2<Integer, Integer>(Integer.parseInt(parsed[cCustKeyInd]), 42);
		}).partitionBy(partitioner);
		oCustKeyComments = sc.textFile(orderFilePath).mapToPair(x -> {
			String[] parsed = x.split(Pattern.quote("|"));
			return new Tuple2<Integer, String>(Integer.parseInt(parsed[oCustKeyInd]), parsed[oCommentInd]);
		}).partitionBy(partitioner);

	}

	/**
	 * Executes distributed join on each node locally using the .zipPartitions()
	 * method, writes results to the output file
	 * 
	 * @throws IOException
	 */
	public void executeDistJoin() throws IOException {
		JavaRDD<Tuple2<Integer, String>> joinRes = oCustKeyComments.zipPartitions(cCustKey, (ordIter, custIter) -> {
			List<Tuple2<Integer, String>> tmp = new ArrayList<Tuple2<Integer, String>>();
			Map<Object, Integer> dictionary = new HashMap<>();
			//Go over customers, put them in a hash table and then go over orders and look for a match
			while(custIter.hasNext()){
				int cID = custIter.next()._1;
				dictionary.put(cID, cID);
			}
			while(ordIter.hasNext()){
				Tuple2<Integer, String> order = ordIter.next();
				if (dictionary.containsKey(order._1)){
						tmp.add(new Tuple2<Integer, String>(dictionary.get(order._1), order._2));
				}
			}
			return tmp.iterator();
		});
		writeToCSV(joinRes.collect());

	}

	/**
	 * Writes results in CSV format to the output file
	 * 
	 * @param res
	 * @throws IOException
	 */
	private void writeToCSV(List<Tuple2<Integer, String>> res) throws IOException {
		// Write the results row by row
		FileWriter fw = new FileWriter(outputFilePath);
		for (Tuple2<Integer, String> tmp : res) {
			fw.write(tmp._1 + "," + tmp._2 + "\n");
		}
		fw.flush();
		fw.close();

	}

	public static void main(String[] args) throws Exception {
		// Initialize spark context
		String master = "local[4]";
		SparkConf conf = new SparkConf().setAppName(Task4.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		String table1FilePath = args[0];
		String table2FilePath = args[1];
		String outputFilePath = args[2];
		Task4 joiner = new Task4(table1FilePath, table2FilePath, outputFilePath);
		joiner.loadColumns(sc);
		joiner.executeDistJoin();

	}
}
