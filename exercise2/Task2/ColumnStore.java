import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ColumnStore {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ColumnStore.class);
	int nColumns;
	String inputFilePath;
	String outputFilePath;
	Map<Integer, String> columnTypes = new HashMap();
	List<JavaPairRDD> columns = new ArrayList<JavaPairRDD>();

	public ColumnStore(int nColumns, String inputFilePath, String outputFilePath, Map<Integer, String> columnTypes) {
		this.nColumns = nColumns;
		this.inputFilePath = inputFilePath;
		this.outputFilePath = outputFilePath;
		this.columnTypes = columnTypes;
	}

	/**
	 * Put each column of the csv file into a RDD
	 * 
	 * @param inputFilePath
	 * @param nColumns
	 * @return
	 * @return
	 * @throws IOException
	 */
	public void createRDDcolumns(JavaSparkContext sc) throws IOException {
		
		JavaRDD<String[]> fileRDD = sc.textFile(inputFilePath).map(line -> line.split(","));
		for (int i = 0; i < nColumns; i++) {
			final int index = i;
			JavaRDD<Object> colRDD; // gives error when <Object> is removed
			if (columnTypes.get(i).equals("Int")) {
				colRDD = fileRDD.map(line -> Integer.parseInt(line[index]));
			} else if (columnTypes.get(i).equals("Float")) {
				colRDD = fileRDD.map(line -> Double.parseDouble(line[index]));
			} else {
				colRDD = fileRDD.map(line -> line[index]);
			}
			columns.add(colRDD.zipWithIndex().mapToPair(x -> new Tuple2(x._2, x._1)));
		}
	}

	/**
	 * Processes a query and writes the results to a csv file
	 * 
	 * @param projectIndexes
	 * @param operations
	 * @throws IOException
	 */
	public void processQuery(List<Integer> selectIndexes, List<Tuple> operations) throws IOException {
		List<Long> resultIndexes; // list of rows that will be printed
		// If no operations, just write the whole column
		if (operations.size() == 0) {
			resultIndexes = LongStream.range(0, columns.get(0).count()).boxed().collect(Collectors.toList());
		}
		// Else, get the results from whereOperation and write those
		else {
			resultIndexes = whereOp(operations);
		}
		// write the results to a CSV file
		writeToCSV(selectIndexes, resultIndexes);
	}

	/**
	 * Comparison function for WHERE operation
	 * 
	 * @param op
	 * @param compVal
	 * @return
	 */

	public static <T extends Comparable<T>> Function<Tuple2, Boolean> predicate(String op, Comparable compVal) {
		switch (op) {
		case "=":
			return p -> ((Comparable) p._2).compareTo(compVal) == 0;
		case "<":
			return p -> ((Comparable) p._2).compareTo(compVal) < 0;
		case "<=":
			return p -> ((Comparable) p._2).compareTo(compVal) <= 0;
		case ">":
			return p -> ((Comparable) p._2).compareTo(compVal) > 0;
		case ">=":
			return p -> ((Comparable) p._2).compareTo(compVal) >= 0;
		default:
			return p -> false;
		}
	}

	/**
	 * Given a set of predicates, returns the row indexes that satisfy them.
	 * 
	 * @param operations
	 * @return
	 */
	private List<Long> whereOp(List<Tuple> operations) {
		// Get desired columns, apply filter to them
		List<JavaPairRDD> filteredCols = new ArrayList<JavaPairRDD>();
		for (Tuple op : operations) {
			Comparable compVal;
			if (columnTypes.get(op.getAttrIndex()).equals("Float")) {
				compVal = Double.parseDouble(op.getValue());
			} else if (columnTypes.get(op.getAttrIndex()).equals("Int")) {
				compVal = Integer.parseInt(op.getValue());

			} else {
				compVal = op.getValue();
			}

			JavaPairRDD filteredCol = columns.get(op.getAttrIndex()).filter(predicate(op.getOperation(), compVal));
			filteredCols.add(filteredCol);

		}
		// Join columns with each other to find which indexes will be printed
		JavaPairRDD joinedCols = filteredCols.get(0);
		for (int i = 1; i < filteredCols.size(); i++) {
			joinedCols = joinedCols.join(filteredCols.get(i));
		}
		// Get and return the resulting indexes
		List indexes = joinedCols.map(x -> ((Tuple2) x)._1).collect();
		return indexes;
	}

	/**
	 * Given indexes of selected columns and results, print them to a CSV file
	 * 
	 * @param selectIndexes
	 * @param resultIndexes
	 * @throws IOException
	 */
	private void writeToCSV(List<Integer> selectIndexes, List<Long> resultIndexes) throws IOException {
		// Fetch and print the results
		FileWriter fw = new FileWriter(outputFilePath);
		for (Long i : resultIndexes) {
			String toPrint = "";
			int ctr = 0;
			for (Integer j : selectIndexes) {
				toPrint += columns.get(j).lookup(i).get(0);
				ctr += 1;
				if (ctr != selectIndexes.size()){ //columns to be printed can be in reverse order, hence keep a ctr
					toPrint += ",";
				}
			}
			fw.append(toPrint + "\n");
		}
		fw.flush();
		fw.close();
	}

	public static void main(String[] args) throws Exception {
		String master = "local[4]";
		SparkConf conf = new SparkConf().setAppName(ColumnStore.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		String[] schema = args[2].split(",");
		Map<String, Tuple2> columns = new HashMap(); // give each column an
														// index
		Map<Integer, String> colTypes = new HashMap();
		for (Integer i = 0; i < schema.length; i++) {
			String[] parsed = schema[i].split(":");
			columns.put(parsed[0], new Tuple2(i, parsed[1]));
			colTypes.put(i, parsed[1]);
		}
		List<Integer> selectIndexes = new ArrayList<Integer>();
		for (String col : args[3].split(",")) {
			selectIndexes.add((Integer) columns.get(col)._1);
		}
		List<Tuple> operations = new ArrayList<Tuple>();
		if (args.length == 5) { // where clause can be empty
			for (String op : args[4].split(",")) {
				String[] parsed = op.split(Pattern.quote("|"));
				Integer index = (Integer) columns.get(parsed[0])._1;
				operations.add(new Tuple(index, parsed[1], parsed[2]));
			}
		}
		ColumnStore cs = new ColumnStore(columns.size(), inputFilePath, outputFilePath, colTypes);
		cs.createRDDcolumns(sc);
		cs.processQuery(selectIndexes, operations);

	}
}

class Tuple {
	int attrIndex;
	String value; // value can be float, int or string. Keep as string here,
					// cast accordingly outside.
	String operation;

	public Tuple(int attrIndex, String operation, String value) {
		this.attrIndex = attrIndex;
		this.value = value;
		this.operation = operation;
	}

	public int getAttrIndex() {
		return attrIndex;
	}

	public String getValue() {
		return value;
	}

	public String getOperation() {
		return operation;
	}

}