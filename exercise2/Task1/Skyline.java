import java.io.DataInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Pattern;

public class Skyline {
    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions) {
        ArrayList<Tuple> merged = new ArrayList<Tuple>();
        for(ArrayList<Tuple> part : partitions){
        	merged.addAll(part);
        }
        return nlSkyline(merged);
    }

    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize) {
    	//Handle edge cases
    	if (blockSize <= 0 || blockSize >= inputList.size())
    		return nlSkyline(inputList);

        //Partition the input list and call nlSkyline on each partition
    	ArrayList<ArrayList<Tuple>> partitions = new ArrayList<ArrayList<Tuple>>();
    	ArrayList<Tuple> part = new ArrayList<Tuple>();
    	int ctr = 0;
    	for (Tuple e : inputList){
    		part.add(e);
    		ctr += 1;
    		if ((ctr % blockSize == 0) || (ctr == inputList.size() - 1)){
    			partitions.add(nlSkyline(part)); //call nlSkyline on a single partition
    			part.clear();
    		}
    	}
    	return mergePartitions(partitions); //return the merged partitions

    }

    public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {
    	ArrayList<Tuple> res = new ArrayList<Tuple>();
		Map deleted = new HashMap();
    	int i = 0, size = partition.size();
        while(i < size){
        	Tuple cur = partition.get(i);
        	if(!deleted.containsValue(i)){
        		for(int j =i + 1; j < size; j++){
        			if (!deleted.containsKey(j)){
	        			if (cur.dominates(partition.get(j))){
	        				deleted.put(j, partition.get(j));
	        			}
	        			else if (partition.get(j).dominates(cur)){
	        				deleted.put(i, cur);
	        				cur = partition.get(j);
	        			}
        			}
        		}
        	}
        	if (!deleted.containsValue(cur) && !res.contains(cur)){
        		res.add(cur);
        	}
        	i += 1;
        }
        return res;
    }


    public static void main(String args[]) throws IOException {


        //FileWriter fileWriter = new FileWriter(new File("car_out.csv"));
        BufferedReader br = new BufferedReader(
                new InputStreamReader(new DataInputStream(new FileInputStream("car.csv"))));
        ArrayList<Tuple> cars = new ArrayList<>();
        Skyline sky = new Skyline();
        String str;
        while ((str = br.readLine()) != null) {
            String[] parsed = str.split(Pattern.quote("|"));
            cars.add(new Tuple(Integer.parseInt(parsed[0]), Integer.parseInt(parsed[1])));
        }
        br.close();
        ArrayList<Tuple> res = sky.dcSkyline(cars, 10);
        //System.out.println(res);
        //fileWriter.close();

    }

}

class Tuple {
    private int price;
    private int age;

    public Tuple(int price, int age) {
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other) {
        if ((this.age < other.age && this.price <= other.price) ||
        		(this.price < other.price && this.age <= other.age))
        	return true;
        return false;
    }

    public boolean isIncomparable(Tuple other) {
        if (this.dominates(other) || other.dominates(this))
        	return false;
        return true;
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    public String toString() {
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if (o instanceof Tuple) {
            Tuple t = (Tuple) o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }
}
