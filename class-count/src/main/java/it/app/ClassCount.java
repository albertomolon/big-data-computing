import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class ClassCount {

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Authors:
    // Favaro Simone
    // Molon Alberto
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {                                                     // Checking number of cmd parameters
            throw new IllegalArgumentException("USAGE: num_partitions file_path");    // Parameters are: number_partitions, <path to file>
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK configuration
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setMaster("local[*]").setAppName("ClassCount");
        JavaSparkContext sc = new JavaSparkContext(conf);                  //Object representing the master process
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions (K=4)
        int K = Integer.parseInt(args[0]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> pairStrings = sc.textFile(args[1]).repartition(K);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // RDD of key/value pairs
        JavaPairRDD<String, Long> count;


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CLASS COUNT with DETERMINISTIC PARTITIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        count = pairStrings
                .flatMapToPair((line) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = line.split(" ");							// tokens is an array of two elements
                    ArrayList<Tuple2<Integer, String>> pairs = new ArrayList<>();
                    int interKey=Integer.parseInt(tokens[0]);						// Convert the array's first value into an integer
                    pairs.add(new Tuple2<>(interKey % K, tokens[1]));				// pairs is a 2-tuple with a key value computed as the mod function
                    return pairs.iterator();
                })

                // We pass a couple in the reduce phase because it has a key (int value) and the class (string value)

                .groupByKey()    // <-- REDUCE PHASE (R1)
                .flatMapToPair((couple) -> {

				  /* The hash-map is used for gathering the set of intermediate pairs with the same key (which now is the class)
				     Then, we count the occurrencies belonging to the same class (but it is a 'partial' count) */
                    HashMap<String, Long> hcounts = new HashMap<>();
                    for (String c : couple._2) {

                        hcounts.put(c, 1L + hcounts.getOrDefault(c, 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();										// We return an iterator of partial counts
                })

                // The MAP PHASE (R2) is empty

                .groupByKey()    // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {			// We sum all the partial counts belonging to the same class
                        sum += c;
                    }
                    return sum;
                });

        // Print all elements of JavaPairRDD count sorted by the key (of type String)
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");
        System.out.print("Output pairs = ");
        for(Tuple2<String,Long> tuple: count.sortByKey().collect()) {
            System.out.print("("+tuple._1()+","+tuple._2()+") ");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CLASS COUNT with SPARK PARTITIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // N_max is the max number of pairs that in Round 1 are processed by a single reducer
        int N_max = 0;

	  /* We access all the partition lengths and we put in N_max the largest one. In particular:
		- glom returns an RDD whose elements are arrays of objects of type String
		  and each such array contains the objects of a distinct partition of pairStrings;
		- collect transforms the RDD of arrays into a list of arrays;
		- get(k) access the k^th value in the array;
		- size returns the length of the list which contains the partition */

        for(int k = 0; k < 4; k++){
            int temp = pairStrings.glom().collect().get(k).size();
            if(temp >= N_max){
                N_max = temp;
            }
        }

        count = pairStrings
                .flatMapToPair((line) -> {    // <-- MAP PHASE (R1)						// tokens is an array of two elements
                    String[] tokens = line.split(" ");
                    ArrayList<Tuple2<Integer, String>> pairs = new ArrayList<>();
                    int interKey=Integer.parseInt(tokens[0]);								// Convert the array's first value into an integer
                    pairs.add(new Tuple2<>(interKey, tokens[1]));							// Insert the key/value retrieved pair into pairs
                    return pairs.iterator();
                })

                // We pass a couple in the reduce phase because it has a key (int value) and the class (string value)

                .mapPartitionsToPair((couple) -> {    // <-- REDUCE PHASE (R1)

			  /* The hash-map is used for gathering the set of intermediate pairs with the same key (which now is the class)
				 Then, we count the occurrencies belonging to the same class (but it is a 'partial' count)*/

                    HashMap<String, Long> hcounts = new HashMap<>();
                    while (couple.hasNext()){
                        Tuple2<Integer, String> tuple = couple.next();
                        hcounts.put(tuple._2(), 1L + hcounts.getOrDefault(tuple._2(), 0L));
                    }

                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();							// We return an iterator of partial counts
                })

                // The MAP PHASE (R2) is empty

                .groupByKey()     // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;										// We sum all the partial counts belonging to the same class
                    }
                    return sum;
                });

        // Print the most frequent class
        System.out.println(" ");
        System.out.println("VERSION WITH SPARK PARTITIONS");
        System.out.print("Most frequent class = ");
        Tuple2<String,Long> tuple_maxvalue = new Tuple2<>("", 0L);

        /* We ispect every tuple and check which one is the max */
        for(Tuple2<String,Long> tuple : count.collect()) {
            if (tuple._2() >= tuple_maxvalue._2()) {
                tuple_maxvalue = tuple;
            }
        }
        System.out.println("(" + tuple_maxvalue._1() + "," + tuple_maxvalue._2() + ") ");

        // Print the max number of pairs that in Round 1 are processed by a single reducer
        System.out.println("Max partition size = " + N_max);

    }
}