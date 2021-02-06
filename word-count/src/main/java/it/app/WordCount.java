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

public class WordCount {

    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: number_partitions, <path to file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setMaster("local[*]").setAppName("Word_Count");
        JavaSparkContext sc = new JavaSparkContext(conf);                   //object rapresenting the master process
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> docs = sc.textFile(args[1]).repartition(K);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long numdocs, numwords;
        numdocs = docs.count();
        System.out.println("Number of documents = " + numdocs);
        JavaPairRDD<String, Long> count;                                    //RDD of key/value pairs

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // STANDARD WORD COUNT with reduceByKey (1)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        /* docs is the initial RDD; count is the final result where we store data */
        /* With flatMapToPair(), each document becomes a Tuple2, e.g. key-value pair */
        /* document = single line of our dataset.txt file */
        /* Hashmap count accumulates the partial counts of the various words */
        /* First for: we process each word and we update the hashmap*/
        /* Second for: we transfer the key-value pairs from the hashmap into the arraylist*/

        count = docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)                //this processes is done in parallel for each doc
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> hcounts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        hcounts.put(token, 1L + hcounts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .reduceByKey((x, y) -> x+y);    // <-- REDUCE PHASE (R1)
        numwords = count.count();
        System.out.println("STANDARD WORD COUNT with reduceByKey");
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // IMPROVED WORD COUNT with groupByKey (2)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        /* docs is the same we have at the beginning (remember RDD are immutable) */
        /* the map phase is identical to the prev one excepts for the random key generation*/

        Random randomGenerator = new Random();
        count = docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> hcounts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<String, Long>>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        hcounts.put(token, 1L + hcounts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(randomGenerator.nextInt(K), new Tuple2<>(e.getKey(), e.getValue())));
                    }
                    return pairs.iterator();
                })

                /* in the reduce phase, we pass a triplet because it has a key (one of the randoms) and an interator of key-value pairs*/
                /* at the end of first for, the hashmap will contains entry (words, sum of counts which are same group of random key)*/

                .groupByKey()    // <-- REDUCE PHASE (R1)
                .flatMapToPair((triplet) -> {
                    HashMap<String, Long> hcounts = new HashMap<>();
                    for (Tuple2<String, Long> c : triplet._2()) {
                        hcounts.put(c._1(), c._2() + hcounts.getOrDefault(c._1(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                /*it is an iterator of partial counts (the one return above)*/

                .groupByKey()    // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        numwords = count.count();
        System.out.println("IMPROVED WORD COUNT with groupByKey");
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // IMPROVED WORD COUNT with mapPartitions (3)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        count = docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> hcounts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        hcounts.put(token, 1L + hcounts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                /* wc is an iterator*/

                .mapPartitionsToPair((wc) -> {    // <-- REDUCE PHASE (R1)
                    HashMap<String, Long> hcounts = new HashMap<>();
                    while (wc.hasNext()){
                        Tuple2<String, Long> tuple = wc.next();
                        hcounts.put(tuple._1(), tuple._2() + hcounts.getOrDefault(tuple._1(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : hcounts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey()     // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        numwords = count.count();
        System.out.println("IMPROVED WORD COUNT with mapPartitions");
        System.out.println("Number of distinct words in the documents = " + numwords);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // COMPUTE AVERAGE WORD LENGTH
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        /* Map method maps each element of count (so it is key-value pair) into an integer e.g. the key length */
        /* tuple is a generic name to indicate the element of count */
        /* with the reduce method, we sum up these lengths producing one single value (the sum of all word lengths)*/

        int avgwordlength = count
                .map((tuple) -> tuple._1().length())
                .reduce((x, y) -> x+y);
        System.out.println("Average word length = " + avgwordlength/numwords);
    }

}

// Every methods transform RDD, but then, they return as the beginning due to immutability property