import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.IOException;

public class DiversityMax {

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Authors:
    // Favaro Simone
    // Molon Alberto
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static void main(String[] args) throws Exception {
        // make sure that three parameters are passed
        if (args.length != 3)
            throw new IllegalArgumentException("Expecting three parameters (file path, k, L)");

        int k = Integer.valueOf(args[1]);                        // reading k value from args[1]
        int L = Integer.valueOf(args[2]);                       // reading L value from args[2]

        long start, end;        // variables for initialization time estimation

        // Spark configuration
        SparkConf conf = new SparkConf(true).setMaster("local[*]").setAppName("Homework3");
        JavaSparkContext sc = new JavaSparkContext(conf);                  // object representing the master process
        sc.setLogLevel("WARN");

        start = System.currentTimeMillis();
        // creation of the JavaRDD (of points) from the file passed in input
        JavaRDD<Vector> inputPoints = sc
                .textFile(args[0])          // take the file as args[0]
                .map(DiversityMax::strToVector)   // map method that exploits the strToVector function (see below) that creates the Vector
                .repartition(L)             // creation of L random partitions of the points
                .cache();                   // store in cache (useful for time estimation)
        end = System.currentTimeMillis();

        long inputPointSize = inputPoints.count();
        if(k > inputPointSize/L || L > inputPointSize)
            throw new IllegalArgumentException("Expecting k and L less then the size of the input dataset");

        System.out.println("\nNumber of points = " + inputPointSize);
        System.out.println("k = " + k);
        System.out.println("L = " + L);
        System.out.println("Initialization time = " + (end - start) + " ms");

        ArrayList<Vector> solution = runMapReduce(inputPoints, k, L);

        double avgDist = measure(solution);
        System.out.println("Average distance = " + avgDist + "\n");
    }   // END main


    // auxiliary method used for reading the input and returning the Vector object
    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }   // END strToVector


    // runSequential method --> Sequential 2-approximation algorithm for diversity maximization returning k solution points
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {
        final int n = points.size();
        if (k >= n) {
            return points;
        }
        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter = 0; iter < k / 2; iter++) {
            // find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i + 1; j < n; j++) {
                        if (candidates[j]) {
                            // use squared euclidean distance to avoid an sqrt computation!
                            double d = Vectors.sqdist(points.get(i), points.get(j));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }   // END runSequential


    // method that implements the 4-approximation MapReduce algorithm for diversity maximization
    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsRDD, int k, int L) {
        // variables for time estimation
        long startR1, endR1;
        long startR2, endR2;

        startR1 = System.currentTimeMillis();
        JavaRDD<Vector> kPointSubset = pointsRDD                        // The Map phase of ROUND 1 was done when RDD where created
                .mapPartitions((set) -> {                               // Reduce phase of ROUND 1
                    ArrayList<Vector> chosenPoints = new ArrayList<>();      
                    while(set.hasNext())
                        chosenPoints.add(set.next());
                    ArrayList<Vector> centers = kcenterMPD(chosenPoints, k);    // apply the kcenter method to extract k points from each partition
                    return centers.iterator();
                })
                .cache();           // store in cache (useful for time estimation)
        kPointSubset.count();
        endR1 = System.currentTimeMillis();
        System.out.println("Runtime of Round 1 = " + (endR1 - startR1) + " ms");

        startR2 = System.currentTimeMillis();
        // collect all the selected k points into the arrayList coreset
        ArrayList<Vector> coreset = new ArrayList<>(kPointSubset.collect());     // ROUND 2
        ArrayList<Vector> kPoints = runSequential(coreset,k);                   // apply the runSequential method to compute the k points       
        endR2 = System.currentTimeMillis();
        System.out.println("Runtime of Round 2 = " + (endR2 - startR2) + " ms");

        return kPoints;
    }   // END runMapReduce


    //kcenterMPD method
    public static ArrayList<Vector> kcenterMPD(ArrayList<Vector> inputPoints, int k){
        ArrayList<Double> dist = new ArrayList<>();         // init of a temp variable for computing distance
        double maxDist = 0.0;                               // init of the max distance
        Vector maxCenter = Vectors.zeros(1);             // init of a temp variable for computing centers
        ArrayList<Vector> centers = new ArrayList<>();      // init of the output

        centers.add(0,inputPoints.get(0));          // c1 is the first center, which is arbitrary (the first point of set S)
        for(int i=0; i<k-1; i++)           // first loop cicles for k-2 times because first center is already determined
        {
            for(int j=0; j<inputPoints.size(); j++)       // second loop cicles for all points of inputPoints (set S)
            {
                if(i == 0)
                {
                    dist.add(Math.sqrt(Vectors.sqdist(centers.get(i),inputPoints.get(j))));
                    if(dist.get(j) > maxDist)
                    {
                        maxCenter = inputPoints.get(j);    // saving the point which has the max distance (so it is a possible center)
                        maxDist = dist.get(j);
                    }
                }
                else    // only for i >= 1
                {
                    if(dist.get(j) > Math.sqrt(Vectors.sqdist(centers.get(i),inputPoints.get(j))))   //find the closest center from every point
                        dist.set(j,Math.sqrt(Vectors.sqdist(centers.get(i),inputPoints.get(j))));
                    if(dist.get(j) > maxDist)
                    {
                        maxCenter = inputPoints.get(j);     // saving the point which has the max distance (so it is a possible center)
                        maxDist = dist.get(j);
                    }
                }
            }
            centers.add(maxCenter);           // add the the temp center to the list of centers (after the end of the second loop)
            maxDist=0.0;                      // set to zero the max distance
        }
        return centers;                 // it is the set of centers returned as output
    }   // END kcenterMPD


    // method that determines the average distance among given points passed as parameters
    public static double measure(ArrayList<Vector> pointsSet) {
        double avgDist = 0.0;
        double dist = 0;
        int numbDist = 0;

        // first for loop cicles all values of pointSet
        for(int i=0; i<pointsSet.size(); i++)
        {
            // second for loop cicles only Vectors in pointSet greater than the i-th one in order to avoid unnecessary calculations of symmetric distances
            for(int j=i+1; j<pointsSet.size(); j++)
            {
                // distance from the i-th to the j-th Vectors of pointSet, using Vectors.sqdist() method
                dist += Math.sqrt(Vectors.sqdist(pointsSet.get(i),pointsSet.get(j)));
                numbDist++;
            }
        }
        if(numbDist == 0)   // consistency check (maybe some errors could occur)
            System.out.println("You can't do the Integer division. Denominator is equal to zero!!");

        // compute the average distance
        avgDist = dist / numbDist;
        return avgDist;
    }   // END measure
}
