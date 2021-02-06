# Diversity Maximization
Example of diversity maximization algorithm using Spark. The code is part of some homeworks (made in team) regarding the 'Big Data Computing' course from the 'Master Degree in ICT Engineering', University of Padova, A.Y. 2019/2020.

## Authors
* [Favaro Simone](https://github.com/suerioh)
* [Molon Alberto](https://github.com/albertomolon)

## Problem Statement
Given a set _P_ of _N_ points in a metric space and an integer _k < N_, diversity maximization (remote-clique variant) requires to find _k_ distinct points of _P_ so to maximize their average distance.

## Description
This algorithm implements two versions of the diversity maximization algorithm:
1. 2-approximate sequential algorithm: for _floor(k/2)_ times, select the two unselected points with maximum distance. If _k_ is odd, add at the end an arbitrary unselected point. For datasets of millions/billions points, the algorithm, whose complexity is quadratic in _N_, becomes impractically slow.
2. 4-approximation coreset-based MapReduce algorithm: Partition _P_ into _L_ subsets and extract _k_ points from each subset using the Farthest-First Traversal algorithm. Compute the final solution by running the 2-approximate sequential algorithm on the coreset of _Lk_ points extracted from the _L_ subsets.
We work with points in Euclidean space (real cooordinates) and with the standard Euclidean L2-distance.
The program does the following things:
+ runSequential(pointSet,k): it receives in input a set of points (_pointSet_) and an integer _k_, and runs the sequential 2-approximation algorithm for diversity maximization returning _k_ solution points.
+ runMapReduce(pointsRDD,k,L): implements the 4-approximation MapReduce algorithm for diversity maximization described above. More specifically, it receives in input an RDD of points (_pointsRDD_), and two integers, _k_ and _L_, and performs the following activities.
	1. Round 1: subdivides pointsRDD into _L_ partitions and extracts _k_ points from each partition using the Farthest-First Traversal algorithm.
	2. Round 2: collects the _Lk_ points extracted in Round 1 from the partitions into a set called coreset and returns, as output, the _k_ points computed by runSequential(coreset,k). Note that coreset is not an RDD but an ArrayList<Vector>.
+ measure(pointsSet): receives in input a set of points (_pointSet_) and computes the average distance between all pairs of points. The set pointSet is represented as ArrayList<Vector>.

## Prerequisites
You need Java Development Kit (JDK) version 8.

## Built With
[Maven](https://maven.apache.org/) - Dependency Management

## Datasets
Two datasets are provided:
* "uber-small.csv": it contains geographic points of Uber pickups in New York City of one day (April 1st 2014). Each line contains two float separated by a comma, respectively representing the latitude and longitude (in decimal degrees) of a pickup.
* "aircraft-mainland.csv": it contains geographic points of 18760 aircraft landing facilities in the United States mainland. A landing facility can be an aeroport, an heliport, an ultralight landing, a seaplane base. Each line contains two float numbers separated by a comma, respectively representing the latitude and longitude (in decimal degrees) of a landing facility.

## Running
```
mvn clean package
```
```
java -cp ./target/diversity-max-1.00-jar-with-dependencies.jar DiversityMax ./datasets/aircraft-mainland.csv 12 50
```