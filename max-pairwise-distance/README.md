# Max Pairwise Distance
Example of max pairwise distance algorithm written in Java. The code is part of some homeworks (made in team) regarding the 'Big Data Computing' course from the 'Master Degree in ICT Engineering', University of Padova, A.Y. 2019/2020.

## Authors
* [Favaro Simone](https://github.com/suerioh)
* [Molon Alberto](https://github.com/MollyLand)

## Problem Statement
Given a set _S_ of points from a metric space, the algorithm requires to determine the maximum distance between two points.
For this problem, we implement the following 3 approaches:
1. Exact solution;
2. 2-approximation by taking _k_ random points and returning the maximum distance between these points and all other points of _S_;
3. Exact solution of a subset _C_ of _S_, where _C_ are _k_ centers retruned by Farthest-First Traversal.
We work with points in Euclidean space (real cooordinates) and with the standard Euclidean L2-distance.

## Description
We develop the following 3 methods:
* exactMPD(S): receives in input a set of points _S_ and returns the max distance between two points in _S_.
* twoApproxMPD(S,k): receives in input a set of points _S_ and an interger _k < |S|_, selects _k_ points at random from _S_ (let _S'_ denote the set of these _k_ points) and returns the maximum distance _d(x,y)_, over all _x_ in _S'_ and _y_ in _S_. We define a constant _SEED_ in our main program and we use that value as a seed for the random generator. Note that _SEED_ must be a long and we use method setSeed from our random generator to initialize the seed.
* kCenterMPD(S,k): receives in input a set of points _S_ and an integer _k < |S|_, and returns a set _C_ of _k_ centers selected from _S_ using the Farthest-First Traversal algorithm.
The input point set _S_ (as well as the output _C_ of kCenterMPD(S,k)) is represented as instances of ArrayList<Vector>.

The program receives in input a path to a text file containing a set of points in Euclidean space, and an integer _k_. The file must contain one point per line, with coordinates separated by comma. The program incorporates the methods developed above and does the following:
* Reads the input: reads the points from the file into an ArrayList<Vector> called "inputPoints".
* Runs exactMPD(inputPoints), measuring its running time (in ms).
* Runs twoApproxMPD(inputPoints,k), measuring its running time (in ms).
* Runs kcenterMPD(inputPoints,k), saves the returned points in an ArrayList<Vector> called "centers", and runs exactMPD(centers), measuring the combined running time (in ms) of the two methods.

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
java -cp ./target/max-pairwise-distance-1.00-jar-with-dependencies.jar MaxPairwiseDistance ./datasets/uber-small.csv 40
```