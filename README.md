# Big Data Computing
List of algorithms analyzed during the 'Big Data Computing' course from the 'Master Degree in ICT Engineering', University of Padova, A.Y. 2019/2020.

## Word count
**Goal:** count the number of distinct words in a given text.

## Class count
**Goal:** count the most frequent class of a given dataset.

## Diversity Maximization
**Goal:** given a set _P_ of _N_ points in a metric space and an integer _k < N_, diversity maximization (remote-clique variant) finds _k_ distinct points of _P_ so to maximize their average distance.

## Max-Pairwise distance
**Goal:** given a set _S_ of points from a metric space, this algorithm determines the maximum distance between two points.

### Authors
* [Favaro Simone](https://github.com/suerioh)
* [Molon Alberto](https://github.com/albertomolon)

### Prerequisites
You need Java Development Kit (JDK) version 8.

### Built With
[Maven](https://maven.apache.org/) - Dependency Management

### Running
```
mvn clean package
```
```
java -cp ./target/file-name.jar name-of-executable 4 ./datasets/dataset-name
```
The two latest parameters (i.e. number of workers/threads and filepath) are mandatory.
