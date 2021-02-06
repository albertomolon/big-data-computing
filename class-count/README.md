# Class Count
Example of class count algorithm using Spark. The code is part of some homeworks (made in team) regarding the 'Big Data Computing' course from the 'Master Degree in ICT Engineering', University of Padova, A.Y. 2019/2020.

## Authors
* [Favaro Simone](https://github.com/suerioh)
* [Molon Alberto](https://github.com/albertomolon)

## Description
This algorithm implements different versions of the class count algorithm. It receives in input an integer _K_ and path to a text file containing a collection of pairs _(i,z<sub>i</sub>)_. The text file stores one pair per line which contains the key _i_ (a progressive integer index starting from 0) and the value _z<sub>i</sub>_ (a string) separated by single space, with no parentheses or comma.
The program must do the following things:
+ Reads the input set of pairs into an RDD _pairStrings_ of strings, and subdivides it into _K_ parts.
+ Runs the following two versions of the class count algorithm:
	1. Version with deterministic partitions: implements the algorithm in order to exploit deterministic partitions. So, in _Round 1_, each key _i_ is mapped into _i mod K_.
	2. Version with Spark partitions: implements the algorithm in order to use partitions provided by Spark. So, in the reduce phase of _Round 1_, each reducer processes one of the partitions of the RDD defined by Spark. Together with the output pairs (class, count) the algorithm produces a special pair _(maxPartitionSize, N<sub>max</sub>)_, where _N<sub>max</sub>_ is the max number of pairs that in _Round 1_ are processed by a single reducer.

## Prerequisites
You need Java Development Kit (JDK) version 8.

## Built With
[Maven](https://maven.apache.org/) - Dependency Management

## Datasets
Datasets are stored in the folder ``` datasets/```. Actually, there are two files: ``` example.txt```, which is trivial, and ```input_1000.txt```, which has 1000 entries.

## Running
```
mvn clean package
```
```
java -cp ./target/class-count-1.00-jar-with-dependencies.jar ClassCount 4 ./datasets/input_10000.txt
```