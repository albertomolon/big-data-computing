# Word Count
Example of word count algorithm using Spark. The code was modified starting from a code snippet viewed in the 'Big Data Computing' course from the 'Master Degree in ICT Engineering', University of Padova, A.Y. 2019/2020.

## Prerequisites
You need Java Development Kit (JDK) version 8.

## Built With
[Maven](https://maven.apache.org/) - Dependency Management

## Datasets
The dataset ``` example.txt``` is provided in the folder ``` datasets/```.

## Running
```
mvn clean package
```
```
java -cp ./target/word-count-1.00-jar-with-dependencies.jar WordCount 4 ./datasets/example.txt
```