# Spark-SQL-dataset

Using Spark SQL to search data in a dataset composed of Wi-fi informations.

This project was made with IntelliJ IDE as a Maven project. 

Using the data provided in the dataset, I searched for:

    Highest number of connection in a single day
    The day in wich connections were at maximun
    The zone with the highest average connections per day
    The zone with the lowest average connections per day
    Sum of connections per day (all zones)

The dataset indicates the number of navigation sessions carried out each day for each zone; a user who connects to WiFi in the morning and in the evening is counted twice. A user who connects with two separate devices, such as smartphones and tablets, is counted twice.

You may want to use Spark DataFrame and/or Spark SQL.



# Search in Milano_Wifi

Compile sources

$ mvn clean && mvn package

Run the Main class locally

$ mvn exec:exec

Run the Main class on Hadoop cluster

$ spark-submit \
  --master yarn \
  --driver-memory 512m  \
  --executor-memory 512m \
  --class "name of your class" \
  --deploy-mode cluster \
  target/
