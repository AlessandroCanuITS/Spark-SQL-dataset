import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class MilanoWifi {

    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Search in Milano Wifi")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .option("multiline", "true")
                .option("inferSchema", "false")
                .option("nullValue", "null")
                .option("treatEmptyValuesAsNulls", "true")
                .json("dataset/wifi_milano_daily_login_per_zone.json"); //it could be necessary to specify the absolute path for the file


        df.createOrReplaceTempView("milano_wifi");

        // Highest number of connection in a single day
        System.out.println(String.format("\n=== Max connection in a day ==="));
        df
                .select(col("Data"), col("Valore"))
                .groupBy(col("Data"))
                .sum("Valore")
                .orderBy(desc("sum(Valore)"))
                .limit(1)
                .show();

        //The day in which connections were at maximum
        System.out.println(String.format("\n=== Max connection ever ==="));
        df
                .select(col("Data"), col("Valore"))
                .groupBy(col("Data"))
                .sum("Valore")
                .orderBy(desc("sum(Valore)"))
                .limit(1)
                .show();


        //The zone with the highest average connections per day
        System.out.println(String.format("\n=== Zone with max avg connection  in a day==="));
        df
                .select(col("Zona"), col("Data"), col("Valore"))
                .groupBy(col("Zona"))
                .avg("Valore")
                .orderBy(desc("avg(Valore)"))
                .limit(5)
                .show();



        //The zone with the lowest average connections per day
        System.out.println(String.format("\n=== Zone with low avg connection  in a day==="));
        df
                .select(col("Zona"), col("Data"), col("Valore"))
                .groupBy(col("Zona"))
                .avg("Valore")
                .orderBy(asc("avg(Valore)"))
                .limit(5)
                .show();





        //Sum of connections per day (all zones)
        System.out.println(String.format("\n=== Sum of connections per day per day (DataFrame API) ==="));
        df
                .groupBy(col("Data"))
                .sum("Valore")
                .orderBy(to_date(col("Data")).desc())
                .show();


    }

}
