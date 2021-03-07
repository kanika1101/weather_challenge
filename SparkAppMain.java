import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.mllib.rdd.RDDFunctions.*;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.JavaConverters;
import scala.collection.Seq;


import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class SparkAppMain {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: SparkAppMain <file>");
//            System.exit(1);
//        }


        SparkSession spark = SparkSession
                .builder()
                .appName("WeatherData")
                .getOrCreate();

        //1. Load the global weather data into your big data technology of choice.

        Dataset<Row> csvDataset = spark.read().format("csv").option("header", "true")
                .load("C:/Users/Kush/Documents/kanika/paytm-weather-challenge/paytmteam-de-weather-challenge-beb4fc53605c/data/2019/part-00000-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv");

        // Appending new csv files to the original weather dataset will happen here.



        // getting country list
        Dataset<Row> countryListDataset = spark.read().format("csv").option("header", "true")
                .load("C:/Users/Kush/Documents/kanika/paytm-weather-challenge/paytmteam-de-weather-challenge-beb4fc53605c/countrylist.csv");

        // getting station list
        Dataset<Row> stationListDataset = spark.read().format("csv").option("header", "true")
                .load("C:/Users/Kush/Documents/kanika/paytm-weather-challenge/paytmteam-de-weather-challenge-beb4fc53605c/stationlist.csv");

        //2. Join the stationlist.csv with the countrylist.csv to get the full country name for each station number
        Dataset <Row> countryNames = countryListDataset.join(stationListDataset, countryListDataset.col("COUNTRY_ABBR").equalTo(stationListDataset.col("COUNTRY_ABBR")));

        // 3. Join the global weather data with the full country names by station number.

        Dataset <Row> fullDataset = csvDataset.join(countryNames, csvDataset.col("STN---").equalTo(countryNames.col("STN_NO")));

        fullDataset.createOrReplaceTempView("csvdataTable");

        // 1. Which country had the hottest average mean temperature over the year?
        Dataset<Row> reducedCSVDataset = spark.sql("select COUNTRY_FULL from csvdataTable where TEMP = (select max(TEMP) from csvdataTable  where TEMP != 9999.9 group by COUNTRY_FULL limit 1) limit 1");
        Dataset<String> rdds = reducedCSVDataset.toDF().select("COUNTRY_FULL").as(Encoders.STRING());


        List<String> listOfStrings = rdds.collectAsList();
        listOfStrings.forEach(x -> System.out.println("country with max temp = " + x));


        //3. Which country had the second highest average mean wind speed over the year?
        // we can also use rank() function here. Due to shortage of time took the simpler path.
        Dataset<Row> rankwdsp = spark.sql("select COUNTRY_FULL from (select COUNTRY_FULL, avg(WDSP) as avg_wind from csvdataTable group by COUNTRY_FULL order by avg(WDSP) desc limit 2) order by avg_wind asc limit 1");
        Dataset<String> rdds1 = rankwdsp.toDF().select("COUNTRY_FULL").as(Encoders.STRING());


        List<String> rankwindspeed = rdds1.collectAsList();
        rankwindspeed.forEach(x -> System.out.println("country with second highest wind speed = " + x));


        spark.stop();
    }
}