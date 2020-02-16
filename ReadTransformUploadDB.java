package com.myproject.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {
	
	public static void main(String args[]) throws InterruptedException {
		
		//
		SparkSession spark = new SparkSession.Builder()
				.appName("Csv to DB")
				.master("local")
				.getOrCreate();
		
		//data frame just like table with rows
		Dataset<Row> df = spark.read().format("csv")
			 .option("header", true)
			 .load("src/main/resources/name_and_comments.txt");
		
		
		//data transformation
		df = df.withColumn("full_name", concat(df.col("first_name"), lit(", "), df.col("last_name")));
		
		//transformation
		df = df.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());
		
		String dbConnectionUrl = "jdbc:postgresql://localhost:5432/postgres"; 
		Properties prop = new Properties();
	    prop.setProperty("driver", "org.postgresql.Driver");
	    prop.setProperty("user", "postgres");
	    prop.setProperty("password", "adm2in"); 
	    
	    
	    
	    df.write()
	    	.mode(SaveMode.Overwrite)
	    	.jdbc(dbConnectionUrl, "project1", prop);
	}
}
