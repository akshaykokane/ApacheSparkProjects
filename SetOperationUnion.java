 package com.project.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SetOperationUnion {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
				.appName("Combine 2 dataset")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> durhamDf = buildDrhamParkDataFrame(spark);
		
		//durhamDf.show(3);
		//durhamDf.printSchema();
		
		 Dataset<Row> philDf = buildPhilParksDataFrame(spark);
	    philDf.printSchema();
		    philDf.show(10);
		
		    combineDataframes(durhamDf, philDf);
	}

	private static void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
		
		Dataset<Row> df = df1.unionByName(df2);
		
		df.show(500);
		
		//System.out.println("We have "+ df.count() +" records");
		
		df = df.repartition(5);
		 
		Partition partition[] = df.rdd().partitions();
		
		System.out.println(partition.length);
		
	}

	private static Dataset<Row> buildPhilParksDataFrame(SparkSession spark) {
		Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.load("src/main/resources/philadelphia_recreations.csv");
		
		
		df = df.filter("lower(USE_) like '%park%' ");
		
		df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
				.withColumnRenamed("ASSET_NAME", "park_name")
				.withColumn("city", lit("Philadelphia"))
				.withColumnRenamed("ADDRESS", "address")
				.withColumn("has_playground", lit("UNKNOWN"))
				.withColumnRenamed("ZIPCODE", "zip_code")
				.withColumnRenamed("ACREAGE", "land_in_acres")
				.withColumn("geoX", lit("UNKNONW"))
				.withColumn("geoY", lit("UNKNONW"))
				.drop("SITE_NAME")
				.drop("OBJECTID")
				.drop("CHILD_OF")
				.drop("TYPE")
				.drop("USE_")
				.drop("DESCRIPTION")
				.drop("SQ_FEET")
				.drop("ALLIAS")
				.drop("CHRONOLOGY")
				.drop("NOTES")
				.drop("DATE_EDITED")
				.drop("EDITED_BY")
				.drop("OCCUPANT")
				.drop("TENANT")
				.drop("LABEL");
				
		
		return df;
	}

	public static Dataset<Row> buildDrhamParkDataFrame(SparkSession spark) {
		Dataset<Row> df = spark.read().format("json").option("multiline", true)
					.load("src/main/resources/durham-parks.json");
		
		df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"),
				df.col("fields.objectid"), lit("_Durham") ))
				.withColumn("park_name",df.col("fields.park_name"))
				.withColumn("city", lit("Durham"))
				.withColumn("address", df.col("fields.address"))
				.withColumn("has_playground", df.col("fields.playground"))
				.withColumn("zip_code", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
				.drop("fields").drop("geometry").drop("record_timestamp").drop("recordid")
				.drop("datasetid");
				
		return df;
	}
	
	
	
	
}
