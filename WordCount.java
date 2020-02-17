
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.LineMapper;

public class WordCount {

	public void start() {
		
		 String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
			  		"'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
			  		"'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
			  		"'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
			  		"'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," + 
			  		"'your', 'you', 'I', "
			  		+ " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
		 
		SparkSession spark = SparkSession.builder()
		        .appName("unstructured text to flatmap")
		        .master("local")
		        .getOrCreate();
		
		String filename = "shakespeare.txt";
		
		Dataset<Row> df = spark.read().format("text")
		        .load(filename);
		
	//	df.show(10);
		
		Dataset<String> wordDS = df.flatMap(new LineMapper(), Encoders.STRING());
		
		
		
		Dataset<Row> wordDF = wordDS.groupBy("value").count();
		
		wordDF = wordDF.orderBy(wordDF.col("count").desc());
		
		wordDF = wordDF.filter("lower(value) NOT IN" + boringWords);
		
		wordDF.show(100);
		
		
		  
	}
	

}
