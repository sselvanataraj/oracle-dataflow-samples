package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/*
 * This example shows converting CSV to Parquet in Spark.
 * 
 * Before you begin, upload fake_data.csv from the sample bundle and customize the paths.
 * 
 * In OCI CLI run:
 * oci os object put --bucket-name <bucket> --file fake_data.csv
 *
 */

public class Example {


	// Customize these before you start.
	private static String inputPath = "oci://siva@axmemlgtri2a/geo-data.csv";
	private static String outputPath = "oci://siva@axmemlgtri2a/csv/geo-data.csv";

	public static void main(String[] args) throws Exception {
		/*
		// Get our Spark session.
		if (args != null && args.length > 1) {
			inputPath = args[0];
			outputPath = args[1];
		}
		SparkSession spark = DataFlowSparkSession.getSparkSession("Sample App");
		//spark.sparkContext().setLogLevel("DEBUG");
		Dataset<Row> df = spark.read().csv(inputPath);
		df.write().mode(SaveMode.Overwrite).format("parquet").save(outputPath);
		System.out.println("Conversion to Parquet complete.");

		spark.stop();
	 */
	}
}
