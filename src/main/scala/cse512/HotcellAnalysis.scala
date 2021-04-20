package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  
  // Filter for the given rectangle and the count the pickups for each unit cube.
  val filtered = pickupInfo.filter(pickupInfo("x") >= minX && pickupInfo("x") <= maxX &&
                                   pickupInfo("y") >= minY && pickupInfo("y") <= maxY &&
                                   pickupInfo("z") >= minZ && pickupInfo("z") <= maxZ).groupBy("x","y","z").count()

  filtered.show()

  // Get the list of count of all the cubes.
  val listValues = filtered.select("count").rdd.map(r => r(0)).collect.toList 

  // Calculate the sum of all the counts
  // and the sum of their squares.
  var countsum = 0.0
  var squaresum = 0.0
  for( a <- listValues ){
      var v = a.toString.toDouble
      countsum = countsum + v
      squaresum = squaresum + (v * v)
  }

  // Calculate mean and standard deviation of the count.
  val mean = countsum/numCells
  val stddev = math.sqrt((squaresum/numCells) - (mean))

  Console.println("mean: ",mean)
  Console.println("Sqsum: ",stddev)

  // Join the filtered table with itself and find the adjacent cubes
  // If two cubes are adjacent, the weight between them is 1.
  // Since the weight is 1, the sum of all the weights is just the count of adjacent cube (column name: weight_count).
  // The sum of the product of weight and the corresponding adjacent cubes pickup count is sum of the pickup counts (column name: weight_sum).
  val adj = filtered.as("first_filter").crossJoin(filtered.as("second_filter"))
            .filter("ABS(first_filter.x-second_filter.x) <= 1 AND ABS(first_filter.y-second_filter.y) <= 1 AND ABS(first_filter.z-second_filter.z) <= 1")
            .select(col("first_filter.x"), col("first_filter.y"),col("first_filter.z"), col("second_filter.count"))
            .groupBy("first_filter.x", "first_filter.y", "first_filter.z")
            .agg(sum("second_filter.count") as "weight_sum", count("second_filter.count") as "weight_count").persist()

  adj.createOrReplaceTempView("weightmatrix")

  adj.show()

  // Calculate the G score for each cube.
  val resultDf = spark.sql("select x,y,z,((weight_sum - (" + (mean).toString + " * weight_count)) / ("+ (stddev).toString +" * SQRT((("+(numCells).toString+" * weight_count)- (weight_count*weight_count))/("+(numCells-1).toString+")))) as gscore from weightmatrix").persist()
  resultDf.show()

  // Sort the dataset in descending order of gscore
  // and get the first 50 rows and select only x,y and z columns.
  val result = resultDf.sort(desc("gscore"))
              .limit(50)
              .repartition(1)
              .sort(desc("gscore"))
              .select("x", "y", "z")


  return result
}

}
