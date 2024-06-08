package com.prathima



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


import org.apache.spark.sql.functions._

import java.sql.Date


case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

case class Passengers(passengerId: Int, firstName: String, lastName: String)

case class FlightsCount(month: String, numberofFlights: Long)

case class TopMostFrequentFlyers(passengerId: Int, numberofFlights: Long, firstName: String, lastName: String)

case class FinalResults(passengerId: Int, routes: String, longestRouteWithoutUK: Int)

case class PassengersFlewTogether(passenger1Id: Int, passenger2Id: Int, numberofFlightsTogether: Long)

case class PassengersFlewTogetherDateRange(passenger1Id: Int, passenger2Id: Int, numberofFlightsTogether: Long, from: String, to: String)

object FlightData {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Inside Args if ")
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val dataPath = args(0)
    val dataPath2 = args(1)
   // println(s"Data path: $dataPath")
    //println(s"Data path: $dataPath2")
    //println(s"Working directory: ${new java.io.File(".").getCanonicalPath}")

    //Creation of Spark Session with 1 core
    val spark: SparkSession = SparkSession.builder().master("local[3]")
      .appName("Flight DataSet")
      .getOrCreate()

    //modularised the readCSV() function
    val flightDataFrame = readCSV(spark, dataPath)
    val passengerDataFrame = readCSV(spark, dataPath2)
    import spark.implicits._
    val flightDS: Dataset[Flight] = flightDataFrame.select("passengerId", "flightId", "from", "to", "date").as[Flight]
    //count Number of Flights per month
    val flightsCountPerMonth: Dataset[FlightsCount] = countNumberofFlights(flightDS)(spark)
    flightsCountPerMonth.show()

    val passengerDS: Dataset[Passengers] = passengerDataFrame.select("passengerId", "firstName", "lastName").as[Passengers]
    //compute top 100 Frequent Flyers
    val topFrequentFlyers: Dataset[TopMostFrequentFlyers] = topHundredMostFrequentFlyers(flightDS, passengerDS)(spark)
    topFrequentFlyers.show(100)

    //compute the flight routes and calculate the longest run without UK
    val longestRouteWithoutUK: Dataset[FinalResults] = computeFlightRoutes(flightDS)(spark)
    longestRouteWithoutUK.show()

    //passenger who have been more than 3 flights together
    //
    val passengersFlewTogether: Dataset[PassengersFlewTogether] = passengerFlewTogether(flightDS)(spark)
    passengersFlewTogether.show()
    // flown together testing for atleast 5 times and date range as below should work for anything
    val passengerFlownTogetherwithinDate: Dataset[PassengersFlewTogetherDateRange] = flownTogether(flightDS, 5, Date.valueOf("2017-01-01"), Date.valueOf("2017-3-01"))(spark)
    passengerFlownTogetherwithinDate.show()

    //result.show()

    spark.stop()
  }

  def flownTogether(flightDS: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date)(implicit spark: SparkSession): Dataset[PassengersFlewTogetherDateRange] = {
    import spark.implicits._

    // joining the flight Dataset and Passenger Dataset applying where clause to get only travelled between that date range
    val joinDS = flightDS.alias("f1").joinWith(flightDS.alias("f2"), $"f1.flightId" === $"f2.flightId" && $"f1.passengerId" =!= $"f2.passengerId")
      .select($"f1.passengerId".alias("passenger1Id").as[Int],
      $"f2.passengerId".alias("passenger2Id").as[Int],
      $"f1.flightId".as[Int],
      $"f1.date".as[String]).filter($"f1.date".between(from, to) && $"f2.date".between(from, to))

    //ordering the flights to avoid repeating the same passenger1Id and passenger2Id interchangeably

    val orderedFlights = joinDS.map { case (passenger1Id, passenger2Id, flightId, date) =>
      val (minId, maxId) = if (passenger1Id < passenger2Id) (passenger1Id, passenger2Id) else (passenger2Id, passenger1Id)
      (minId, maxId, flightId, date)
    }.toDF("passenger1Id", "passenger2Id", "flightId", "date").distinct()

    //getting the flight count of passengerId 1 and passengerId 2 between the date Ranges
    val passengerPairsCount = orderedFlights
      .groupBy("passenger1Id", "passenger2Id")
      .agg(count("flightId").alias("numberofFlightsTogether"),
        min("date").alias("from"),
        max("date").alias("to")
      )

    //filtering to exclude the join result of less than 3
    val frequentFlyerPairs = passengerPairsCount
      .filter($"numberofFlightsTogether" > atLeastNTimes).orderBy(desc("numberofFlightsTogether"))

    return frequentFlyerPairs.as[PassengersFlewTogetherDateRange]
  }

  def passengerFlewTogether(flightDS: Dataset[Flight])(implicit spark: SparkSession): Dataset[PassengersFlewTogether] = {
    import spark.implicits._


    val joinDS = flightDS.alias("f1").joinWith(flightDS.alias("f2"), $"f1.flightId" === $"f2.flightId" && $"f1.passengerId" =!= $"f2.passengerId").select(
      $"f1.passengerId".alias("passenger1Id").as[Int],
      $"f2.passengerId".alias("passenger2Id").as[Int],
      $"f1.flightId".as[Int]
    )
    //the below logic is implemented to avoid the same set of passengerId's interchangeably in passenger1Id and passenger2Id columns
    val orderedFlights = joinDS.map { case (passenger1Id, passenger2Id, flightId) =>
      val (minId, maxId) = if (passenger1Id < passenger2Id) (passenger1Id, passenger2Id) else (passenger2Id, passenger1Id)
      (minId, maxId, flightId)
    }.toDF("passenger1Id", "passenger2Id", "flightId").distinct()


    //getting the flight count of passengerId 1 and passengerId 2
    val passengerPairsCount = orderedFlights
      .groupBy("passenger1Id", "passenger2Id")
      .agg(count("flightId").alias("numberofFlightsTogether"))
    //filtering to exclude the join result of less than 3
    val frequentFlyerPairs = passengerPairsCount
      .filter($"numberofFlightsTogether" > 3).orderBy(desc("numberofFlightsTogether"))


    return frequentFlyerPairs.as[PassengersFlewTogether]


  }

  def computeFlightRoutes(flightDS: Dataset[Flight])(implicit spark: SparkSession): Dataset[FinalResults] = {
    import spark.implicits._

    // Flight Route is computed based on from chained as example uk->no  and grouped per passenger ID sorted by date of travel

    val groupedDS = flightDS
      .groupByKey(_.passengerId)
      .mapGroups { case (passengerId, iter) =>
        val sortedRoutes = iter.toSeq.sortBy(_.date).map(_.from).toList
        val routesList = sortedRoutes.mkString("->")
        val longestRouteWithoutUK = longestNonUkSequence(sortedRoutes)
        FinalResults(passengerId, routesList, longestRouteWithoutUK)
      }.orderBy(desc("longestRouteWithoutUK"))

    return groupedDS
  }

  //The function is being invoked from map transformation to compute the longest route without UK
  def longestNonUkSequence(countries: List[String]): Int = {
    var maxCount = 0
    var currentCount = 0

    for (country <- countries) {

      if (country == "uk") {
        maxCount = math.max(maxCount, currentCount)
        currentCount = 0
      } else {
        currentCount += 1
      }
    }

    // Check the last sequence as well
    maxCount = math.max(maxCount, currentCount)
    return maxCount
  }

  def topHundredMostFrequentFlyers(flightDS: Dataset[Flight], passengerDS: Dataset[Passengers])(spark: SparkSession): Dataset[TopMostFrequentFlyers] = {
    import spark.implicits._
    // joining the flight Dataset and passenger Dataset and retrieving the required fields
    val joinDS = flightDS.joinWith(passengerDS, flightDS.col("passengerId") === passengerDS.col("passengerId")).map {
      case (flight, passenger) => (flight.passengerId, flight.flightId, passenger.firstName, passenger.lastName)
    }

    val countFlights = joinDS.
      groupBy("_1", "_3", "_4")
      .agg(count("_2").alias("Number of Flights")).orderBy(desc("Number of Flights"))
      .limit(100)

    // countFlights.show()

    val result = countFlights.select(
      $"_1".alias("passengerId"),
      $"Number of Flights".alias("numberofFlights"),
      $"_3".alias("firstName"),
      $"_4".alias("lastName")
    ).as[TopMostFrequentFlyers]

    return result

  }

  def countNumberofFlights(flightDS: Dataset[Flight])(spark: SparkSession): Dataset[FlightsCount] = {
    import spark.implicits._

    val countNumberOfFlightsByMonth = flightDS.map(flight => {
      val month = flight.date.substring(5, 7) // Extracting '' as the month
      (month, flight.flightId)
    })


    val flightsCountByMonth = countNumberOfFlightsByMonth
      .groupBy("_1")
      .count()
      .withColumnRenamed("_1", "month")
      .withColumnRenamed("count", "numberofFlights").orderBy("month").
      as[FlightsCount]


    return flightsCountByMonth

  }

  def readCSV(spark: SparkSession, dataPath: String): DataFrame = {
    val dataFrame = {
      spark.read.option("header", true).option("inferSchema", true).csv(dataPath)
    }
    // dataFrame.show()
    return dataFrame
  }

}
