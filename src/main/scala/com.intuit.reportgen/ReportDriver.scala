package com.intuit.reportgen


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * User: jeyanthan
  * Date: 2019-03-15
  *
  * Description:
  *   The spark driver class to operate on customer and sales data in hdfs to produce sales report with various granularities.
  *   It will be submitted to yarn for execution. Yarn will schedule, manage the resources and run the job.
  *
  *   Eg:
  *   spark-submit --verbose --master yarn --deploy-mode cluster  --executor-memory 4G --num-executors 50  ~/workspace/ReportGen/ReportGen-1.0.jar
  */

class ReportDriver {

  def main(args: Array[String]) = {

    /**
      * type alias for tuples. Makes the series of tuples more readable
      */

    type CustomerState = (String, String)
    type GroupByCustomer = (String, Int)
    type HourSales = (String, Int)
    type DaySales = (String, HourSales)
    type MonthSales = (String, DaySales)
    type YearSales = (String, MonthSales)
    type CustomerSales = (String, YearSales)

    /**
      * To Run locally against the hdfs
      *   1. Set app name and master to local
      */
    //val conf:SparkConf = new SparkConf().setAppName("test").setMaster("local")

    /**
      * To run on yarn cluster
      *   1. Allow yarn to set the app name and master
      */
    val conf:SparkConf = new SparkConf()
    val sc:SparkContext = new SparkContext(conf)


    /**
      * Definition:
      *   RDD - Abstraction spark provides which is a collection of elements partitioned across the nodes of the
      *         cluster that can be operated on in parallel
      *  Description:
      *   Read the hdfs cluster to return a RDD of Strings
      *
      */

    val customerRDD = sc.textFile("hdfs://localhost:8020/user/jeyanthan/intuit/input/customer")
    val salesRDD = sc.textFile("hdfs://localhost:8020/user/jeyanthan/intuit/input/sales")

    /**
      * Description:
      *   Returns a new RDD which is a kay/value pair of Customer Id & State
      * Sample Output:
      *   (123,CA)
      *   (456,AK)
      *   (789,AL)
      *   (101112,OR)
      *
      */

    val keyCustStateRDD:RDD[CustomerState] = customerRDD.map(line => {
      val splits = line.split("\\#",4)
      (splits(0), splits(2).substring(splits(2).length-2))
    })

    //keyCustStateRDD.collect foreach println

    /**
      *  Join is one of the most expensive operations we will be doing here. The below steps are to combine and reduce to
      *  only the customers that we need before we join
      *
      *  Assumption:
      *     Generally the customers that are signed up will be exponentially higher than the regular customers who transact.
      *     So aggregating and getting a list of unique customer ids we are interested in will result
      *     in a smaller set of data to join
      */

    /**
      * Description:
      *   Returns a new RDD which is a series of tuples that represent the various granularities
      *   for generating the sales report by customer
      *
      * Sample Output:
      *   (123,(2016,(2,(1,(0,123456)))))
      *   (789,(2017,(8,(1,(2,123457)))))
      *   (789,(2017,(8,(1,(2,123457)))))
      *   (789,(2017,(8,(1,(2,123457)))))
      *   (123,(2016,(2,(1,(0,123456)))))
      *   (123,(2019,(3,(17,(12,123456)))))
      *   (123,(2016,(2,(1,(11,123456)))))
      *   (456,(2016,(8,(1,(3,123458)))))
      *   (789,(2016,(8,(1,(4,123459)))))
      *   (789,(2016,(8,(1,(4,123459)))))
      *   (123,(2019,(3,(17,(5,123456)))))
      */

    val customerSalesRDD:RDD[CustomerSales] = salesRDD.map(f = line => {
      val splits = line.split("\\#", 3)
      val ts = splits(0).toLong * 1000L
      val date = new DateTime(ts)

      (splits(1), (date.getYear.toString, (date.getMonthOfYear.toString, (date.getDayOfMonth.toString,
                    (date.getHourOfDay.toString, Integer.parseInt(splits(2)))))))
    })

    //customerSalesRDD.collect foreach println

    /**
      * Description:
      *   Returns a new RDD which is a tuple of customers with keys that we will use in the step below to get unique
      *   customer ids that we are interested in before the join
      *
      * Sample Output:
      * (123#2016######,123456)
      * (789#2017######,123457)
      * (789#2017######,123457)
      * (789#2017######,123457)
      * (123#2016######,123456)
      * (123#2019######,123456)
      * (123#2016######,123456)
      * (456#2016######,123458)
      * (789#2016######,123459)
      * (789#2016######,123459)
      * (123#2019######,123456)
      *
      */

    val yearSalesByUserRDD = customerSalesRDD.keyBy(t => (t._1.concat("#").concat(t._2._1).concat("#")
      .concat("#").concat("#")
      .concat("#").concat("#").concat("#"), t._2._2._2._2._2)).keys

    //yearSalesByUserRDD.collect foreach println

    /**
      * Description:
      *   To reduce the shuffling across nodes while we try to group by customers we create a combine function that is used
      *   to combine within the same node and a merger function that is used to group customers across nodes
      *
      * Sample Output: (Note: We are only interested in the key of the tuple. we dont need the value part of the tuple.)
      * (123,123#2016######)
      * (789,789#2017######)
      * (456,456#2016######)
      *
      */

    val createCustIdCombiner = (customerGroups: GroupByCustomer) => customerGroups._1

    val custIdCombiner = (collector: String, customerGroups: GroupByCustomer) => {
      (customerGroups._1)
    }

    val custIdMerger = (collector1: String, collector2: String) => {
          collector1
    }

    val uniqueCustIds  = yearSalesByUserRDD.keyBy(s => {
      val splits = s._1.split("\\#",7)
      splits(0)
    }).combineByKey(createCustIdCombiner, custIdCombiner, custIdMerger)

    //uniqueCustIds.collect foreach println

    /**
      * Description:
      *   We are now ready to join with customer states RDD.
      *
      * Sample Output:
      * (123,123#2016######)
      * (789,789#2017######)
      * (456,456#2016######)
      *
      */

    val custInfoRDD:RDD[(String, (String, String))] = keyCustStateRDD.join(uniqueCustIds)

    //custInfoRDD.collect foreach println

    /**
      * Description:
      *   1. We are now caching the customer info RDD in memory which is a key/value pair of customer and state info.
      *   2. Collect the cached RDD as a map so we can use it for a quick lookup
      *
      * Sample Output:
      * (123,(CA,123#2016######))
      * (789,(AL,789#2017######))
      * (456,(AK,456#2016######))
      *
      */
    val custStates = custInfoRDD.cache().collectAsMap()

    //custInfoRDD.collect foreach println

    /**
      * Description:
      *   1. Generating a set of keys to generate the hour based report by Customer state
      *
      * Sample Output:
      *   (CA#2016#2#1#0#,123456)
      *   (AL#2017#8#1#2#,123457)
      *   (AL#2017#8#1#2#,123457)
      *   (AL#2017#8#1#2#,123457)
      *   (CA#2016#2#1#0#,123456)
      *   (CA#2019#3#17#12#,123456)
      *   (CA#2016#2#1#11#,123456)
      *   (AK#2016#8#1#3#,123458)
      *   (AL#2016#8#1#4#,123459)
      *   (AL#2016#8#1#4#,123459)
      *   (CA#2019#3#17#5#,123456)
      */
    val hourSalesRDD = customerSalesRDD.keyBy(t => (custStates.get(t._1).headOption.get._1.concat("#")
      .concat(t._2._1).concat("#")
      .concat(t._2._2._1).concat("#")
      .concat(t._2._2._2._1).concat("#")
      .concat(t._2._2._2._2._1).concat("#"), t._2._2._2._2._2)).keys

    //hourSalesRDD.collect foreach println

    /**
      * Description:
      *   1. Generating a set of keys to generate the day based report by Customer state
      *
      * Sample Output:
      *   (CA#2016#2#1##,123456)
      *   (AL#2017#8#1##,123457)
      *   (AL#2017#8#1##,123457)
      *   (AL#2017#8#1##,123457)
      *   (CA#2016#2#1##,123456)
      *   (CA#2019#3#17##,123456)
      *   (CA#2016#2#1##,123456)
      *   (AK#2016#8#1##,123458)
      *   (AL#2016#8#1##,123459)
      *   (AL#2016#8#1##,123459)
      *   (CA#2019#3#17##,123456)
      *
      */
    val daySalesRDD = customerSalesRDD.keyBy(t => (custStates.get(t._1).headOption.get._1.concat("#")
      .concat(t._2._1).concat("#")
      .concat(t._2._2._1).concat("#")
      .concat(t._2._2._2._1).concat("#")
      .concat("#"), t._2._2._2._2._2)).keys

    //daySalesRDD.collect foreach println

    /**
      * Description:
      *   1. Generating a set of keys to generate the month based report by Customer state
      *
      * Sample Output:
      *   (CA#2016#2###,123456)
      *   (AL#2017#8###,123457)
      *   (AL#2017#8###,123457)
      *   (AL#2017#8###,123457)
      *   (CA#2016#2###,123456)
      *   (CA#2019#3###,123456)
      *   (CA#2016#2###,123456)
      *   (AK#2016#8###,123458)
      *   (AL#2016#8###,123459)
      *   (AL#2016#8###,123459)
      *   (CA#2019#3###,123456)
      *
      */
    val monthSalesRDD = customerSalesRDD.keyBy(t => (custStates.get(t._1).headOption.get._1.concat("#")
      .concat(t._2._1).concat("#")
      .concat(t._2._2._1).concat("#")
      .concat("#").concat("#"), t._2._2._2._2._2)).keys

    //monthSalesRDD.collect foreach println

    /**
      * Description:
      *   1. Generating a set of keys to generate the year based report by Customer state
      *
      * Sample Output:
      *   (CA#2016#####,123456)
      *   (AL#2017#####,123457)
      *   (AL#2017#####,123457)
      *   (AL#2017#####,123457)
      *   (CA#2016#####,123456)
      *   (CA#2019#####,123456)
      *   (CA#2016#####,123456)
      *   (AK#2016#####,123458)
      *   (AL#2016#####,123459)
      *   (AL#2016#####,123459)
      *   (CA#2019#####,123456)
      *
      */
    val yearSalesRDD = customerSalesRDD.keyBy(t => (custStates.get(t._1).headOption.get._1
      .concat("#").concat(t._2._1).concat("#")
      .concat("#").concat("#")
      .concat("#").concat("#"), t._2._2._2._2._2)).keys

    //yearSalesRDD.collect foreach println

    /**
      * Description:
      *   1. Generating a set of keys to generate the cumulative state based report
      *
      * Sample Output:
      *   (CA######,123456)
      *   (AL#2017#####,123457)
      *   (AL#2017#####,123457)
      *   (AL#2017#####,123457)
      *   (CA#2016#####,123456)
      *   (CA#2019#####,123456)
      *   (CA#2016#####,123456)
      *   (AK#2016#####,123458)
      *   (AL#2016#####,123459)
      *   (AL#2016#####,123459)
      *   (CA#2019#####,123456)
      */
    val stateSalesReportRDD = customerSalesRDD.keyBy(t => (custStates.get(t._1).headOption.get._1.concat("#")
      .concat("#").concat("#")
      .concat("#").concat("#"), t._2._2._2._2._2)).keys

    //stateSalesReportRDD.collect foreach println


    /**
      * Description:
      *   1. Aggregating by key passing in a sequential function to aggregate within the same node and a combine function to
      *   aggregate across nodes.
      *
      * Sample Output:
      *
      * By State:
      *   (AL#####,617289)
      *   (AK#####,123458)
      *   (CA#####,617280)
      *
      * By State/Year
      *   (AL#2016#####,246918)
      *   (CA#2019#####,246912)
      *   (AL#2017#####,370371)
      *   (CA#2016#####,370368)
      *   (AK#2016#####,123458)
      *
      * By State/Year/Month
      *   (AL#2017#8###,370371)
      *   (CA#2016#2###,370368)
      *   (CA#2019#3###,246912)
      *   (AK#2016#8###,123458)
      *   (AL#2016#8###,246918)
      *
      * By State/Year/Month/Day
      *   (AL#2016#8#1##,246918)
      *   (CA#2019#3#17##,246912)
      *   (AL#2017#8#1##,370371)
      *   (CA#2016#2#1##,370368)
      *   (AK#2016#8#1##,123458)
      *
      * By State/Year/Month/Day
      *   (AL#2016#8#1#4#,246918)
      *   (AK#2016#8#1#3#,123458)
      *   (AL#2017#8#1#2#,370371)
      *   (CA#2019#3#17#12#,123456)
      *   (CA#2016#2#1#11#,123456)
      *   (CA#2019#3#17#5#,123456)
      *   (CA#2016#2#1#0#,246912)
      */

    val seqOp = (accumulator: (Int), element: (Int)) =>{
      accumulator+element
    }

    val combOp = (accumulator1: (Int), accumulator2: (Int)) => {
      accumulator1+accumulator2
    }

    val zeroVal = (0)
    val hourSalesReport = hourSalesRDD.map(t => (t._1, t._2)).aggregateByKey(zeroVal)(seqOp, combOp)
    val daySalesReport = daySalesRDD.map(t => (t._1, t._2)).aggregateByKey(zeroVal)(seqOp, combOp)
    val monthSalesReport = monthSalesRDD.map(t => (t._1, t._2)).aggregateByKey(zeroVal)(seqOp, combOp)
    val yearSalesReport = yearSalesRDD.map(t => (t._1, t._2)).aggregateByKey(zeroVal)(seqOp, combOp)
    val stateTotalSalesReport = stateSalesReportRDD.map(t => (t._1, t._2)).aggregateByKey(zeroVal)(seqOp, combOp)

//    hourSalesReport.collect foreach println
//    daySalesReport.collect foreach println
//    monthSalesReport.collect foreach println
//    yearSalesReport.collect foreach println
//    stateTotalSalesReport.collect foreach println

    val header: RDD[String] = sc.parallelize(Array("state#year#month#day#hour#sales"))

    val sortedOutput = (hourSalesReport.map(x => x._1 + x._2))
      .union(daySalesReport.map(x => x._1 + x._2))
      .union(monthSalesReport.map(x => x._1 + x._2))
      .union(yearSalesReport.map(x => x._1 + x._2))
      .union(stateTotalSalesReport.map(x => x._1 + x._2)).sortBy[String]({string => string})

      header.union(sortedOutput).saveAsTextFile("hdfs://localhost:8020/user/jeyanthan/intuit/output")

    /**
      *
      * Final Output:
      *   state#year#month#day#hour#sales
      *   AK#####123458
      *   AK#2016#####123458
      *   AK#2016#8###123458
      *   AK#2016#8#1##123458
      *   AK#2016#8#1#3#123458
      *   AL#####617289
      *   AL#2016#####246918
      *   AL#2016#8###246918
      *   AL#2016#8#1##246918
      *   AL#2016#8#1#4#246918
      *   AL#2017#####370371
      *   AL#2017#8###370371
      *   AL#2017#8#1##370371
      *   AL#2017#8#1#2#370371
      *   CA#####617280
      *   CA#2016#####370368
      *   CA#2016#2###370368
      *   CA#2016#2#1##370368
      *   CA#2016#2#1#0#246912
      *   CA#2016#2#1#11#123456
      *   CA#2019#####246912
      *   CA#2019#3###246912
      *   CA#2019#3#17##246912
      *   CA#2019#3#17#12#123456
      *   CA#2019#3#17#5#123456
      *
      */

  }

}
