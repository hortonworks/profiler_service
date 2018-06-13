/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */

package job.interactive

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import domain._
import javax.inject.{Inject, Singleton}
import job.WSResponseMapper
import livy.helper.{GenericSupervisor, SessionConfigReader}
import livy.interactor.LivyInteractor
import livy.session.messages.SessionProviderInteractions._
import livy.session.models.LivyRequests
import livy.session.models.LivyRequests.SubmitStatement
import livy.session.models.LivyResponses.{ErrorStatementOutput, StateOfStatement, SuccessfulStatementOutPut}
import livy.session.provider.SessionProvider
import livy.session.{SessionRepository, SessionRepositoryCleaner}
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.WSClient
import play.api.{Configuration, Logger}

import scala.concurrent.Future
import scala.language.postfixOps

@Singleton
class LivyInteractiveRunner @Inject()(configuration: Configuration, ws: WSClient, actorSystem: ActorSystem, interactor: LivyInteractor) extends WSResponseMapper {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val metricPath = configuration.getString("clusterconfigs.assetMetricsPath").getOrElse("/user/dpprofiler/dwh")


  val datasetMetricName: String = configuration.getString("dataset.metric.name").getOrElse("dataset")


  val metaStoreMetricName = "hivemetastorestats"

  val sensitivePartitionedMetricName: String = configuration.getString("dpprofiler.sensitivepartitioned.metric.name").getOrElse("hivesensitivitypartitioned")
  val sensitiveMetricName: String = configuration.getString("dpprofiler.senstitive.metric.name").getOrElse("hivesensitivity")


  import scala.concurrent.duration._


  val readSessionRepository: ActorRef = actorSystem.actorOf(Props(
    new GenericSupervisor(Props(new SessionRepository(configuration, "read")), "read-SessionRepository")),
    "read-SessionRepository-Supervisor"
  )


  val writeSessionRepository: ActorRef = actorSystem.actorOf(Props(
    new GenericSupervisor(Props(new SessionRepository(configuration, "write")), "write-SessionRepository")),
    "write-SessionRepository-Supervisor"
  )


  val readTimeoutInSeconds: Int =
    configuration.getInt(s"livy.session.config.read.timeoutInSeconds").getOrElse(90)

  val writeTimeoutInSeconds: Int =
    configuration.getInt(s"livy.session.config.write.timeoutInSeconds").getOrElse(90)

  val readSessionConfig: LivyRequests.CreateSession =
    SessionConfigReader.readSessionConfig(configuration, "read")

  val writeSessionConfig: LivyRequests.CreateSession =
    SessionConfigReader.readSessionConfig(configuration, "write")


  val readSessionProvider: ActorRef = actorSystem.actorOf(Props(
    new GenericSupervisor(Props(
      new SessionProvider(interactor, readSessionRepository, configuration, readSessionConfig)
    ), "SessionProvider-Read")),
    "SessionProvider-Read-Supervisor"
  )

  val writeSessionProvider: ActorRef = actorSystem.actorOf(Props(
    new GenericSupervisor(Props(
      new SessionProvider(interactor, writeSessionRepository, configuration, writeSessionConfig)
    ), "SessionProvider-Write")),
    "SessionProvider-Write-Supervisor"
  )

  SessionRepositoryCleaner.clearUnconsumedSessionAfter(interactor, readSessionRepository, 1 minutes, actorSystem.scheduler)

  SessionRepositoryCleaner.clearUnconsumedSessionAfter(interactor, writeSessionRepository, 1 minutes, actorSystem.scheduler)


  def runStatement(sessionProvider: ActorRef, statement: String, livyQueryTimeout: Int): Future[JsValue] = {
    val sessionId = ask(sessionProvider, GetSessionId)(Timeout(5 seconds))
    sessionId.flatMap {
      case SessionNotAvailable => Future.failed(
        new Exception("Livy session is unvailable. Try after some time")
      )
      case InvalidStateRestartRequired => Future.failed(
        new Exception("Inconsistent state. Profiler agent restart required")
      )
      case SessionId(id) =>
        sessionProvider ! RequestSubmissionIndicator(id)
        val eventualResult = Future firstCompletedOf Seq(interactor.submitStatement(id, SubmitStatement("spark", statement)).
          flatMap(
            statementId => pollForResults(id, statementId)
          ),
          akka.pattern.after(duration = livyQueryTimeout seconds,
            using = actorSystem.scheduler)(Future.failed(new LivyQueryTimeoutException(livyQueryTimeout seconds)))
        )
        eventualResult onSuccess {
          case _ => sessionProvider ! SuccessfulRunIndication(id)
        }
        eventualResult onFailure {
          case error: LivySessionNotRunningException =>
            Logger.error("Session dead trying to restart,", error)
            sessionProvider ! SessionYouGaveIsDead(id)
          case _ => sessionProvider ! ErrorEncounteredIndication(id)
        }
        eventualResult
    }
  }

  def pollForResults(sessionId: Int, statementId: Int): Future[JsValue] = {
    interactor.getStatement(sessionId, statementId).flatMap(
      statement => {
        if (statement.state == StateOfStatement.available) {
          if (statement.output.isDefined) {
            val output = statement.output.get
            output match {
              case successoutput: SuccessfulStatementOutPut =>
                Future.successful(Json.parse((successoutput.data \ "text/plain").as[String]))
              case error: ErrorStatementOutput =>
                Future.failed(new Exception(s"Execution of statement failed, cause : ${error.evalue} , stacktrace:" +
                  s"${error.traceback.mkString(";")}"))
            }
          }
          else {
            Future.failed(new Exception(s"Invalid response from livy $statement"))
          }
        }
        else if (Set(StateOfStatement.error, StateOfStatement.cancelled,
          StateOfStatement.cancelling).contains(statement.state)) {
          Future.failed(new Exception(s"Statement is either cancelled or" +
            s" faced an unexpected error, $statement"))
        }
        else {
          akka.pattern.after(200.milliseconds, actorSystem.scheduler)(pollForResults(sessionId, statementId))
        }
      }
    )
  }


  def encloseInLocalScope(interpretableCode: String): String = {
    s"""|
        |{
        | $interpretableCode
        | }
     """.stripMargin
  }


  def refreshTable(tableName: String): Future[JsValue] = {
    Logger.debug(s"Refreshing table $tableName")
    val statement =
      s"""
         |if(spark.sqlContext.tableNames().contains("$tableName")){
         |  spark.sqlContext.sql(s"refresh table $tableName")
         |  spark.sqlContext.sql(s"CACHE TABLE $tableName")
         |}
         |println("hello")
      """.stripMargin

    runStatement(readSessionProvider, statement, readTimeoutInSeconds)
  }

  def refreshAllTables(): Future[JsValue] = {
    val statement =
      """
        |spark.sqlContext.tableNames().foreach {
        |  table =>
        |     {
        |    spark.sqlContext.sql(s"refresh table $table")
        |    spark.sqlContext.sql(s"CACHE TABLE $table")
        |     }
        |}
        |
        |val tables = spark.sqlContext.tableNames().map(t => "\"" + t +  "\"").mkString("[",",","]")
        |println(tables)
      """.stripMargin

    runStatement(readSessionProvider, statement, readTimeoutInSeconds)
  }


  def postStatement(query: InteractiveQuery): Future[JsValue] = {
    runStatement(readSessionProvider, createStatement(query), readTimeoutInSeconds)
  }

  def postInteractiveQueryAndGetResult(query: InteractiveQuery): Future[JsValue] = {
    postStatement(query)
  }

  def cacheMetrics(metrics: CacheMetrics): Future[JsValue] = {
    val query = getTableAndCache(metrics)
    val statusStatement = "\n println(\"true\")"
    runStatement(readSessionProvider, query + statusStatement, readTimeoutInSeconds)
  }

  def createStatement(query: InteractiveQuery) = {
    val tableCreatStatement = getTableCreateStatement(query)
    val statement = tableCreatStatement + "\n" +
      s"""
         |val sqldf = spark.sqlContext.sql("${query.sql}")
         |val json = sqldf.toJSON.collect().mkString("[",",","]")
         |println(json)
       """.stripMargin
    statement
  }


  private def getTableAndCache(metrics: CacheMetrics) = {
    metrics.metrics.map(metric => {
      val tablePath = s"$metricPath/${metric.metric}/${metric.aggType.toString.toLowerCase}"
      val tableName = getTable(metric)

      s"""
         |  val df = spark.read.parquet("$tablePath")
         |  df.createOrReplaceTempView("$tableName")
         |  df.unpersist()
         |  df.cache()
    """.stripMargin
    }).mkString("\n")

  }

  private def getTableCreateStatement(query: InteractiveQuery) = {

    query.metrics.map(metric => {
      val tablePath = s"$metricPath/${metric.metric}/${metric.aggType.toString.toLowerCase}"
      val tableName = getTable(metric)

      s"""
         |if(!spark.sqlContext.tableNames().contains("$tableName")) {
         |  val df = spark.read.parquet("$tablePath")
         |  df.createTempView("$tableName")
         |  spark.sqlContext.sql("CACHE TABLE $tableName")
         |}
    """.stripMargin
    }).mkString("\n")


  }

  private def getTable(metric: Metric) = s"${metric.metric}_${metric.aggType.toString.toLowerCase}"

  def saveDatasetMapping(datasetAndAssetIds: DatasetAndAssetIds): Future[JsValue] = {
    val eventualSaveStatus = runStatement(writeSessionProvider,
      datasetMappingStatement(datasetAndAssetIds), writeTimeoutInSeconds)
    eventualSaveStatus onComplete {
      _ =>
        val tableName = getTable(Metric(datasetMetricName, AggregationType.Snapshot))
        refreshTable(tableName)
    }
    eventualSaveStatus
  }

  def saveTags(tableResults: Seq[TableResult], dbName: String, tableName: String) = {
    val eventualTagSave = runStatement(writeSessionProvider,
      tagsSaveStatement(tableResults, dbName, tableName), writeTimeoutInSeconds)
    eventualTagSave onComplete {
      _ =>
        val partitionedTable = getTable(Metric(sensitivePartitionedMetricName, AggregationType.Snapshot))
        val aggregateTable = getTable(Metric(sensitiveMetricName, AggregationType.Snapshot))
        refreshTable(partitionedTable)
        refreshTable(aggregateTable)
    }
    eventualTagSave
  }

  private def tagsSaveStatement(tableResults: Seq[TableResult], dbName: String, tableName: String): String = {

    val partitionedPath = s"$metricPath/$sensitivePartitionedMetricName/${AggregationType.Snapshot.toString.toLowerCase}"
    val tablePartitionedPath = s"$partitionedPath/database=$dbName/table=$tableName"
    val aggregatePath = s"$metricPath/$sensitiveMetricName/${AggregationType.Snapshot.toString.toLowerCase}"
    val partitionedTable = getTable(Metric(sensitivePartitionedMetricName, AggregationType.Snapshot))
    val aggregateTable = getTable(Metric(sensitiveMetricName, AggregationType.Snapshot))

    val propSeparator = "@#@"
    val entitySeparator = "@@##@@"

    val tableResultsStringList = tableResults.map { r =>
      s"${r.database}$propSeparator${r.table}$propSeparator${r.column}$propSeparator${r.label}$propSeparator${r.percentMatch}$propSeparator${r.status}"
    }

    val tableResultsString = Seq(tableResultsStringList.map(e => s"$e").mkString(entitySeparator)).map(e => s""""$e"""").head

    s"""
       |  import org.apache.spark.sql.SaveMode
       |  import org.apache.spark.rdd.RDD
       |  import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
       |  import org.apache.spark.sql.Row
       |
       |  import spark.implicits._
       |  val fields = List(
       |  StructField("database", StringType, nullable = true),
       |  StructField("table", StringType, nullable = true),
       |  StructField("column", StringType, nullable = true),
       |  StructField("label", StringType, nullable = true),
       |  StructField("percentMatch", DoubleType, nullable = false),
       |  StructField("status", StringType, nullable = true)
       |  )
       |
       |  val schema = StructType(fields)
       |
       |  val tableResults = $tableResultsString.split("$entitySeparator").map { str =>
       |    val tb = str.split("$propSeparator")
       |    Row.fromSeq(List(tb(0), tb(1), tb(2), tb(3), 100.0D, tb(5)))
       |  }.toSeq
       |
       |  val rdd:RDD[Row] = spark.sparkContext.makeRDD(tableResults)
       |
       |  val tableResultDF  = spark.createDataFrame(rdd,schema)
       |
       |  tableResultDF.write.mode(SaveMode.Overwrite).parquet("$tablePartitionedPath")
       |
       |  if(!spark.sqlContext.tableNames().contains("$partitionedTable")){
       |    val dfT = spark.read.parquet("$partitionedPath")
       |    dfT.createTempView("$partitionedTable")
       |  } else {
       |    spark.sqlContext.sql(s"refresh table $partitionedTable")
       |  }
       |  spark.sqlContext.sql("CACHE TABLE $partitionedTable")
       |
       |  spark.read.parquet("$partitionedPath").write.mode(SaveMode.Overwrite).parquet("$aggregatePath")
       |
       |  if(!spark.sqlContext.tableNames().contains("$aggregateTable")){
       |    val dfT = spark.read.parquet("$aggregatePath")
       |    dfT.createTempView("$aggregateTable")
       |  } else {
       |    spark.sqlContext.sql(s"refresh table $aggregateTable")
       |  }
       |  spark.sqlContext.sql("CACHE TABLE $aggregateTable")
       |
       |   val sqldf = spark.sqlContext.sql("select count(*) as count from $partitionedTable where database='$dbName' and table='$tableName'")
       |   val json = sqldf.toJSON.collect().mkString("[",",","]")
       |   println(json)
       |
     """.stripMargin

  }

  def getExistingProfiledAssetCount(profilerName: String): Future[JsValue] = {
    getExistingProfiledAssetCountStmnt(profilerName).map(runStatement(readSessionProvider, _, readTimeoutInSeconds))
      .getOrElse(Future.failed(new Exception(s"Unsupported profiler $profilerName")))
  }

  private def getExistingProfiledAssetCountStmnt(profilerName: String): Option[String] = {
    profilerName match {
      case "sensitiveinfo" => Some(getExistingSensitiveAssetCountStmnt)
      case _ =>
        Logger.error("operation not supported for this profiler")
        None
    }
  }

  private def getExistingSensitiveAssetCountStmnt(): String = {

    val sensitivePartitionedPath = s"$metricPath/$sensitivePartitionedMetricName/${AggregationType.Snapshot.toString.toLowerCase}"
    val metaStorePath = s"$metricPath/$metaStoreMetricName/${AggregationType.Snapshot.toString.toLowerCase}"

    s"""
       |import org.apache.spark.rdd.RDD
       |import org.apache.spark.sql.Row
       |import org.apache.spark.sql.execution.datasources.FilePartition
       |import org.apache.spark.sql.types.{StringType, StructField, StructType}
       |
       |val configuration = spark.sparkContext.hadoopConfiguration
       |
       |val fields = List(StructField("table", StringType, nullable = true))
       |
       |val schema = StructType(fields)
       |
       |val tablesProfiled = spark.read.parquet("$sensitivePartitionedPath").rdd.partitions.map(
       |
       |  _.asInstanceOf[FilePartition].files.map(f => f.partitionValues).distinct
       |
       |).flatMap(
       |
       |  p => {
       |
       |    p.map(row => {
       |
       |      val table = row.getString(0)+"."+row.getString(1)
       |      List(table)
       |
       |    })
       |
       |  }
       |
       |).distinct.map(Row.fromSeq)
       |
       |val rdd: RDD[Row] = spark.sparkContext.makeRDD(tablesProfiled)
       |
       |
       |val tablesProfiledDf = spark.createDataFrame(rdd, schema)
       |
       |val metadf = spark.read.parquet("$metaStorePath")
       |
       |if(!metadf.storageLevel.useMemory){
       |  metadf.cache
       |}
       |
       |val joineddf = metadf.join(tablesProfiledDf, "table")
       |
       |println(joineddf.count)
     """.stripMargin
  }

  private def datasetMappingStatement(datasetAndAssetIds: DatasetAndAssetIds) = {

    val assetIdsString = datasetAndAssetIds.assetIds.map(e => s""""$e"""").mkString(",")
    val tablePath = s"$metricPath/$datasetMetricName/${AggregationType.Snapshot.toString.toLowerCase}"
    val datasetName = datasetAndAssetIds.datasetName
    val savePath = s"$tablePath/dataset=$datasetName"
    val tableName = getTable(Metric(datasetMetricName, AggregationType.Snapshot))

    s"""
       |  import org.apache.spark.sql.SaveMode
       |
       |  val list : List[String] = List($assetIdsString)
       |
       |  val df = spark.createDataFrame(list.map(Tuple1(_))).toDF("assetid")
       |  df.write.mode(SaveMode.Overwrite).parquet("$savePath")
       |
       |  if(!spark.sqlContext.tableNames().contains("$tableName")){
       |    val dfT = spark.read.parquet("$tablePath")
       |    dfT.createTempView("$tableName")
       |  } else {
       |    spark.sqlContext.sql(s"refresh table $tableName")
       |  }
       |   spark.sqlContext.sql("CACHE TABLE $tableName")
       |
       |   val sqldf = spark.sqlContext.sql("select count(assetid) as count from $tableName where dataset='$datasetName'")
       |   val json = sqldf.toJSON.collect().mkString("[",",","]")
       |   println(json)
       |
     """.stripMargin
  }

}
