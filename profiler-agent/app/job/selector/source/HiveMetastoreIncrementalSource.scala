package job.selector.source

import java.time.Instant

import domain.{Asset, ChangeEvent}
import job.incremental.source.HiveClientChangeEventSource
import job.incremental.{ChangeDetectorJob, LogAddStatus}
import job.selector.source.hive.HiveMetastoreClientFactory
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import play.api.{Configuration, Logger}
import repo.{AssetSelectorDefinitionRepo, ChangeNotificationLogRepo}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}

class HiveMetastoreIncrementalSource(hiveClient: IMetaStoreClient,
                                     assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo,
                                     name: String, configuration: Configuration,
                                     changeNotificationLogRepo: ChangeNotificationLogRepo)
  extends HiveMetastoreSource(hiveClient) {

  private val savedSelectorDef = assetSelectorDefinitionRepo.findByName(name)
  private val keepRangeMs = (configuration.getLong("dpprofiler.incremental.changedetector.keeprange").getOrElse(86400L)) * 1000
  private val purgeOldLogs = (configuration.getBoolean("dpprofiler.incremental.changedetector.purge.oldlogs").getOrElse(true))
  private val changeEventSource = HiveClientChangeEventSource(configuration, changeNotificationLogRepo)
  private val logChangeStatus = ChangeDetectorJob.logChangeEvents(changeEventSource, changeNotificationLogRepo, keepRangeMs, purgeOldLogs)
  private val logger = Logger(classOf[HiveMetastoreIncrementalSource])

  override def fetchNext(): Future[Seq[Asset]] = {
    savedSelectorDef.flatMap { selectorDefinition =>

      logChangeStatus.flatMap { addstatus =>

        selectorDefinition match {
          case Some(selDef) => {
            val currTime = Instant.now().toEpochMilli
            if (selDef.sourceLastSubmitted.isDefined && selDef.sourceLastSubmitted.get >= (currTime - keepRangeMs) && addstatus == LogAddStatus.SUCCESS) {

              logger.info(s"assets were last submitted at ${selDef.sourceLastSubmitted.get} for $name")

              done.getAndSet(true)

              changeNotificationLogRepo.getAllInRange(selDef.sourceLastSubmitted, Some(currTime)).map { logs =>

                val uniqueLogs = logs.filter(t => t.eventType != ChangeEvent.DROP_TABLE.toString).map(t => (t.tableId, t.partitions)).distinct

                //construct map and combine all partitions of a table in a single Seq. An empty List in partitions of a table means change was at table level and profiler should run on full table.
                val uniqueLogsMap = uniqueLogs.groupBy(_._1).mapValues(_.map(_._2))
                val tablesWithPartitions = uniqueLogsMap.map {
                  case (tid, parts) => if (parts.exists(_ == Nil)) (tid, Nil) else (tid, parts.flatten)
                }.toSeq

                tablesWithPartitions.map {
                  case (tid, partitions) =>
                    logger.info(s"submitting asset $tid with following partitions: ")
                    partitions.foreach(t => logger.info(t.mkString))
                    val dbAndTableName = tid.split("\\.")
                    getAsset(dbAndTableName(0), dbAndTableName(1), partitions)
                }
              }

            } else {
              logger.info(s"last submitted is not set or last submitted is older than keep range or ChangeDetector execution failed. continue submitting assets for $name")
              super.fetchNext()
            }
          }
          case None => {
            logger.info("selector definition not found.")
            done.getAndSet(true)
            Future.successful(Seq())
          }
        }
      }
    }
      .recover {
        case th: Throwable =>
          logger.warn("Error occurred. Please see error message for detail.", th)
          Seq()
      }
  }
}

object HiveMetastoreIncrementalSource {
  def apply(assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo, name: String, configuration: Configuration, changeNotificationLogRepo: ChangeNotificationLogRepo) = {
    val hiveClient = HiveMetastoreClientFactory.get(configuration)
    new HiveMetastoreIncrementalSource(hiveClient, assetSelectorDefinitionRepo, name, configuration, changeNotificationLogRepo)
  }
}