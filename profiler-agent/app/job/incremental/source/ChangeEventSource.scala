package job.incremental.source

import java.time.Instant

import domain.{ChangeEvent, ChangeNotificationLog}
import job.selector.source.hive.HiveMetastoreClientFactory
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import play.api.libs.json.Json
import play.api.{Configuration, Logger}
import repo.ChangeNotificationLogRepo

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait ChangeEventSource {
  def nextChangeLogs: Future[Seq[ChangeNotificationLog]]
}

class HiveClientChangeEventSource(hiveClient: IMetaStoreClient, changeNotificationLogRepo: ChangeNotificationLogRepo) extends ChangeEventSource{

  val logger = Logger(classOf[HiveClientChangeEventSource])

  override def nextChangeLogs: Future[Seq[ChangeNotificationLog]]= {

    getLastEventId(hiveClient, changeNotificationLogRepo).map {

      case lastEventId: Long =>

        logger.debug(s"lastEventId is = $lastEventId")
        val maxEvent = 50000000 - 1 //50000000 is max value, after that getNextNotification throws exception

        Try(hiveClient.getNextNotification(lastEventId, maxEvent, null)) match {

          case Success(notificationEventResponse) =>

            val changeNotifications = notificationEventResponse.getEvents.asScala.filter(event => ChangeEvent.exists(event.getEventType)).map { event =>
              ChangeEvent.withName(event.getEventType) match {

                case ChangeEvent.ADD_PARTITION | ChangeEvent.DROP_PARTITION =>
                  val json = Json.parse(event.getMessage)
                  val partitions = (json \ "partitions").validate[Seq[Map[String, String]]].getOrElse(Nil)
                  ChangeNotificationLog(eventId = event.getEventId, tableId = s"${event.getDbName}.${event.getTableName}", eventTime = event.getEventTime, eventType = event.getEventType, partitions = partitions, recordedTime = Instant.now().toEpochMilli())

                case ChangeEvent.ALTER_PARTITION =>
                  val json = Json.parse(event.getMessage)
                  val partitions = (json \ "keyValues").validate[Map[String, String]].map(entry => Seq(entry)).getOrElse(Nil)
                  ChangeNotificationLog(eventId = event.getEventId, tableId = s"${event.getDbName}.${event.getTableName}", eventTime = event.getEventTime, eventType = event.getEventType, partitions = partitions, recordedTime = Instant.now().toEpochMilli())

                case ChangeEvent.ALTER_TABLE | ChangeEvent.CREATE_TABLE | ChangeEvent.DROP_TABLE =>
                  ChangeNotificationLog(eventId = event.getEventId, tableId = s"${event.getDbName}.${event.getTableName}", eventTime = event.getEventTime, eventType = event.getEventType, recordedTime = Instant.now().toEpochMilli())
              }
            }
            changeNotifications
          case Failure(e) =>
            logger.error("get next notification failed", e)
            Nil
        }
      case th: Throwable =>
        logger.error("Failed to get lastEventId", th)
        Nil
    }
  }

  //first fetch lastEventid from db, if its None in DB then use hiveClient.getCurrentNotificationEventId.getEventId
  private def getLastEventId(hiveClient: IMetaStoreClient, changeNotificationLogRepo: ChangeNotificationLogRepo) = {

    changeNotificationLogRepo.findLastSavedEventId.map {
      case Some(id) => id
      case None => Try(hiveClient.getCurrentNotificationEventId) match {
        case Failure(e) =>
          logger.warn("failed to get current notification id from hive client. No event found.", e)
          e
        case Success(t) =>
          // save the lastEventId in db. This step will happen only when change_notification_log table is empty
          val eType = ChangeEvent.DROP_TABLE.toString
          val lastEventId = t.getEventId - 1
          val dummy = ChangeNotificationLog(eventId = lastEventId, tableId = "dummyDB.dummyTable", eventTime = 1, eventType = eType, recordedTime = Instant.now().toEpochMilli())
          changeNotificationLogRepo.add(Seq(dummy))
          lastEventId
      }
    }
      .recover {
        case th: Throwable =>
          logger.error("Failed to get last event id. DB exception", th)
          th
      }
  }
}

object HiveClientChangeEventSource {
  def apply(configuration: Configuration, changeNotificationLogRepo: ChangeNotificationLogRepo): HiveClientChangeEventSource = {
    val hiveClient = HiveMetastoreClientFactory.get(configuration)
    new HiveClientChangeEventSource(hiveClient, changeNotificationLogRepo)
  }
}