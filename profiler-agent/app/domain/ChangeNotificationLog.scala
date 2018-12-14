package domain

case class ChangeNotificationLog(id: Option[Long] = None, eventId: Long, tableId: String, eventTime: Int, eventType: String, partitions: Seq[Map[String,String]] = Nil, message: Option[String] = None, recordedTime: Long)

object ChangeEvent extends Enumeration {
  type ChangeEvent = Value
  val ADD_PARTITION, ALTER_PARTITION, ALTER_TABLE, CREATE_TABLE, DROP_PARTITION, DROP_TABLE = Value

  def exists(s: String): Boolean = values.exists(_.toString == s)
}
