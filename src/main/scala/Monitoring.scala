import com.amazonaws.services.logs.AWSLogsClient
import com.amazonaws.services.logs.model.{InputLogEvent, PutLogEventsRequest}

object Monitoring {
  def createLog(message: String, logGroupName: String, logStreamName: String): Unit ={
    val cloudWatchLogsClient = AWSLogsClient.builder().build()


    val logEvent = new InputLogEvent()
      .withTimestamp(System.currentTimeMillis())
      .withMessage(message)

    val putLogEventsRequest = new PutLogEventsRequest()
      .withLogGroupName(logGroupName)
      .withLogStreamName(logStreamName)
      .withLogEvents(logEvent)

    val response = cloudWatchLogsClient.putLogEvents(putLogEventsRequest)
    if (response.getRejectedLogEventsInfo != null) {
      println(s"Rejected ${response.getRejectedLogEventsInfo.getTooNewLogEventStartIndex} log events.")
    }

    cloudWatchLogsClient.shutdown()
  }
}
