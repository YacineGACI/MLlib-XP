package fr.insa.distml.experiments

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.scheduler._
import java.io.FileWriter


class CustomListener(file: FileWriter) extends SparkListener{
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

        try{
             val line = s"${stageCompleted.stageInfo.taskMetrics.diskBytesSpilled}," + 
                    s"${stageCompleted.stageInfo.taskMetrics.executorCpuTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.executorDeserializeCpuTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.executorDeserializeTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.executorRunTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.inputMetrics.bytesRead}," +
                    s"${stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead}," +
                    s"${stageCompleted.stageInfo.taskMetrics.jvmGCTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.memoryBytesSpilled}," +
                    s"${stageCompleted.stageInfo.taskMetrics.outputMetrics.bytesWritten}," +
                    s"${stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten}," +
                    s"${stageCompleted.stageInfo.taskMetrics.peakExecutionMemory}," +
                    s"${stageCompleted.stageInfo.taskMetrics.resultSerializationTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.resultSize}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.fetchWaitTime}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.localBlocksFetched}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.localBytesRead}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.recordsRead}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.remoteBlocksFetched}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.remoteBytesRead}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.recordsWritten}," +
                    s"${stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.writeTime}\n"

            file.write(line)
        
        }

        catch{
            case e: Throwable => file.close()
        }

       
        
    }
}
