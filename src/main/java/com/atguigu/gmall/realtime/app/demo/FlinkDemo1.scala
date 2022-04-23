//package com.atguigu.gmall.realtime.app.demo
//
//import java.util.Date
//
//import com.atguigu.gmall.realtime.bean.FlinDemo
//import org.apache.flink.streaming.api.CheckpointingMode
//import org.apache.flink.streaming.api.environment.CheckpointConfig
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.{EnvironmentSettings, Table}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//
///**
// * @author sunzhipeng
// * @create 2022-04-22 13:59
// */
//object FlinkDemo1 {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(4)
//    // 每 120 秒触发一次 checkpoint，不会特别频繁
//    env.enableCheckpointing(120000)
//    // Flink 框架内保证 EXACTLY_ONCE
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    // 两个 checkpoints 之间最少有 120s 间隔
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(120000)
//    // checkpoint 超时时间 600s
//    env.getCheckpointConfig.setCheckpointTimeout(600000)
//    // 同时只有一个 checkpoint 运行
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//    // 取消作业时保留 checkpoint，因为有时候任务 savepoint 可能不可用，这时我们就可以直接从 checkpoint 重启任务
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    // checkpoint 失败时 task 不失败，因为可能会有偶尔的写入 HDFS 失败，但是这并不会影响我们任务的运行
//    // 偶尔的由于网络抖动 checkpoint 失败可以接受，但是如果经常失败就要定位具体的问题！
//    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
//    tableEnv.executeSql("""create  table   if not exists   dwd_page_log (
//                          |common map<String,String>,
//                          |page  map<String,String>,
//                          |ts bigint,
//                          |rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
//                          |watermark for rowtime as rowtime - interval '4' second
//                          |)
//                          |with
//                          |(
//                          |'connector' = 'kafka',
//                          |'topic' = 'dwd_page_log',
//                          |'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',
//                          |'properties.group.id' = 'flinkdemo1',
//                          |'format' = 'json',
//                          |'scan.startup.mode' = 'latest-offset'
//                          |)""".stripMargin)
//    tableEnv.executeSql("""create  table   if not exists   dwd_start_log (
//                          |common map<String,String>,
//                          |`start`  map<String,String>,
//                          |ts bigint,
//                          |rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),
//                          |watermark for rowtime as rowtime - interval '4' second
//                          |)
//                          |with
//                          |(
//                          |'connector' = 'kafka',
//                          |'topic' = 'dwd_start_log',
//                          |'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',
//                          |'properties.group.id' = 'flinkdemo1',
//                          |'format' = 'json',
//                          |'scan.startup.mode' = 'latest-offset'
//                          |)
//                          |""".stripMargin)
//
//    val table: Table = tableEnv.sqlQuery(
//      """select
//        |date_format(tumble_start(a.rowtime,interval '10' second),'yyyy-MM-dd HH:mm:ss') stt,
//        |date_format(tumble_end(a.rowtime,interval '10' second),'yyyy-MM-dd HH:mm:ss')  edt,
//        |a.common['uid'] uid,
//        |sum(cast(b.`start`['loading_time'] as int))
//        |from   dwd_page_log  a
//        |left   join   dwd_start_log b
//        |on   a.common['uid']=b.common['uid']
//        |and a.rowtime between b.rowtime and   b.rowtime-interval '2' second
//        |group by  a.common['uid'],
//        |tumble(a.rowtime,interval '10'  second)""".stripMargin)
//    val value: DataStream[(String, String, String, Long)] = tableEnv.toAppendStream[(String, String, String, Long)](table)
//    println(value)
//
//    env.execute()
//
//
//  }
//
//}
