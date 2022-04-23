package com.atguigu.gmall.realtime.app.demo;

import com.atguigu.gmall.realtime.bean.Demo2;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author sunzhipeng
 * @create 2022-04-19 9:28
 */
public class Demo2_join {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 每 120 秒触发一次 checkpoint，不会特别频繁
        env.enableCheckpointing(120000);
        // Flink 框架内保证 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两个 checkpoints 之间最少有 120s 间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(120000);
        // checkpoint 超时时间 600s
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // 同时只有一个 checkpoint 运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 取消作业时保留 checkpoint，因为有时候任务 savepoint 可能不可用，这时我们就可以直接从 checkpoint 重启任务
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpoint 失败时 task 不失败，因为可能会有偶尔的写入 HDFS 失败，但是这并不会影响我们任务的运行
        // 偶尔的由于网络抖动 checkpoint 失败可以接受，但是如果经常失败就要定位具体的问题！
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env, settings);
        String start_topic="dwd_start_log";
        String page_topic="dwd_page_log";
        String groupId="demo2";
        //page表
        tableEnv.executeSql("CREATE TABLE dwd_page_common (" +
                " common MAP<STRING, STRING>," +
                " page MAP<STRING, STRING>," +
                " ts BIGINT," +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(page_topic, groupId) + ")");
        //start表
        tableEnv.executeSql("CREATE TABLE dwd_start_common (" +
                " common MAP<STRING, STRING>," +
                " `start` MAP<STRING, STRING>," +
                " ts BIGINT," +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(start_topic, groupId) + ")");
        //查询SQL
        Table table = tableEnv.sqlQuery("select\n" +
                "a.common['uid'] as uid,\n" +
                "count(case when b.common['mid'] is not null then 1 else 0 end) as midamount\n" +
                "from  dwd_page_common   a\n" +
                "left join\n" +
                "dwd_start_common b\n" +
                "on  a.common['uid']=b.common['uid']\n" +
                "and a.rowtime between b.rowtime  and b.rowtime +interval '10' second\n" +
                "group by \n" +
                "a.common['uid']");
        tableEnv.toRetractStream(table, Demo2.class).print();
        env.execute();

    }
}
