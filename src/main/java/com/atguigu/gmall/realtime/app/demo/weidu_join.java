package com.atguigu.gmall.realtime.app.demo;

import com.atguigu.gmall.realtime.bean.WeiduJoin;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunzhipeng
 * @create 2022-04-22 22:19
 */
public class weidu_join {
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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("create  table   dwd_page_log(\n" +
                "common map<String,String>,\n" +
                "page map<String,String>,\n" +
                "ts  bigint,\n" +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "watermark for rowtime as rowtime - interval '3' second\n" +
                ")with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'dwd_start_log',\n" +
                "'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',\n" +
                "'properties.group.id' = 'flinkdemo1',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'latest-offset' \n" +
                ")");

        tableEnv.executeSql("create table dwd_display_log(\n" +
                "display_type String,\n" +
                "page_id String,\n" +
                "item  String,\n" +
                "item_type String,\n" +
                "pos_id String,\n" +
                "order String,\n" +
                "PERIOD FOR SYSTEM_TIME,\n" +
                "PRIMARY KEY(page_id)\n" +
                ")\n" +
                "with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'dwd_display_log',\n" +
                "'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',\n" +
                "'properties.group.id' = 'flinkdemo1',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'latest-offset' \n" +
                ")");
        Table table = tableEnv.sqlQuery("select\n" +
                "t1.page['page_id'],\n" +
                "t2.display_type,\n" +
                "t2.item,\n" +
                "t2.item_type,\n" +
                "t2.pos_id,\n" +
                "t2.order,\n" +
                "from   dwd_page log AS t1\n" +
                "left join   dwd_display_log FOR SYSTEM_TIME AS OF t1.proctime AS t2\n" +
                "on t1.page['page_id']=t2.page_id");
        tableEnv.toAppendStream(table, WeiduJoin.class).print();
        System.out.println("test一下");
        env.execute();



    }
}
