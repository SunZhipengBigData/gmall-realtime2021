package com.atguigu.gmall.realtime.app.demo;

import com.atguigu.gmall.realtime.bean.FlinDemo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunzhipeng
 * @create 2022-04-22 17:08
 */
public class FlinkDemo3 {
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

        tableEnv.executeSql("create  table   if not exists   dwd_page_log (\n" +
                "common map<String,String>,\n" +
                "page  map<String,String>,\n" +
                "ts bigint,\n" +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "watermark for rowtime as rowtime - interval '2' second\n" +
                ")\n" +
                "with\n" +
                "(\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'dwd_page_log',\n" +
                "'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',\n" +
                "'properties.group.id' = 'flinkdemo1',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'latest-offset' \n" +
                ")");
        tableEnv.executeSql("create  table   if not exists   dwd_start_log (\n" +
                "common map<String,String>,\n" +
                "`start`  map<String,String>,\n" +
                "ts bigint,\n" +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "watermark for rowtime as rowtime - interval '2' second\n" +
                ")\n" +
                "with\n" +
                "(\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'dwd_start_log',\n" +
                "'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',\n" +
                "'properties.group.id' = 'flinkdemo1',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'latest-offset' \n" +
                ")");
        Table table = tableEnv.sqlQuery("select \n" +
                "date_format(tumble_start(a.rowtime,interval '30' second),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "date_format(tumble_end(a.rowtime,interval '30' second),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "a.common['uid'] uid,\n" +
                "count(cast(b.`start`['uid'] as bigint))  midamount\n" +
                "from   dwd_page_log  a\n" +
                "left   join   dwd_start_log b \n" +
                "on   a.common['uid']=b.common['uid']\n" +
                "and a.rowtime between b.rowtime and   b.rowtime-interval '2' second \n" +
                "group by  a.common['uid'],\n" +
                "tumble(a.rowtime,interval '30'  second)");
        tableEnv.toAppendStream(table, FlinDemo.class).print();
        System.out.println("测试一下新账户");
        System.out.println("再测试一下合并分支");
        env.execute();

    }
}
