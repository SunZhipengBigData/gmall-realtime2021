package com.atguigu.gmall.realtime.app.demo;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.Demo;
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
 * @create 2022-04-17 10:25
 * 3> {ar=110000, uid=38, os=iOS 13.2.3, ch=Appstore, is_new=0, md=iPhone Xs Max, mid=mid_14, vc=v2.1.134, ba=iPhone},{page_id=home, during_time=9865},1648956648000,2022-04-03T11:30:48
 * 3> {ar=110000, uid=38, os=iOS 13.2.3, ch=Appstore, is_new=0, md=iPhone Xs Max, mid=mid_14, vc=v2.1.134, ba=iPhone},{page_id=good_detail, item=4, source_type=promotion, item_type=sku_id, during_time=9713, last_page_id=home},1648956648000,2022-04-03T11:30:48
 * 3> {ar=440000, uid=34, os=Android 11.0, ch=xiaomi, is_new=0, md=Xiaomi Mix2 , mid=mid_5, vc=v2.1.134, ba=Xiaomi},{page_id=home, during_time=10333},1648956648000,2022-04-03T11:30:48
 * 3> {ar=440000, uid=34, os=Android 11.0, ch=xiaomi, is_new=0, md=Xiaomi Mix2 , mid=mid_5, vc=v2.1.134, ba=Xiaomi},{page_id=search, during_time=8596, last_page_id=home},1648956648000,2022-04-03T11:30:48
 */
public class Demo1 {
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
        tableEnv.createTemporarySystemFunction("ik", KeywordUDTF.class);
        String topic="dwd_page_log";
        String groupId="demo";
        tableEnv.executeSql("CREATE TABLE dwd_page_common (" +
                " common MAP<STRING, STRING>," +
                " page MAP<STRING, STRING>," +
                " ts BIGINT," +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        Table table = tableEnv.sqlQuery("select DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
                " DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
                "common['mid']  as mid,count(*)  as midamount from dwd_page_common " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND),common['mid']");
        tableEnv.toRetractStream(table, Demo.class).print();
        //     tableEnv.createTemporarySystemFunction("ik",KeywordUDTF.class);

//        Table sqlQuery = tableEnv.sqlQuery("select DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
//                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
//                " count(common['mid']) midamount ," +
//                "UNIX_TIMESTAMP()*1000 ts from  dwd_page_common" +
//                "group by TUMBLE(rowtime, INTERVAL '10' SECOND)");
//
//        DataStream<Demo1> demo1DataStream = tableEnv.toAppendStream(sqlQuery, Demo1.class);
//       demo1DataStream.addSink(ClickHouseUtil.getJdbcSink("" +
//              "insert into demo1(stt,edt,midamount,ts) values(?,?,?,?) "));

    env.execute();


    }
}
