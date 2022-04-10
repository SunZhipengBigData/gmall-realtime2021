package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunzhipeng
 * @create 2022-04-08 23:10
 */
public class ProvinceStatsApp {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
         /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
       tableEnvironment.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
               "province_name STRING,province_area_code STRING" +
               ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
               "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
               "WATERMARK FOR  rowtime  AS rowtime)" +
               "with("+MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId)+")");
        Table querytable = tableEnvironment.sqlQuery("select\n" +
                "date_format(TUMBLE_START(rowtime,interval '10' second),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "date_format(TUMBLE_END(rowtime,interval '10' second),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "province_id,\n" +
                "provinnce_name,\n" +
                "province_area_code,\n" +
                "province_iso_code,\n" +
                "province_3166_2_code,\n" +
                "count(distinct order_id) ,\n" +
                "sum(split_total_amount)\n" +
                "group by \n" +
                "TUMBLE(rowtime,interval '10' second),\n" +
                "province_id,\n" +
                "provinnce_name,\n" +
                "province_area_code,\n" +
                "province_iso_code,\n" +
                "province_3166_2_code");
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnvironment.toAppendStream(querytable, ProvinceStats.class);
        provinceStatsDataStream.addSink(ClickHouseUtil.getJdbcSink(
                "insert into  province_stats_0820  values(?,?,?,?,?,?,?,?,?,?)"
        ));
        env.execute();


    }
}
