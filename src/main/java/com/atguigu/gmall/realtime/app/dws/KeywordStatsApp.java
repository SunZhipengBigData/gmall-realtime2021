package com.atguigu.gmall.realtime.app.dws;


import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunzhipeng
 * @create 2022-04-09 11:02
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        tableEnvironment.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";

        tableEnvironment.executeSql("CREATE TABLE page_view (" +
                " common MAP<STRING, STRING>," +
                " page MAP<STRING, STRING>," +
                " ts BIGINT," +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");
        Table fullwordTable = tableEnvironment.sqlQuery("select page['item'] fullword,rowtime " +
                " from page_view " +
                " where page['page_id']='good_list' and page['item'] IS NOT NULL");
        Table keywordTable = tableEnvironment.sqlQuery("SELECT keyword, rowtime " +
                "FROM  " + fullwordTable + "," +
                "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)");
        Table reduceTable = tableEnvironment.sqlQuery(
                "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                        " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        DataStream<KeywordStats> keywordStatsDataStream = tableEnvironment.toAppendStream(reduceTable, KeywordStats.class);
        keywordStatsDataStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );
        System.out.println("这是dev_szp分支");
        env.execute();


    }
}
