package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @author sunzhipeng
 * @create 2022-04-07 16:03
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
         /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewsourceDS = env.addSource(pageViewSource);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        DataStreamSource<String> uniqueVisitsource = env.addSource(uniqueVisitSource);
        FlinkKafkaConsumer<String> userJumpDetailSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> userJumpJsonStrDS = env.addSource(userJumpDetailSource);
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pageViewsourceDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0l,
                        1l,
                        0l,
                        0l,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
                return visitorStats;
            }
        });

        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uniqueVisitsource.map(new MapFunction<String, VisitorStats>() {

            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        SingleOutputStreamOperator<VisitorStats> svStatsDS = pageViewsourceDS.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取当前页面的lastPageId
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats(
                                    "",
                                    "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L,
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObj.getLong("ts")
                            );
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        DataStream<VisitorStats> unionDS = pvStatsDS.union(uvStatsDS, svStatsDS, userJumpStatsDS);

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWatermarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(
                        visitorStats.getAr(),
                        visitorStats.getCh(),
                        visitorStats.getVc(),
                        visitorStats.getIs_new()
                );
            }
        });
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS=keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
                                                                              @Override
                                                                              public VisitorStats reduce(VisitorStats t1, VisitorStats t2) throws Exception {
                                                                                  t1.setPv_ct(t1.getPv_ct() + t2.getPv_ct());
                                                                                  t1.setUj_ct(t1.getUv_ct() + t2.getUv_ct());
                                                                                  t1.setSv_ct(t1.getSv_ct() + t2.getSv_ct());
                                                                                  t1.setDur_sum(t1.getDur_sum() + t2.getDur_sum());
                                                                                  return t1;
                                                                              }
                                                                          },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        for (VisitorStats visitorStats : elements) {
                            //获取窗口的开放时间
                            String startDate = sdf.format(new Date(context.window().getStart()));
                            String endDate = sdf.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            visitorStats.setTs(new Date().getTime());
                            out.collect(visitorStats);
                        }
                    }
                }
        );
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into visitot_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }
}
