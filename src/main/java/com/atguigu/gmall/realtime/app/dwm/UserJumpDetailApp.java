package com.atguigu.gmall.realtime.app.dwm;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author sunzhipeng
 * @create 2022-04-05 21:57
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))

        String Sourcetopic="dwd_page_log";
        String groupId="user_jump_detail_group";
        String sinkTopic="dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(Sourcetopic, groupId);
        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> map = stringDataStreamSource.map(data -> JSON.parseObject(data));

        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = map.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, String> keyByMidDS = jsonObjWithTSDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String last_Page_id = value.getJSONObject("page").getString("last_page_id");
                        if (last_Page_id == null || last_Page_id.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //获取当前页面的id
                        String pageId = value.getJSONObject("page").getString("page_id");
                        //判断当前访问的页面id是否为null
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.milliseconds(10000));
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, pattern);
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
        SingleOutputStreamOperator<Object> filterDS = patternStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        for (JSONObject jsonObject : jsonObjectList) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<Object> out) throws Exception {

                    }
                });

        DataStream<String> jumpDS = filterDS.getSideOutput(timeoutTag);
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();


    }
}
