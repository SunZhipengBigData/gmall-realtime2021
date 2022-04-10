package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sunzhipeng
 * @create 2022-04-04 21:04
 */
public class BaseLogApp {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3设置Checkpoint
        //每5000ms开始一次checkpoint，模式是EXACTLY_ONCE（默认）
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/baselogApp"));

        //System.setProperty("HADOOP_USER_NAME","atguigu");
        String topic="ods_base_log";
        String groupId="base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> map = source.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;

            }
        });


        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = map.keyBy(data -> data.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = jsonObjectStringKeyedStream.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> firstVisitDateState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState= getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        Long ts = value.getLong("ts");

                        if ("1".equals(isNew)) {
                            String stateDate = firstVisitDateState.value();
                            String format = sdf.format(new Date(ts));
                            if (stateDate != null && stateDate.length() != 0) {
                                if (!stateDate.equals(format)) {
                                    isNew = "0";
                                    value.getJSONObject("common").put("is_new", isNew);
                                } else {
                                    firstVisitDateState.update(format);
                                }
                            }
                        }
                        return value;
                    }
                }
        );

        OutputTag<String> start = new OutputTag<String>("start"){};
        OutputTag<String> display = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDs = jsonDSWithFlag.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                        JSONObject jsonObject = jsonObj.getJSONObject("start");
                        String dataString = jsonObj.toString();
                        if (jsonObject != null && jsonObject.size() > 0) {
                            ctx.output(start, dataString);
                        } else {
                            out.collect(dataString);
                            JSONArray displays = jsonObj.getJSONArray("display");
                            if (displays != null && displays.size() > 0) {
                                //如果是曝光日志，遍历输出到侧输出流
                                for (int i = 0; i < displays.size(); i++) {
                                    //获取每一条曝光事件
                                    JSONObject displaysJsonObj = displays.getJSONObject(i);
                                    //获取页面id
                                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                    //给每一条曝光事件加pageId
                                    displaysJsonObj.put("page_id", pageId);
                                    ctx.output(display, displaysJsonObj.toString());
                                }
                            }
                        }
                    }
                }
        );
        DataStream<String> startDS = pageDs.getSideOutput(start);
        DataStream<String> displayDS = pageDs.getSideOutput(display);

        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startDS.addSink(startSink);

        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        displayDS.addSink(displaySink);

        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
       pageDs.addSink(pageSink);
       env.execute();

    }
}
