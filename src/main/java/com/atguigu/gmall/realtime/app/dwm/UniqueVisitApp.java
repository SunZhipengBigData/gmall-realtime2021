package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sunzhipeng
 * @create 2022-04-06 12:27
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))
        String SourceTopic="dwd_page_log";
        String groupId="unique_visit_app_group";
        String SinkTopic="dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(SourceTopic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> map = source.map(data -> JSON.parseObject(data));

        KeyedStream<JSONObject, String> keyedStream = map.keyBy(data -> data.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> filterDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> firstValueState = null;
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
                this.firstValueState = getRuntimeContext().getState(lastVisitDateStateDes);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                Long ts = jsonObject.getLong("ts");
                String logDate = sdf.format(new Date(ts));
                String lastVisitDate = firstValueState.value();

                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                    System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate" + logDate);
                    firstValueState.update(logDate);
                    return true;
                }

            }
        });
        SingleOutputStreamOperator<String> kafkaDS = filterDs.map(data -> data.toJSONString());
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(SinkTopic));
        env.execute();

    }
}
