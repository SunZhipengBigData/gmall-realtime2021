package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author sunzhipeng
 * @create 2022-04-06 16:35
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
          /*
        //1.3 检查点相关的配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/paymentWide"));
        */
        String paymentInfoSourceTopic="dwd_payment_info";
        String orderWideSourceTopic="dwm_order_wide";
        String groupId="dwm_payment_wide";
        String paymentWideSinkTopic="paymentwide_app_group";

        FlinkKafkaConsumer<String> paymentkafkaSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentsource = env.addSource(paymentkafkaSource);
        FlinkKafkaConsumer<String> orderkafkaSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> ordersource = env.addSource(orderkafkaSource);
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentsource.map(data -> JSON.parseObject(data, PaymentInfo.class));
        SingleOutputStreamOperator<OrderWide> orderWideDS = ordersource.map(data -> JSON.parseObject(data, OrderWide.class));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithFlagDs= paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                    }
                }));

        SingleOutputStreamOperator<OrderWide> orderWideWithFlagsDS = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long l) {
                        Long aLong = DateTimeUtil.toTs(orderWide.getCreate_time());
                        return aLong;
                    }
                }));

        KeyedStream<PaymentInfo, Long> paymentInfoLongKeyedStream = paymentInfoWithFlagDs.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideLongKeyedStream = orderWideWithFlagsDS.keyBy(OrderWide::getOrder_id);

        SingleOutputStreamOperator<PaymentWide> paymentWideDS= paymentInfoLongKeyedStream.intervalJoin(orderWideLongKeyedStream)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
            paymentWideDS.map(data-> JSON.toJSONString(data))
            .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

            env.execute();


    }
}
