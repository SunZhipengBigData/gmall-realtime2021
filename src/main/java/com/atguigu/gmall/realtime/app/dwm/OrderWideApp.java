package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author sunzhipeng
 * @create 2022-04-06 23:03
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))
        String orderInfoSourceTopic="dwd_order_info";
        String orderDetailSourceTopic="dwd_order_detail";
        String groupId="order_wide_group";
        String sinkTopic="dwm_order_wide";

        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoSource);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);
        SingleOutputStreamOperator<OrderInfo> infoSingleOutputStreamOperator = orderInfoDS.map(new RichMapFunction<String, OrderInfo>() {

            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String s) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });

        SingleOutputStreamOperator<OrderDetail> detailSingleOutputStreamOperator = orderDetailDS.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String s) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        SingleOutputStreamOperator<OrderInfo> orderInfoSingleOutputStreamOperator = infoSingleOutputStreamOperator.assignTimestampsAndWatermarks(WatermarkStrategy.
                <OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> detailSingleOutputStreamOperator1 = detailSingleOutputStreamOperator.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                })
        );
        KeyedStream<OrderInfo, Long> orderInfoLongKeyedStream = orderInfoSingleOutputStreamOperator.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = detailSingleOutputStreamOperator1.keyBy(OrderDetail::getOrder_id);

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoLongKeyedStream
                .intervalJoin(orderDetailLongKeyedStream)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER") {
            @Override
            public String getKey(OrderWide obj) {
                return obj.getUser_id().toString();
            }

            @Override
            public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date parse = sdf.parse(birthday);
                Long birthdayTs = parse.getTime();
                Long curTs = System.currentTimeMillis();
                Long ageTs = curTs - birthdayTs;
                Long ageLong = ageTs / 1000L / 60L / 60L / 60L / 24L / 365L;
                Integer age = ageLong.intValue();
                obj.setUser_age(age);
                obj.setUser_gender(dimInfoJsonObj.getString("GENDER"));
            }
        }, 60, TimeUnit.SECONDS);


        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide obj) {
                return obj.getUser_id().toString();
            }

            @Override
            public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                obj.setProvince_name(dimInfoJsonObj.getString("NAME"));
                obj.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                obj.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                obj.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }
                }, 60,
                TimeUnit.SECONDS);
        orderWideWithTmDS.map(data->JSON.toJSONString(data))
        .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();


    }
}
