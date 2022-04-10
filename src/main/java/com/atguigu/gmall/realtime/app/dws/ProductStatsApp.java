package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author sunzhipeng
 * @create 2022-04-08 11:04
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception{
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

        //TODO 2.从Kafka中获取数据流
        //2.1 声明相关的主题名称以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageKafkaSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageKafkaSourceDS = env.addSource(pageKafkaSource);
        FlinkKafkaConsumer<String> favorKafkaSource = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorKafkaSourceDS  = env.addSource(favorKafkaSource);
        FlinkKafkaConsumer<String> cartkafkaSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartkafkaSourceDS = env.addSource(cartkafkaSource);
        FlinkKafkaConsumer<String> orderwidekafkaSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String>  orderwidekafkaSourceDS = env.addSource(orderwidekafkaSource);
        FlinkKafkaConsumer<String> paymentkafkaSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentkafkaSourceDS = env.addSource(paymentkafkaSource);
        FlinkKafkaConsumer<String> refundInfokafkaSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfokafkaSourceDS = env.addSource(refundInfokafkaSource);
        FlinkKafkaConsumer<String> commentInfokafkaSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String>commentInfokafkaSourceDS = env.addSource(commentInfokafkaSource);

        SingleOutputStreamOperator<ProductStats> productClickAndDispalyDS = pageKafkaSourceDS.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                JSONObject jsonobject = JSON.parseObject(jsonStr);
                JSONObject pageJsonObj = jsonobject.getJSONObject("page");
                String page_id = pageJsonObj.getString("page_id");
                if (page_id == null) {
                    System.out.println(">>>>" + jsonobject);
                }
                Long ts = jsonobject.getLong("ts");
                if ("good_detail".equals(page_id)) {
                    Long skuId = pageJsonObj.getLong("item");
                    ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1l).ts(ts).build();
                    out.collect(productStats);
                }

                JSONArray displays = jsonobject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayjsonObject = displays.getJSONObject(i);
                        if ("sku_id".equals(displayjsonObject.getString("item_type"))) {
                            Long skuid = displayjsonObject.getLong("item");
                            ProductStats productStats = ProductStats.builder().sku_id(skuid).display_ct(1l).ts(ts).build();
                            out.collect(productStats);
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderwidekafkaSourceDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                String create_time = orderWide.getCreate_time();
                Long ts = DateTimeUtil.toTs(create_time);
                ProductStats productStats = ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(ts)
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .build();
                return productStats;
            }
        });
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorKafkaSourceDS.map(new MapFunction<String, ProductStats>() {

            @Override
            public ProductStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1l)
                        .ts(ts)
                        .build();
                return productStats;
            }
        });

        SingleOutputStreamOperator<ProductStats>  cartStatsDS = cartkafkaSourceDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .cart_ct(1l)
                        .ts(ts)
                        .build();
                return productStats;
            }
        });

        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentkafkaSourceDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());

                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts)
                        .build();
            }
        });

        //3.6转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDS= refundInfokafkaSourceDS.map(
                jsonStr -> {
                    JSONObject refundJsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                    return productStats;

                });

        //3.7转换评价流数据
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS= commentInfokafkaSourceDS.map(
                jsonStr -> {
                    JSONObject commonJsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L)
                            .good_comment_ct(goodCt)
                            .ts(ts)
                            .build();
                    return productStats;
                });

        DataStream<ProductStats> unionDS = productClickAndDispalyDS.union(
                orderWideStatsDS,
                favorStatsDS,
                cartStatsDS,
                paymentStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                }));

        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(new KeySelector<ProductStats, Long>() {
            @Override
            public Long getKey(ProductStats productStats) throws Exception {
                return productStats.getSku_id();
            }
        });
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        SingleOutputStreamOperator<ProductStats> reduceDs = windowDS.reduce(new ReduceFunction<ProductStats>() {
                                                                              @Override
                                                                              public ProductStats reduce(ProductStats t1, ProductStats t2) throws Exception {
                                                                                  t1.setDisplay_ct(t1.getDisplay_ct() + t2.getDisplay_ct());
                                                                                  t1.setClick_ct(t1.getClick_ct() + t2.getClick_ct());
                                                                                  t1.setCart_ct(t1.getCart_ct() + t2.getCart_ct());
                                                                                  t1.setFavor_ct(t1.getFavor_ct() + t2.getFavor_ct());
                                                                                  t1.setOrder_amount(t1.getOrder_amount().add(t2.getOrder_amount()));
                                                                                  t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                                                                                  t1.setOrder_ct(t1.getOrderIdSet().size() + 0L);
                                                                                  t1.setOrder_sku_num(t1.getOrder_sku_num() + t2.getOrder_sku_num());

                                                                                  t1.getRefundOrderIdSet().addAll(t2.getRefundOrderIdSet());
                                                                                  t1.setRefund_order_ct(t1.getRefundOrderIdSet().size() + 0L);
                                                                                  t1.setRefund_amount(t1.getRefund_amount().add(t2.getRefund_amount()));

                                                                                  t1.getPaidOrderIdSet().addAll(t2.getPaidOrderIdSet());
                                                                                  t1.setPaid_order_ct(t1.getPaidOrderIdSet().size() + 0L);
                                                                                  t1.setPayment_amount(t1.getPayment_amount().add(t2.getPayment_amount()));

                                                                                  t1.setComment_ct(t1.getComment_ct() + t2.getComment_ct());
                                                                                  t1.setGood_comment_ct(t1.getGood_comment_ct() + t2.getGood_comment_ct());
                                                                                  return t1;
                                                                              }
                                                                          },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats productStats:elements){
                            productStats.setStt(simpleDateFormat.format(new Date(context.window().getStart())));
                            productStats.setEdt(simpleDateFormat.format(new Date(context.window().getEnd())));
                            productStats.setTs(new Date().getTime());
                            out.collect(productStats);
                        }

                    }
                });

        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDs= AsyncDataStream.unorderedWait(reduceDs,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS);
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS=AsyncDataStream.unorderedWait(productStatsWithSkuDs,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO"){

                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS);

        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS= AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK"){

                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS);

        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
                productStatsWithTMDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        productStatsWithCategoryDS.<ProductStats>addSink(ClickHouseUtil.getJdbcSink(
                "insert into product_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        productStatsWithCategoryDS.map(data->JSON.toJSONString(data,new SerializeConfig(true)))
                .addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));
        env.execute();

    }
}
