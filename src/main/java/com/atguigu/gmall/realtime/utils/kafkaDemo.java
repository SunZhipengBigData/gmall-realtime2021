package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author sunzhipeng
 * @create 2021-04-21 20:03
 */
public class kafkaDemo {
    private static String kafkaServer = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties prop=new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }
    public static FlinkKafkaProducer<String> getkafkaSink(String topic){
        return new FlinkKafkaProducer<>(kafkaServer,topic,new SimpleStringSchema());
    }
}
