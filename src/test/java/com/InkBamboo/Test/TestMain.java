package com.InkBamboo.Test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;

/**
 * Created By InkBamboo
 * Date: 2019/1/19 15:03
 * Calm Positive
 * Think Then Ask
 *
 * 测试通过，javaAPI获取数据源时使用的是DatastreamSource，在进行算子操作之后就会转变为Datastream了。
 */
public class TestMain {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        Tuple3 tuple3 = new Tuple3("1","2","aas");
        ArrayList list = new ArrayList();
        list.add(tuple3);
        list.add(new Tuple3<String,String,String>("2","3","4"));
      DataStreamSource<Tuple3> streams =  senv.fromElements(new Tuple3("1","2","ass"),new Tuple3("2","3","bbc")).setParallelism(1);
     streams.map(new MapFunction<Tuple3, Tuple2>() {
         @Override
         public Tuple2 map(Tuple3 value) throws Exception {
             return new Tuple2(value._1(),value);
         }
     })
     .flatMap(new FlatMapFunction<Tuple2, wordcount>() {
         @Override
         public void flatMap(Tuple2 value, Collector<wordcount> out) throws Exception {
             Tuple3 tpl3 = (Tuple3) value._2();
             out.collect(new wordcount(value._1.toString(),Integer.parseInt(tpl3._1().toString()),tpl3._2().toString(),tpl3._3().toString()));
         }
     }).keyBy("colOne")
             .sum("colTwo")
     .print();

     senv.execute("test Main");
    }
}


