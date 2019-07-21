package com.Inkbamboo.Flink;

import com.Inkbamboo.Flink.sink.DruidSink;
import com.Inkbamboo.dao.wordcount2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;

/**
 * Created By InkBamboo
 * Date: 2019/1/21 10:02
 * Calm Positive
 * Think Then Ask
 *
 * 测试未通过，PropertiesBasedConfig类无法序列化
 *
 * 需要细细研究
 */
public class FlinkToDruid {

    public static void main(String[] args) throws Exception {

        /*//序列化指定类
        Kryo kryo = new Kryo();
        kryo.register(DruidSink.class);
        kryo.register(PropertiesBasedConfig$.class);*/


        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //senv.setParallelism(1);
        ArrayList<wordcount2> arr = new ArrayList();
        for(int i=100;i<200;i++){
            arr.add(new wordcount2("c"+i,i*2,"bcd"+i,"edf"+i));
        }

        DataStream ds = senv.fromCollection(arr).map(new MapFunction<wordcount2, wordcount2>() {
            @Override
            public wordcount2 map(wordcount2 value) throws Exception {
                return value;
            }
        });
        ds.print();

        //BeamSink sink = new BeamSink(new DruidSink(),true);

        ds.addSink(new DruidSink());

        senv.execute("flinkToDruidTest");
    }
}
