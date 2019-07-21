package flinkSrc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;

/**
 * Created By InkBamboo
 * Date: 2019/5/16 17:47
 * Calm Positive
 * Think Then Ask
 */
public class DataStreamTestJava {
    public static void main(String[] args){

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        URL fileurl = DataStreamTestJava.class.getClassLoader().getResource("UserBehavior.csv");
       Path filepath =  Path.fromLocalFile(new File(fileurl.getPath()));

        //构建数据需要的typeinfomation信息
        PojoTypeInfo<UserBehavior3> pojoType = (PojoTypeInfo<UserBehavior3>)TypeExtractor.createTypeInfo(UserBehavior3.class);
        String[] str = {"userId","itemId","categoryId","behavior","timestamp"};
        PojoCsvInputFormat csvinput = new PojoCsvInputFormat(filepath,pojoType,str);

        DataStream<UserBehavior3> dataStream = senv.createInput(csvinput);
        dataStream.map(new MapFunction<UserBehavior3, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(UserBehavior3 value) throws Exception {
                return new Tuple2<>(value.userId()+""+value.itemId(),1);
            }
        })
        .keyBy(0)
        .timeWindow(Time.minutes(10))
                //类型不匹配报错，不知道怎么解决==>>   结合timewindow返回的类型处理
       .process(new myprocessFunctions())
        ;
    }
}

class myprocessFunctions extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>, Tuple,TimeWindow>{
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> collector) throws Exception {
        Long key = (Long)tuple.getField(0);
        for(Tuple2<String, Integer> ele:elements){
            collector.collect(ele);
            if(ele._1.contains("23")){
                context.output(new OutputTag("side-output"),ele);
            }
        }
    }
    //key的类型给为tuple，然后再将tuple中的key获取到即可
    /*@Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        for(Tuple2<String, Integer> ele:elements){
            out.collect(ele);
            if(ele._1.contains("23")){
                context.output(new OutputTag("side-output"),ele);
            }
        }
    }*/

}
