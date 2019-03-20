package com.InkBamboo.Druid;

import com.InkBamboo.Test.wordcount;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.joda.time.DateTime;
import scala.runtime.BoxedUnit;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * Created By InkBamboo
 * Date: 2019/1/21 10:06
 * Calm Positive
 * Think Then Ask
 */
public class DruidSink implements SinkFunction{

    final Logger log = new Logger(DruidSink.class);
    // Read config from "example.json" on the classpath.
    final InputStream configStream = DruidSink.class.getClassLoader().getResourceAsStream("druidWordCount.json");
    final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
    final DataSourceConfig<PropertiesBasedConfig> wikipediaConfig = config.getDataSource("flinkToDruidTest2");  //对应的数据分片，类似sql中的表名
    final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(wikipediaConfig)
            .buildTranquilizer(wikipediaConfig.tranquilizerBuilder());
    @Override
    public void invoke(Object value, Context context) throws Exception {

        sender.start();
        wordcount word = (wordcount)value;

        try {
            // Send 10000 objects

           // for (int i = 0; i < 100; i++) {
                // Build a sample event to send; make sure we use a current date
                final Map<String, Object> obj = ImmutableMap.<String, Object>of(
                        "timestamp",new DateTime().toString(),
                        "ColTwo", ((wordcount) value).getColTwo()
                );
                // Asynchronously send event to Druid:
                sender.send(obj).addEventListener(
                        new FutureEventListener<BoxedUnit>()
                        {
                            @Override
                            public void onSuccess(BoxedUnit value)
                            {
                                log.info("Sent message: %s", obj);
                            }

                            @Override
                            public void onFailure(Throwable e)
                            {
                                if (e instanceof MessageDroppedException) {
                                    log.warn(e, "Dropped message: %s", obj);
                                } else {
                                    log.error(e, "Failed to send message: %s", obj);
                                }
                            }
                        }
                );
        }
        finally {
            sender.flush();
            sender.stop();
        }
    }

}
