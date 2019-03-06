package com.Inkbamboo.flinkSources.Druid;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.joda.time.DateTime;
import scala.runtime.BoxedUnit;

import java.io.InputStream;
import java.util.Map;

/**
 * Created By InkBamboo
 * Date: 2019/1/11 11:19
 * Calm Positive
 * Think Then Ask
 *
 * 来自druid官网的例子https://github.com/druid-io/tranquility/blob/master/core/src/test/java/com/metamx/tranquility/example/JavaExample.java
 *
 * 需要配合tranquality对应的配置的json文件
 *
 * 测试用过可以使用
 */
public class DataToDruidDemo
{
    private static final Logger log = new Logger(DataToDruidDemo.class);

    public static void main(String[] args)
    {
        // Read config from "example.json" on the classpath.
        final InputStream configStream = DataToDruidDemo.class.getClassLoader().getResourceAsStream("example.json");
        final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> wikipediaConfig = config.getDataSource("a1abc");  //对应的数据分片，类似sql中的表名
        final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(wikipediaConfig)
                .buildTranquilizer(wikipediaConfig.tranquilizerBuilder());
        sender.start();

        try {
            // Send 10000 objects

            for (int i = 0; i < 100; i++) {
                // Build a sample event to send; make sure we use a current date
                final Map<String, Object> obj = ImmutableMap.<String, Object>of(
                        "timestamp", new DateTime().toString(),
                        "page", "foo",
                        "added", i
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
        }
        finally {
            sender.flush();
            sender.stop();
        }
    }
}