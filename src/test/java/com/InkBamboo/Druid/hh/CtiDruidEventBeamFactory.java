package com.InkBamboo.Druid.hh;

import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.joda.time.DateTime;
import org.joda.time.Period;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.flink.BeamFactory;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;

import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;


/*

public  class CtiDruidEventBeamFactory implements BeamFactory<Map<String, Object>> {
    @Override
    public Beam<Map<String, Object>> makeBeam() {
        CuratorFramework curator = CuratorFrameworkFactory.newClient("node-1:2181,node-2:2181,node-3:2181", new BoundedExponentialBackoffRetry(100, 3000, 5));
        curator.start();

        String indexService = "druid/overlord";
        String discoveryPath = "/druid/discovery";
        String dataSource = "sinkTest1";
        List<String> dimensions = ImmutableList.of("page");
        List<AggregatorFactory> aggregators = ImmutableList.of(new LongSumAggregatorFactory("count", "count2"));
        boolean isRollup = true;
//        final String timestampColumnName = "timestamp";
//        
//        final Timestamper<Map<String, Object>> timestamper = (Timestamper<Map<String, Object>>) theMap -> new DateTime(Long.valueOf(String.valueOf(theMap.get(timestampColumnName))));
        return DruidBeams.builder(new Timestamper<Map<String, Object>>() {
					@Override
					public DateTime timestamp(Map<String, Object> map) {
						map.put("timestamp", new DateTime().toString());
						return new DateTime();
					}
				})
        		.curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(indexService, dataSource))
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularities.MINUTE))
                .tuning(
                        ClusteredBeamTuning.builder()
                                .segmentGranularity(Granularity.HOUR)
                                .windowPeriod(new Period("PT1M"))
                                .partitions(1)
                                .replicants(1)
                                .build()
                )
                .buildBeam();
    }

*/
/*	@Override
	public Tranquilizer<Map<String, Object>> tranquilizer() {
		Tranquilizer<Map<String, Object>> t = Tranquilizer.create(makeBeam());
	    t.start();
		return t;
	}

    final Tran*//*


}*/
