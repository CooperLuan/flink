package org.apache.flink.end2end;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/** test FLIP-56 features 测试方法. 1. with slot 2. without slot */
public class Flip56 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, Integer>> source =
                env.addSource(
                        new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public void run(SourceContext<Tuple2<Integer, Integer>> ctx)
                                    throws Exception {}

                            @Override
                            public void cancel() {}
                        });
        DataStream<Tuple2<Integer, Integer>> map =
                source.map(
                        new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        });
        DataStream<Tuple2<Integer, Integer>> filter =
                map.filter(
                        new FilterFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                                return false;
                            }
                        });
        DataStream<Tuple2<Integer, Integer>> reduce =
                filter.keyBy(0)
                        .reduce(
                                new ReduceFunction<Tuple2<Integer, Integer>>() {
                                    @Override
                                    public Tuple2<Integer, Integer> reduce(
                                            Tuple2<Integer, Integer> value1,
                                            Tuple2<Integer, Integer> value2)
                                            throws Exception {
                                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                                    }
                                });
        DataStreamSink<Tuple2<Integer, Integer>> sink =
                reduce.addSink(
                        new SinkFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public void invoke(Tuple2<Integer, Integer> value) throws Exception {}
                        });
        //        JobGraph jobGraph =
        // StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
        env.execute();
    }
}
