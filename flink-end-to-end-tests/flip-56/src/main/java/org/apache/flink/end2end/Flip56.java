package org.apache.flink.end2end;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/** test FLIP-56 features 测试方法. 1. with slot 2. without slot */
public class Flip56 {

    private static final Logger LOG = LoggerFactory.getLogger(Flip56.class);

    public static void main(String[] args) throws Exception {
        LOG.info("start");
        ResourceSpec resource1 = ResourceSpec.newBuilder(0.1, 100).build();
        ResourceSpec resource2 = ResourceSpec.newBuilder(0.2, 200).build();
        ResourceSpec resource3 = ResourceSpec.newBuilder(0.3, 300).build();
        ResourceSpec resource4 = ResourceSpec.newBuilder(0.4, 400).build();
        ResourceSpec resource5 = ResourceSpec.newBuilder(0.5, 500).build();
        ResourceSpec resource6 = ResourceSpec.newBuilder(1, 1000).build();

        Method opMethod = getSetResourcesMethodAndSetAccessible(SingleOutputStreamOperator.class);
        Method sinkMethod = getSetResourcesMethodAndSetAccessible(DataStreamSink.class);

        Configuration configuration = new Configuration();
        //必须设置这个,否则在operator里面设置资源之后会无法启动任务
        configuration.setString("jobmanager.scheduler", "adaptive");
        configuration.setString("parallelism.default", "5");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(
                configuration);


        DataStream<Tuple2<Integer, Integer>> source = env.addSource(new Generator(
                10,
                3000,
                60)
        );
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
                                return true;
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
                source.addSink(
                        new RichSinkFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public void invoke(Tuple2<Integer, Integer> value) throws Exception {
                                LOG.info("final sink:" + value.toString());
                            }
                        });

        opMethod.invoke(source, resource1, resource6);
        opMethod.invoke(map, resource2, resource6);
        opMethod.invoke(filter, resource3, resource6);
        opMethod.invoke(reduce, resource4, resource6);
        sinkMethod.invoke(sink, resource5, resource6);

        env.disableOperatorChaining();
        //System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static Method getSetResourcesMethodAndSetAccessible(final Class<?> clazz)
            throws NoSuchMethodException {
        final Method setResourcesMethod =
                clazz.getDeclaredMethod("setResources", ResourceSpec.class, ResourceSpec.class);
        setResourcesMethod.setAccessible(true);
        return setResourcesMethod;
    }

    /** Data-generating source function. */
    public static final class Generator
            implements SourceFunction<Tuple2<Integer, Integer>>, CheckpointedFunction {

        private static final long serialVersionUID = -2819385275681175792L;

        private final int numKeys;
        private final int idlenessMs;
        private final int recordsToEmit;

        private volatile int numRecordsEmitted = 0;
        private volatile boolean canceled = false;

        private ListState<Integer> state = null;

        Generator(final int numKeys, final int idlenessMs, final int durationSeconds) {
            this.numKeys = numKeys;
            this.idlenessMs = idlenessMs;

            this.recordsToEmit = ((durationSeconds * 1000) / idlenessMs) * numKeys;
        }

        @Override
        public void run(final SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (numRecordsEmitted < recordsToEmit) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int i = 0; i < numKeys; i++) {
                        ctx.collect(Tuple2.of(i, numRecordsEmitted));
                        numRecordsEmitted++;
                    }
                }
                Thread.sleep(idlenessMs);
            }

            while (!canceled) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<Integer>(
                                            "state", IntSerializer.INSTANCE));

            for (Integer i : state.get()) {
                numRecordsEmitted += i;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(numRecordsEmitted);
        }
    }
}
