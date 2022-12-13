/*
 * Copyright 2022 The Feathub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.udf;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A KeyedProcessFunction that aggregate sliding windows with different sizes. With this process
 * function, we only need to keep the rows for the maximum window size, instead of rows for each
 * sliding window.
 *
 * <p>The ProcessFunction assumes that: 1. rows of each key are ordered by the row time 2. row time
 * attribute of rows with the same key are all distinct. The assumptions hold true after applying
 * the tumbling window aggregation whose window size is the same as the step size of the {@link
 * MultiSizeSlidingWindowKeyedProcessFunction} to be applied.
 */
public class MultiSizeSlidingWindowKeyedProcessFunction
        extends KeyedProcessFunction<Row, Row, Row> {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiSizeSlidingWindowKeyedProcessFunction.class);
    private final AggFieldsDescriptor descriptor;
    private final TypeSerializer<Row> rowTypeSerializer;
    private final String rowTimeFieldName;
    private final long stepSizeMs;

    private MultiWindowSizeState state;

    public MultiSizeSlidingWindowKeyedProcessFunction(
            AggFieldsDescriptor descriptor,
            TypeSerializer<Row> rowTypeSerializer,
            String rowTimeFieldName,
            long stepSizeMs) {
        this.descriptor = descriptor;
        this.rowTypeSerializer = rowTypeSerializer;
        this.rowTimeFieldName = rowTimeFieldName;
        this.stepSizeMs = stepSizeMs;
    }

    @Override
    public void open(Configuration parameters) {
        state =
                MultiWindowSizeState.buildMultiWindowSizeState(
                        getRuntimeContext(), rowTypeSerializer);
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        final long rowTime = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();
        long triggerTime = rowTime;
        while (triggerTime <= rowTime + descriptor.getMaxWindowSizeMs()) {
            LOG.debug("[Key={}] Register timer {}", ctx.getCurrentKey(), triggerTime);
            ctx.timerService().registerEventTimeTimer(triggerTime);
            triggerTime += stepSizeMs;
        }
        LOG.debug(
                "[Key={}] Add row to state {} with row time {}", ctx.getCurrentKey(), row, rowTime);
        state.addRow(rowTime, row);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Row, Row, Row>.OnTimerContext ctx,
            Collector<Row> out)
            throws Exception {
        LOG.debug("[Key={}] onTimer timestamp: {}", ctx.getCurrentKey(), timestamp);
        LOG.debug("Current state: {}", CollectionUtil.iterableToList(state.rowState.entries()));
        state.pruneRow(timestamp - descriptor.getMaxWindowSizeMs());
        descriptor.getAggFieldDescriptors().forEach(d -> d.aggFunc.reset());

        boolean hasRow = false;
        for (long rowTime : state.orderedTimestamp) {
            if (rowTime > timestamp) {
                break;
            }
            Row curRow = state.rowState.get(rowTime);
            Preconditions.checkNotNull(
                    curRow, String.format("Fail to find row with timestamp %s in state.", rowTime));
            for (AggFieldsDescriptor.AggFieldDescriptor descriptor :
                    descriptor.getAggFieldDescriptors()) {
                long lowerBound = timestamp - descriptor.windowSize;
                if (lowerBound >= rowTime) {
                    continue;
                }
                descriptor.aggFunc.aggregate(curRow.getField(descriptor.inFieldName), rowTime);
                hasRow = true;
            }
        }

        if (!hasRow) {
            // output nothing if no row is aggregated.
            return;
        }

        final Row aggResultRow =
                Row.of(
                        descriptor.getAggFieldDescriptors().stream()
                                .map(d -> d.aggFunc.getResult())
                                .toArray());

        out.collect(
                Row.join(
                        ctx.getCurrentKey(),
                        aggResultRow,
                        Row.of(Instant.ofEpochMilli(timestamp))));
    }

    /** The state of {@link MultiSizeSlidingWindowKeyedProcessFunction}. */
    public static class MultiWindowSizeState {
        private final MapState<Long, Row> rowState;
        private final LinkedList<Long> orderedTimestamp;
        private boolean initialized = false;

        private MultiWindowSizeState(MapState<Long, Row> mapState) {

            this.rowState = mapState;
            this.orderedTimestamp = new LinkedList<>();
        }

        public static MultiWindowSizeState buildMultiWindowSizeState(
                RuntimeContext context, TypeSerializer<Row> rowTypeSerializer) {
            final MapState<Long, Row> mapState =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "RowState", LongSerializer.INSTANCE, rowTypeSerializer));

            return new MultiWindowSizeState(mapState);
        }

        public void addRow(long timestamp, Row row) throws Exception {
            if (!initialized) {
                CollectionUtil.iterableToList(rowState.keys()).stream()
                        .sorted()
                        .forEach(orderedTimestamp::add);
                initialized = true;
            }
            rowState.put(timestamp, row);
            orderedTimestamp.addLast(timestamp);
            LOG.debug("Current state: {}", CollectionUtil.iterableToList(rowState.entries()));
        }

        public void pruneRow(long lowerBound) throws Exception {
            LOG.debug("Prune rows with lower bound: {}", lowerBound);
            final Iterator<Long> iterator = orderedTimestamp.iterator();
            while (iterator.hasNext()) {
                final long cur = iterator.next();
                if (cur >= lowerBound) {
                    break;
                }

                LOG.debug("Removed row with timestamp {}", cur);
                rowState.remove(cur);
                iterator.remove();
            }
            LOG.debug("Current state: {}", CollectionUtil.iterableToList(rowState.entries()));
        }
    }
}
