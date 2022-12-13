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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

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
    private ValueState<Long> maxTriggerTimeState;

    private ValueState<Long> lastRowTimeState;

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
    public void open(Configuration parameters) throws IOException {
        state =
                MultiWindowSizeState.buildMultiWindowSizeState(
                        getRuntimeContext(), rowTypeSerializer);
        maxTriggerTimeState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<Long>("maxTriggerTimer", Long.class));
        lastRowTimeState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("lastRowTimeState", Long.class));
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        final long rowTime = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();
        final Long lastRowTime = lastRowTimeState.value();
        if (lastRowTime != null) {
            Preconditions.checkState(
                    lastRowTime < rowTime,
                    String.format(
                            "key: %s row out of ordered. lastRowTime: %s current row time: %s",
                            ctx.getCurrentKey(), lastRowTime, rowTime));
        }
        lastRowTimeState.update(rowTime);

        Long maxTriggerTime = this.maxTriggerTimeState.value();
        if (maxTriggerTime == null) {
            maxTriggerTimeState.update(rowTime - stepSizeMs);
            maxTriggerTime = rowTime - stepSizeMs;
        }
        if (rowTime + descriptor.getMaxWindowSizeMs() > maxTriggerTime) {
            long triggerTime = rowTime + descriptor.getMaxWindowSizeMs();
            while (triggerTime > maxTriggerTime) {
                LOG.debug("[Key={}] Register timer {}", ctx.getCurrentKey(), triggerTime);
                ctx.timerService().registerEventTimeTimer(triggerTime);
                triggerTime -= stepSizeMs;
            }
            maxTriggerTimeState.update(rowTime + descriptor.getMaxWindowSizeMs());
        }
        LOG.debug(
                "[Key={}] Add row to state {} with row time {}", ctx.getCurrentKey(), row, rowTime);
        state.addRow(ctx.getCurrentKey(), rowTime, row);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Row, Row, Row>.OnTimerContext ctx,
            Collector<Row> out)
            throws Exception {
        LOG.debug("[Key={}] onTimer timestamp: {}", ctx.getCurrentKey(), timestamp);
        LOG.debug("Current state: {}", CollectionUtil.iterableToList(state.rowState.entries()));
        state.pruneRow(ctx.getCurrentKey(), timestamp - descriptor.getMaxWindowSizeMs());
        descriptor.getAggFieldDescriptors().forEach(d -> d.aggFunc.reset());

        boolean hasRow = false;
        for (long rowTime : state.orderedTimestampMap.get(ctx.getCurrentKey())) {
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
        private final Map<Row, LinkedList<Long>> orderedTimestampMap;

        private MultiWindowSizeState(MapState<Long, Row> mapState) {

            this.rowState = mapState;
            this.orderedTimestampMap = new HashMap<>();
        }

        public static MultiWindowSizeState buildMultiWindowSizeState(
                RuntimeContext context, TypeSerializer<Row> rowTypeSerializer) {
            final MapState<Long, Row> mapState =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "RowState", LongSerializer.INSTANCE, rowTypeSerializer));

            return new MultiWindowSizeState(mapState);
        }

        public void addRow(Row key, long timestamp, Row row) throws Exception {
            LinkedList<Long> orderedTimestamp = orderedTimestampMap.get(key);
            if (orderedTimestamp == null) {
                orderedTimestamp = new LinkedList<>();
                CollectionUtil.iterableToList(rowState.keys()).stream()
                        .sorted()
                        .forEach(orderedTimestamp::add);
            }
            rowState.put(timestamp, row);
            orderedTimestamp.addLast(timestamp);
            orderedTimestampMap.put(key, orderedTimestamp);
            LOG.debug("Current state: {}", CollectionUtil.iterableToList(rowState.entries()));
        }

        public void pruneRow(Row key, long lowerBound) throws Exception {
            LOG.debug("Prune rows with lower bound: {}", lowerBound);
            LinkedList<Long> orderedTimestamp = orderedTimestampMap.get(key);
            if (orderedTimestamp == null) {
                return;
            }
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
