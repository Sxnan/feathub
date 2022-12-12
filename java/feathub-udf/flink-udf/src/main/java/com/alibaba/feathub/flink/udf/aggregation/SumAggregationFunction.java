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

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of values. */
public abstract class SumAggregationFunction<IN_T> implements AggregationFunction<IN_T, IN_T> {

    /** Aggregation functions that calculates the sum of Integer values. */
    public static class IntSumAggregationFunction extends SumAggregationFunction<Integer> {
        private int agg = 0;

        @Override
        public void reset() {
            agg = 0;
        }

        @Override
        public void aggregate(Integer value, long timestamp) {
            agg += value;
        }

        @Override
        public Integer getResult() {
            return agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.INT();
        }
    }

    /** Aggregation functions that calculates the sum of Long values. */
    public static class LongSumAggregationFunction extends SumAggregationFunction<Long> {
        private long agg = 0;

        @Override
        public void reset() {
            agg = 0;
        }

        @Override
        public void aggregate(Long value, long timestamp) {
            agg += value;
        }

        @Override
        public Long getResult() {
            return agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.BIGINT();
        }
    }

    /** Aggregation functions that calculates the sum of Float values. */
    public static class FloatSumAggregationFunction extends SumAggregationFunction<Float> {
        private float agg = 0.0f;

        @Override
        public void reset() {
            agg = 0;
        }

        @Override
        public void aggregate(Float value, long timestamp) {
            agg += value;
        }

        @Override
        public Float getResult() {
            return agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.FLOAT();
        }
    }

    /** Aggregation functions that calculates the sum of Double values. */
    public static class DoubleSumAggregationFunction extends SumAggregationFunction<Double> {
        private double agg = 0.0;

        @Override
        public void reset() {
            agg = 0;
        }

        @Override
        public void aggregate(Double value, long timestamp) {
            agg += value;
        }

        @Override
        public Double getResult() {
            return agg;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.DOUBLE();
        }
    }
}
