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

import org.apache.flink.table.types.DataType;

/** Utility of aggregation functions. */
public class AggregationFunctionUtils {
    public static AggregationFunction<?, ?> getAggregationFunction(
            String aggFunc, DataType inDataType) {
        if ("SUM".equals(aggFunc)) {
            return getSumAggregationFunction(inDataType);
        } else if ("AVG".equals(aggFunc)) {
            return new RowAvgAggregationFunction(inDataType);
        } else if ("FIRST_VALUE".equals(aggFunc)) {
            return new FirstValueAggregationFunction<>(inDataType);
        } else if ("LAST_VALUE".equals(aggFunc)) {
            return new LastValueAggregationFunction<>(inDataType);
        } else if ("MAX".equals(aggFunc)) {
            return new MaxAggregationFunction<>(inDataType);
        } else if ("MIN".equals(aggFunc)) {
            return new MinAggregationFunction<>(inDataType);
        } else if ("COUNT".equals(aggFunc) || "ROW_NUMBER".equals(aggFunc)) {
            return new CountAggregationFunction();
        } else if ("VALUE_COUNTS".equals(aggFunc)) {
            return new MergeValueCountsAggregationFunction(inDataType);
        }

        throw new RuntimeException(String.format("Unsupported aggregation function %s", aggFunc));
    }

    @SuppressWarnings({"unchecked"})
    private static <IN_T> SumAggregationFunction<IN_T> getSumAggregationFunction(
            DataType inDataType) {
        final Class<?> inClass = inDataType.getConversionClass();
        if (inClass.equals(Integer.class)) {
            return (SumAggregationFunction<IN_T>)
                    new SumAggregationFunction.IntSumAggregationFunction();
        } else if (inClass.equals(Long.class)) {
            return (SumAggregationFunction<IN_T>)
                    new SumAggregationFunction.LongSumAggregationFunction();
        } else if (inClass.equals(Float.class)) {
            return (SumAggregationFunction<IN_T>)
                    new SumAggregationFunction.FloatSumAggregationFunction();
        } else if (inClass.equals(Double.class)) {
            return (SumAggregationFunction<IN_T>)
                    new SumAggregationFunction.DoubleSumAggregationFunction();
        }
        throw new RuntimeException(
                String.format("Unsupported type for AvgAggregationFunction %s.", inDataType));
    }
}
