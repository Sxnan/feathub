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

import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SumAggregationFunction}. */
class SumAggregationFunctionTest {

    @Test
    void testSumAggregationFunctions() {
        innerTest(
                Arrays.array(1, 2, 3),
                6,
                0,
                new SumAggregationFunction.IntSumAggregationFunction());
        innerTest(
                Arrays.array(1L, 2L, 3L),
                6L,
                0L,
                new SumAggregationFunction.LongSumAggregationFunction());
        innerTest(
                Arrays.array(1.0f, 2.0f, 3.0f),
                6.0f,
                0.0f,
                new SumAggregationFunction.FloatSumAggregationFunction());
        innerTest(
                Arrays.array(1.0, 2.0, 3.0),
                6.0,
                0.0,
                new SumAggregationFunction.DoubleSumAggregationFunction());
    }

    private <T> void innerTest(
            T[] inputs, T expectedResult, T expectedInitRes, SumAggregationFunction<T> aggFunc) {
        assertThat(aggFunc.getResult()).isEqualTo(expectedInitRes);
        for (T input : inputs) {
            aggFunc.aggregate(input, 0);
        }
        assertThat(aggFunc.getResult()).isEqualTo(expectedResult);
        aggFunc.reset();
        assertThat(aggFunc.getResult()).isEqualTo(expectedInitRes);
    }
}
