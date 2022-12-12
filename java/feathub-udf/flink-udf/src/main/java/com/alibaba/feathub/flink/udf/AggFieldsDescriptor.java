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

import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggregationFunction;
import com.alibaba.feathub.flink.udf.aggregation.AggregationFunctionUtils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/** The descriptor of aggregation fields in the window operator. */
public class AggFieldsDescriptor implements Serializable {
    private final List<AggFieldDescriptor> aggFieldDescriptors;

    private AggFieldsDescriptor(List<AggFieldDescriptor> aggFieldDescriptors) {
        this.aggFieldDescriptors = aggFieldDescriptors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<AggFieldDescriptor> getAggFieldDescriptors() {
        return aggFieldDescriptors;
    }

    public long getMaxWindowSizeMs() {
        return aggFieldDescriptors.stream()
                .mapToLong(descriptor -> descriptor.windowSize)
                .max()
                .orElseThrow(() -> new RuntimeException("Fail to get max window size."));
    }

    /** Builder for {@link AggFieldsDescriptor}. */
    public static class Builder {
        private final List<AggFieldDescriptor> aggFieldDescriptors;

        private Builder() {
            aggFieldDescriptors = new LinkedList<>();
        }

        public Builder addField(
                String inFieldName,
                DataType inDataType,
                String outFieldNames,
                DataType outDataType,
                Long windowSize,
                String aggFunc) {
            aggFieldDescriptors.add(
                    new AggFieldDescriptor(
                            inFieldName,
                            inDataType,
                            outFieldNames,
                            outDataType,
                            windowSize,
                            aggFunc));
            return this;
        }

        public AggFieldsDescriptor build() {
            return new AggFieldsDescriptor(aggFieldDescriptors);
        }
    }

    /** The descriptor of an aggregation field. */
    public static class AggFieldDescriptor implements Serializable {
        public String inFieldName;
        public String outFieldName;
        public DataType outDataType;
        public Long windowSize;
        public AggregationFunction<Object, ?> aggFunc;

        @SuppressWarnings({"unchecked"})
        public AggFieldDescriptor(
                String inFieldName,
                DataType inDataType,
                String outFieldNames,
                DataType outDataType,
                Long windowSize,
                String aggFunc) {
            this.inFieldName = inFieldName;
            this.outFieldName = outFieldNames;
            this.outDataType = outDataType;
            this.windowSize = windowSize;
            this.aggFunc =
                    (AggregationFunction<Object, ?>)
                            AggregationFunctionUtils.getAggregationFunction(aggFunc, inDataType);
        }
    }
}
