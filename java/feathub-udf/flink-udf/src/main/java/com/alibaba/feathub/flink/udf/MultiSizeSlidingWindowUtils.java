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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/** Utility method to apply sliding windows with different size. */
public class MultiSizeSlidingWindowUtils {

    /**
     * Apply sliding window with the {@link MultiSizeSlidingWindowKeyedProcessFunction} for the
     * given {@link Table} and {@link AggFieldsDescriptor}.
     *
     * @param tEnv The StreamTableEnvironment of the table.
     * @param table The input table.
     * @param keyFieldNames The names of the group by keys for the sliding window.
     * @param rowTimeFieldName The name of the row time field.
     * @param aggFieldsDescriptor The descriptor of the aggregation field in the sliding window.
     */
    public static Table applyMultiSizeSlidingWindowKeyedProcessFunction(
            StreamTableEnvironment tEnv,
            Table table,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            AggFieldsDescriptor aggFieldsDescriptor) {
        final ResolvedSchema resolvedSchema = table.getResolvedSchema();
        DataStream<Row> rowDataStream =
                tEnv.toChangelogStream(
                        table,
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        ChangelogMode.all());

        final TypeSerializer<Row> rowTypeSerializer =
                ExternalTypeInfo.<Row>of(resolvedSchema.toPhysicalRowDataType())
                        .createSerializer(null);
        rowDataStream =
                rowDataStream
                        .keyBy(
                                (KeySelector<Row, Row>)
                                        value ->
                                                Row.of(
                                                        Arrays.stream(keyFieldNames)
                                                                .map(value::getField)
                                                                .toArray()))
                        .process(
                                new MultiSizeSlidingWindowKeyedProcessFunction(
                                        aggFieldsDescriptor,
                                        rowTypeSerializer,
                                        rowTimeFieldName,
                                        stepSizeMs))
                        .returns(
                                ExternalTypeInfo.of(
                                        DataTypes.ROW(
                                                getTableFields(
                                                        keyFieldNames,
                                                        rowTimeFieldName,
                                                        aggFieldsDescriptor,
                                                        resolvedSchema))));

        table =
                tEnv.fromDataStream(
                        rowDataStream,
                        getResultTableSchema(
                                resolvedSchema,
                                aggFieldsDescriptor,
                                rowTimeFieldName,
                                keyFieldNames));
        for (AggFieldsDescriptor.AggFieldDescriptor aggFieldDescriptor :
                aggFieldsDescriptor.getAggFieldDescriptors()) {
            table =
                    table.addOrReplaceColumns(
                            $(aggFieldDescriptor.outFieldName)
                                    .cast(aggFieldDescriptor.outDataType)
                                    .as(aggFieldDescriptor.outFieldName));
        }
        return table;
    }

    private static List<DataTypes.Field> getTableFields(
            String[] keyFieldNames,
            String rowTimeFieldName,
            AggFieldsDescriptor aggFieldsDescriptor,
            ResolvedSchema resolvedSchema) {
        List<DataTypes.Field> keyFields =
                Arrays.stream(keyFieldNames)
                        .map(
                                fieldName ->
                                        DataTypes.FIELD(
                                                fieldName, getDataType(resolvedSchema, fieldName)))
                        .collect(Collectors.toList());
        List<DataTypes.Field> aggFieldDataTypes =
                aggFieldsDescriptor.getAggFieldDescriptors().stream()
                        .map(d -> DataTypes.FIELD(d.outFieldName, d.aggFunc.getResultDatatype()))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> fields = new LinkedList<>();
        fields.addAll(keyFields);
        fields.addAll(aggFieldDataTypes);
        fields.add(
                DataTypes.FIELD(rowTimeFieldName, getDataType(resolvedSchema, rowTimeFieldName)));
        return fields;
    }

    private static DataType getDataType(ResolvedSchema resolvedSchema, String fieldName) {
        return resolvedSchema
                .getColumn(fieldName)
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        String.format("Cannot find column %s.", fieldName)))
                .getDataType();
    }

    private static Schema getResultTableSchema(
            ResolvedSchema resolvedSchema,
            AggFieldsDescriptor descriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        final Schema.Builder builder = Schema.newBuilder();

        for (String keyFieldName : keyFieldNames) {
            builder.column(keyFieldName, getDataType(resolvedSchema, keyFieldName).notNull());
        }

        if (keyFieldNames.length > 0) {
            builder.primaryKey(keyFieldNames);
        }

        for (AggFieldsDescriptor.AggFieldDescriptor aggFieldDescriptor :
                descriptor.getAggFieldDescriptors()) {
            builder.column(
                    aggFieldDescriptor.outFieldName,
                    aggFieldDescriptor.aggFunc.getResultDatatype());
        }

        builder.column(rowTimeFieldName, getDataType(resolvedSchema, rowTimeFieldName));

        // Records are ordered by row time after sliding window.
        builder.watermark(
                rowTimeFieldName,
                String.format("`%s` - INTERVAL '0.001' SECONDS", rowTimeFieldName));
        return builder.build();
    }
}
