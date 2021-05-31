/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.validate.JetSqlOperatorTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;

public class SlidingWindowPhysicalRel extends TableFunctionScan implements PhysicalRel {

    private final int timeFieldIndex;
    private final SlidingWindowPolicy windowPolicy;

    SlidingWindowPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RexNode rexCall,
            Type elementType,
            RelDataType rowType,
            Set<RelColumnMapping> columnMappings
    ) {
        super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);

        // TODO: RexToExpressionVisitor ???
        RexCall call = (RexCall) getCall();
        List<RexNode> operands = call.getOperands();
        if (call.getOperator() == JetSqlOperatorTable.TUMBLE) {
            this.timeFieldIndex = ((RexInputRef) ((RexCall) operands.get(0)).getOperands().get(0)).getIndex();
            this.windowPolicy = tumblingWinPolicy(((BigDecimal) ((RexLiteral) operands.get(1)).getValue()).longValue());
        } else {
            throw new IllegalArgumentException("Unsupported operator: " + call.getOperator());
        }
    }

    public FunctionEx<Object[], LocalDateTime> windowStartFn() {
        int timeFieldIndex = this.timeFieldIndex;
        SlidingWindowPolicy windowPolicy = this.windowPolicy;
        return row -> {
            // TODO: ...
            LocalDateTime timestamp = (LocalDateTime) row[timeFieldIndex];
            long timestampMillis = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
            long windowStartMillis = windowPolicy.floorFrameTs(timestampMillis);
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(windowStartMillis), ZoneOffset.UTC);
        };
    }

    public FunctionEx<Object[], LocalDateTime> windowEndFn() {
        int timeFieldIndex = this.timeFieldIndex;
        SlidingWindowPolicy windowPolicy = this.windowPolicy;
        return row -> {
            // TODO: ...
            LocalDateTime timestamp = (LocalDateTime) row[timeFieldIndex];
            long timestampMillis = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
            long windowEndMillis = windowPolicy.higherFrameTs(timestampMillis);
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEndMillis), ZoneOffset.UTC);
        };
    }

    public SlidingWindowPolicy windowPolicy() {
        return windowPolicy;
    }

    public RelNode getInput() {
        return sole(getInputs());
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onSlidingWindow(this);
    }

    @Override
    public TableFunctionScan copy(
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RexNode rexCall,
            Type elementType,
            RelDataType rowType,
            Set<RelColumnMapping> columnMappings
    ) {
        return new SlidingWindowPhysicalRel(getCluster(), traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    }
}
