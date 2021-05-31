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

package com.hazelcast.sql.impl.calcite.opt.metadata;

import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.metadata.WindowProperties.WindowEndProperty;
import com.hazelcast.sql.impl.calcite.opt.metadata.WindowProperties.WindowProperty;
import com.hazelcast.sql.impl.calcite.opt.metadata.WindowProperties.WindowStartProperty;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public final class HazelcastRelMdWindowProperties implements MetadataHandler<HazelcastRelMetadata.WindowPropertiesMetadata> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            HazelcastRelMetadata.WindowPropertiesMetadata.METHOD,
            new HazelcastRelMdWindowProperties()
    );

    private HazelcastRelMdWindowProperties() {
    }

    @Override
    public MetadataDef<HazelcastRelMetadata.WindowPropertiesMetadata> getDef() {
        return HazelcastRelMetadata.WindowPropertiesMetadata.DEF;
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(SlidingWindowPhysicalRel rel, RelMetadataQuery mq) {
        int fieldCount = rel.getRowType().getFieldCount();
        WindowProperties windowProperties = new WindowProperties(
                new WindowStartProperty(fieldCount - 2, rel.windowPolicy()),
                new WindowEndProperty(fieldCount - 1, rel.windowPolicy())
        );

        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties inputWindowProperties = query.extractWindowProperties(rel.getInput());

        return windowProperties.merge(inputWindowProperties);
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(Project rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties inputWindowProperties = query.extractWindowProperties(rel.getInput());
        if (inputWindowProperties == null) {
            return null;
        } else {
            Map<Integer, List<Integer>> projections = toProjections(rel.getProjects());
            WindowProperty[] windowProperties = inputWindowProperties.getProperties()
                    .flatMap(property ->
                            projections.getOrDefault(property.index(), emptyList()).stream().map(property::withIndex)
                    ).toArray(WindowProperty[]::new);
            return new WindowProperties(windowProperties);
        }
    }

    private Map<Integer, List<Integer>> toProjections(List<RexNode> projects) {
        final class ProjectFieldVisitor extends RexVisitorImpl<Integer> {

            private ProjectFieldVisitor() {
                super(false);
            }

            @Override
            public Integer visitInputRef(RexInputRef input) {
                return input.getIndex();
            }

            @Override
            public Integer visitCall(RexCall call) {
                if (call.getKind() == SqlKind.AS) {
                    return call.getOperands().get(0).accept(this);
                }

                // any operation on the window field loses the window attribute, even if it's a simple cast

                return null;
            }
        }

        RexVisitor<Integer> visitor = new ProjectFieldVisitor();
        Map<Integer, List<Integer>> projections = new HashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            Integer index = projects.get(i).accept(visitor);
            if (index != null) {
                projections.computeIfAbsent(index, key -> new ArrayList<>()).add(i);
            }
        }
        return projections;
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(Aggregate rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties inputWindowProperties = query.extractWindowProperties(rel.getInput());
        return inputWindowProperties.retain(rel.getGroupSet().asSet());
    }

    // TODO: support join once we lay our hands on 1.24+, see https://issues.apache.org/jira/browse/CALCITE-4077

    // i.e. Filter, AggregateAccumulateByKeyPhysicalRel, AggregateAccumulatePhysicalRel
    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(SingleRel rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractWindowProperties(rel.getInput());
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
        return query.extractWindowProperties(rel);
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(RelNode rel, RelMetadataQuery mq) {
        return null;
    }
}
