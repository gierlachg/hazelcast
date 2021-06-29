/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static java.util.Collections.emptyList;

public class ProjectLogicalRel extends Project implements LogicalRel {

    ProjectLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType
    ) {
        super(cluster, traits, emptyList(), input, projects, rowType);
    }

    @Override
    public final Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new ProjectLogicalRel(getCluster(), traitSet, input, getProjects(), rowType);
    }
}
