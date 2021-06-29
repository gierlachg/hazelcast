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

package com.hazelcast.sql.impl.calcite.opt.metadata;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.lang.reflect.Method;

public abstract class HazelcastRelMetadata {

    public interface WindowPropertiesMetadata extends Metadata {

        Method METHOD = Types.lookupMethod(WindowPropertiesMetadata.class, "extractWindowProperties");

        MetadataDef<WindowPropertiesMetadata> DEF = MetadataDef.of(
                WindowPropertiesMetadata.class,
                WindowPropertiesMetadata.Handler.class,
                METHOD
        );

        @SuppressWarnings("unused")
        WindowProperties extractWindowProperties();

        interface Handler extends MetadataHandler<WindowPropertiesMetadata> {

            WindowProperties extractWindowProperties(RelNode rel, RelMetadataQuery mq);
        }
    }
}
