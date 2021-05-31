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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.core.SlidingWindowPolicy;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class WindowProperties {

    private final Map<Integer, WindowProperty> propertiesByIndex;

    public WindowProperties(WindowProperty... properties) {
        this(stream(properties).collect(toMap(WindowProperty::index, identity())));
    }

    private WindowProperties(Map<Integer, WindowProperty> propertiesByIndex) {
        this.propertiesByIndex = Collections.unmodifiableMap(propertiesByIndex);
    }

    public WindowProperties merge(WindowProperties other) {
        if (other == null || other.propertiesByIndex.isEmpty()) {
            return this;
        }

        Map<Integer, WindowProperty> propertiesByIndex = new HashMap<>(this.propertiesByIndex);
        for (WindowProperty otherProperty : other.propertiesByIndex.values()) {
            assert !propertiesByIndex.containsKey(otherProperty.index());
            propertiesByIndex.put(otherProperty.index(), otherProperty);
        }
        return new WindowProperties(propertiesByIndex);
    }

    public WindowProperties retain(Set<Integer> indices) {
        Map<Integer, WindowProperty> propertiesByIndex = new HashMap<>(this.propertiesByIndex);
        propertiesByIndex.keySet().retainAll(indices);
        return new WindowProperties(propertiesByIndex);
    }

    public WindowProperty findFirst(List<Integer> indices) {
        for (int index : indices) {
            WindowProperty property = propertiesByIndex.get(index);
            if (property != null) {
                return property;
            }
        }
        // TODO: is it possible to detect it earlier?
        throw new IllegalStateException("Invalid window aggregation. " +
                "Use either 'window_start' or 'window_end' in GROUP BY clause.");
    }

    public Stream<WindowProperty> getProperties() {
        return propertiesByIndex.values().stream();
    }

    public interface WindowProperty {

        int index();

        ToLongFunctionEx<Object[]> timestampFn();

        SupplierEx<SlidingWindowPolicy> windowPolicyFn();

        WindowProperty withIndex(int index);
    }

    public static class WindowStartProperty implements WindowProperty {

        private final int index;
        private final SlidingWindowPolicy windowPolicy;

        public WindowStartProperty(int index, SlidingWindowPolicy windowPolicy) {
            this.index = index;
            this.windowPolicy = windowPolicy;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public ToLongFunctionEx<Object[]> timestampFn() {
            int index = this.index;
            return row -> {
                // TODO: ...
                LocalDateTime timestamp = (LocalDateTime) row[index];
                return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
            };
        }

        @Override
        public SupplierEx<SlidingWindowPolicy> windowPolicyFn() {
            SlidingWindowPolicy windowPolicy = this.windowPolicy;
            return () -> windowPolicy;
        }

        @Override
        public WindowStartProperty withIndex(int index) {
            return new WindowStartProperty(index, windowPolicy);
        }
    }

    public static class WindowEndProperty implements WindowProperty {

        private final int index;
        private final SlidingWindowPolicy windowPolicy;

        public WindowEndProperty(int index, SlidingWindowPolicy windowPolicy) {
            this.index = index;
            this.windowPolicy = windowPolicy;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public ToLongFunctionEx<Object[]> timestampFn() {
            int index = this.index;
            SlidingWindowPolicy windowPolicy = this.windowPolicy;
            return row -> {
                // TODO: ...
                LocalDateTime timestamp = (LocalDateTime) row[index];
                return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli() - windowPolicy.windowSize();
            };
        }

        @Override
        public SupplierEx<SlidingWindowPolicy> windowPolicyFn() {
            SlidingWindowPolicy windowPolicy = this.windowPolicy;
            return () -> windowPolicy;
        }

        @Override
        public WindowEndProperty withIndex(int index) {
            return new WindowEndProperty(index, windowPolicy);
        }
    }
}
