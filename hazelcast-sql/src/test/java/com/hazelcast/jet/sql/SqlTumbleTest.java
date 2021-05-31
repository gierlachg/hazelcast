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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.timestamp;
import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.watermark;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlTumbleTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_windowMetadata() {
        String name = createTable();

        try (SqlResult result = sqlService.execute("SELECT * FROM " +
                "TABLE(TUMBLE(TABLE " + name + " , DESCRIPTOR(ts), INTERVAL '1' SECOND))")
        ) {
            assertThat(result.getRowMetadata().findColumn("window_start")).isGreaterThan(-1);
            assertThat(result.getRowMetadata().getColumn(2).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
            assertThat(result.getRowMetadata().findColumn("window_end")).isGreaterThan(-1);
            assertThat(result.getRowMetadata().getColumn(3).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        }
    }

    @Test
    public void test_count() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), null},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestamp(0L), timestamp(2L), 1L),
                        new Row(timestamp(2L), timestamp(4L), 2L)
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(*) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.003' SECOND)) " +
                        "GROUP BY window_end, window_start",
                asList(
                        new Row(timestamp(0L), timestamp(3L), 3L),
                        new Row(timestamp(3L), timestamp(6L), 1L)
                )
        );
    }

    @Test
    public void test_emptyCount() {
        String name = createTable(watermark(10));

        assertEmptyResult(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_distinctCount() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Alice"},
                new Object[]{timestamp(2), "Bob"},
                new Object[]{timestamp(2), "Joey"},
                new Object[]{timestamp(3), "Alice"},
                new Object[]{timestamp(4), "Joey"},
                new Object[]{timestamp(5), "Alice"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 1L),
                        new Row(timestamp(2L), 3L),
                        new Row(timestamp(4L), 2L)
                )
        );
    }

    @Test
    public void test_groupCount() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 2L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupDistinctCount() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupExpressionCount() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s', COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice-s", 2L),
                        new Row(timestamp(2L), "Alice-s", 1L),
                        new Row(timestamp(2L), "Bob-s", 1L)
                )
        );
    }

    @Test
    public void test_groupCountExpression() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, 2 * COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 4L),
                        new Row(timestamp(2L), "Alice", 2L),
                        new Row(timestamp(2L), "Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupCountHaving() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(2), "Bob"},
                new Object[]{timestamp(3), "Bob"},
                new Object[]{timestamp(4), "Joey"},
                new Object[]{timestamp(5), "Joey"},
                new Object[]{timestamp(5), "Joey"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(*) c FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start HAVING c <> 2",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(4L), "Joey", 3L)
                )
        );
    }

    @Test
    public void test_filterCount() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Bob"},
                new Object[]{timestamp(3), "Alice"},
                new Object[]{timestamp(5), "Joey"},
                new Object[]{timestamp(5), "Joey"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "WHERE ts > '1970-01-01T00:00:00.000' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Bob", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_nested_filter() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, name, window_start window_start_inner FROM" +
                        "      TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) WHERE ts > '1970-01-01T00:00:00.000'" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_aggregate() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Bob"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Alice"},
                new Object[]{timestamp(5), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT name, window_start window_start_inner FROM " +
                        "       TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) GROUP BY name, window_start_inner" +
                        "   )" +
                        "   , DESCRIPTOR(window_start_inner)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(0L), "Bob", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(4L), "Bob", 1L)
                )
        );
    }

    @Test
    @Ignore // TODO: enable once we lay our hands on 1.24+, see https://issues.apache.org/jira/browse/CALCITE-4077
    public void test_nested_join() {
        instance().getMap("map").put(0L, "value-0");
        instance().getMap("map").put(1L, "value-1");
        instance().getMap("map").put(2L, "value-1");
        instance().getMap("map").put(3L, "value-1");

        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), "Bob"},
                new Object[]{timestamp(2), "Joey"},
                new Object[]{timestamp(3), "Alice"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, this, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, window_start window_start_inner, this FROM " +
                        "       TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) JOIN map ON ts = __key" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, this",
                asList(
                        new Row(timestamp(0L), "value-0", 1L),
                        new Row(timestamp(0L), "value-1", 1L),
                        new Row(timestamp(2L), "value-1", 2L)
                )
        );
    }

    @Test
    @Ignore // TODO: enable once we lay our hands on 1.24+
    public void test_namedParameters() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   \"data\" => TABLE " + name +
                        "   , timecol => DESCRIPTOR(ts)" +
                        "   , size => INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestamp(0L), timestamp(2L), 1L),
                        new Row(timestamp(2L), timestamp(4L), 2L)
                )
        );
    }

    @Test
    @Ignore // TODO: ???
    public void test_dynamicParameters() {
        String name = createTable(
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"},
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), ?)) " +
                        "GROUP BY window_start, window_end",
                singletonList(new SqlDaySecondInterval(2)),
                asList(
                        new Row(timestamp(0L), timestamp(2L), 1L),
                        new Row(timestamp(2L), timestamp(4L), 2L)
                )
        );
    }

    @Test
    public void test_batchSource() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(QueryDataTypeFamily.TIMESTAMP, QueryDataTypeFamily.VARCHAR),
                new Object[]{timestamp(0), "Alice"},
                new Object[]{timestamp(1), null},
                new Object[]{timestamp(2), "Alice"},
                new Object[]{timestamp(3), "Bob"}
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestamp(0L), timestamp(2L), 1L),
                        new Row(timestamp(2L), timestamp(4L), 2L)
                )
        );
    }

    @Test
    public void when_projectionIsUsedOnWindowEdge_then_throws() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT window_start + INTERVAL '0.001' SECOND FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start + INTERVAL '0.001' SECOND"
        )).hasMessageContaining("Invalid window aggregation");
    }

    private static String createTable(Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(QueryDataTypeFamily.TIMESTAMP, QueryDataTypeFamily.VARCHAR),
                values
        );
        return name;
    }
}
