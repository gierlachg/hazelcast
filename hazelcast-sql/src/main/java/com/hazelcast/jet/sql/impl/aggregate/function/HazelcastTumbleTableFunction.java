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

package com.hazelcast.jet.sql.impl.aggregate.function;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.DescriptorOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.SqlDaySecondIntervalOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandCheckerProgram;
import com.hazelcast.sql.impl.calcite.validate.operand.RowOperandChecker;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public class HazelcastTumbleTableFunction extends HazelcastWindowTableFunction {

    public HazelcastTumbleTableFunction() {
        super(SqlKind.TUMBLE.name());
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(3);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        OperandCheckerProgram checker = new OperandCheckerProgram(
                RowOperandChecker.INSTANCE,
                DescriptorOperandChecker.INSTANCE,
                SqlDaySecondIntervalOperandChecker.INSTANCE
        );
        if (!checker.check(binding, throwOnFailure)) {
            return false;
        }

        if (!checkTimestampColumnDescriptorOperand(binding, 1)) {
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        return true;
    }
}
