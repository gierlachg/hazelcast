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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class RowOperandChecker implements OperandChecker {

    public static final RowOperandChecker INSTANCE = new RowOperandChecker();

    private RowOperandChecker() {
    }

    @Override
    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure, int operandIndex) {
        HazelcastSqlValidator validator = callBinding.getValidator();

        SqlNode operand = callBinding.getCall().operand(operandIndex);

        RelDataType type = validator.deriveType(callBinding.getScope(), operand);
        if (type.getSqlTypeName() == SqlTypeName.ROW) {
            return true;
        }

        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}
