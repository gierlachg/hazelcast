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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

public final class DescriptorOperandChecker implements OperandChecker {

    public static final DescriptorOperandChecker INSTANCE = new DescriptorOperandChecker();

    private DescriptorOperandChecker() {
    }

    @Override
    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure, int operandIndex) {
        SqlNode operand = callBinding.getCall().operand(operandIndex);

        if (operand.getKind() == SqlKind.DESCRIPTOR) {
            return true;
        }

        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}
