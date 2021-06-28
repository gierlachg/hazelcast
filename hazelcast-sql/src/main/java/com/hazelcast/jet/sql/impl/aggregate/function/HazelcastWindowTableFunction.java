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
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastOperandTypeCheckerAware;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

abstract class HazelcastWindowTableFunction extends SqlWindowTableFunction implements HazelcastOperandTypeCheckerAware {

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
            binding -> {
                RelDataType inputRowType = binding.getOperandType(0);
                List<RelDataTypeField> returnFields = new ArrayList<>(inputRowType.getFieldList());
                RelDataType timestampType = binding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                returnFields.add(new RelDataTypeFieldImpl("window_start", returnFields.size(), timestampType));
                returnFields.add(new RelDataTypeFieldImpl("window_end", returnFields.size(), timestampType));
                return new RelRecordType(inputRowType.getStructKind(), returnFields);
            };

    // dirty hack around SqlWindowTableFunction's window edges type, Calcite 1.24+ provides proper extension points
    // till then it is what it is...
    static {
        try {
            Field returnTypeInferenceField = SqlWindowTableFunction.class.getField("ARG0_TABLE_FUNCTION_WINDOWING");
            returnTypeInferenceField.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(returnTypeInferenceField, returnTypeInferenceField.getModifiers() & ~Modifier.FINAL);

            returnTypeInferenceField.set(null, RETURN_TYPE_INFERENCE);
        } catch (Exception e) {
            throw new ExceptionInInitializerError("Unable to initialize "
                    + HazelcastWindowTableFunction.class.getName() + ": " + e.getMessage());
        }
    }

    protected HazelcastWindowTableFunction(String name) {
        super(name);
    }

    @Override
    public final boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        HazelcastCallBinding bindingOverride = prepareBinding(binding);

        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    protected abstract boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure);

    protected boolean checkTimestampColumnDescriptorOperand(SqlCallBinding binding, int position) {
        SqlCall descriptor = (SqlCall) binding.operands().get(position);
        List<SqlNode> descriptorIdentifiers = descriptor.getOperandList();
        if (descriptorIdentifiers.size() != 1) {
            return false;
        }

        SqlValidator validator = binding.getValidator();
        SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();

        String timestampColumnName = ((SqlIdentifier) descriptorIdentifiers.get(0)).getSimple();
        return validator.getValidatedNodeType(binding.operands().get(0)).getFieldList().stream()
                .filter(field -> matcher.matches(field.getName(), timestampColumnName))
                .anyMatch(field -> field.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }
}
