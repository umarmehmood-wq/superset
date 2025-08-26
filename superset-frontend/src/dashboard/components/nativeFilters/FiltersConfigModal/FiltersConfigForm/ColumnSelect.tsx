/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useCallback, useMemo, useEffect } from 'react';
import { Column, ensureIsArray, useChangeEffect, t } from '@superset-ui/core';
import { type FormInstance, Select } from '@superset-ui/core/components';
import { Datasource } from 'src/dashboard/types';
import { NativeFiltersForm, NativeFiltersFormItem } from '../types';

interface ColumnSelectProps {
  allowClear?: boolean;
  filterValues?: (column: Column) => boolean;
  form: FormInstance<NativeFiltersForm>;
  formField?: keyof NativeFiltersFormItem;
  filterId: string;
  datasetId?: number;
  value?: string | string[];
  onChange?: (value: string) => void;
  mode?: 'multiple';
  dataset?: Datasource;
}

/** Special purpose AsyncSelect that selects a column from a dataset */
// eslint-disable-next-line import/prefer-default-export
export function ColumnSelect({
  allowClear = false,
  filterValues = () => true,
  form,
  formField = 'column',
  filterId,
  datasetId,
  value,
  onChange,
  mode,
  dataset, // 🔥 MURDER CACHE: Accept dataset via props
}: ColumnSelectProps) {
  const resetColumnField = useCallback(() => {
    form.setFields([
      { name: ['filters', filterId, formField], touched: false, value: null },
    ]);
  }, [form, filterId, formField]);

  const columns = useMemo(() => dataset?.columns || [], [dataset?.columns]);

  const options = useMemo(
    () =>
      ensureIsArray(columns)
        .filter(filterValues)
        .map((col: Column) => col.column_name)
        .map((column: string) => ({ label: column, value: column })),
    [columns, filterValues],
  );

  const currentFilterType =
    form.getFieldValue('filters')?.[filterId].filterType;
  const currentColumn = useMemo(
    () => columns?.find(column => column.column_name === value),
    [columns, value],
  );

  useEffect(() => {
    if (currentColumn && !filterValues(currentColumn)) {
      resetColumnField();
    }
  }, [currentColumn, currentFilterType, resetColumnField, filterValues]);

  // Reset column when dataset changes
  useChangeEffect(datasetId, previous => {
    if (previous != null && previous !== datasetId) {
      resetColumnField();
    }
  });

  // Validate that current value exists in new dataset
  useEffect(() => {
    if (value && columns.length > 0) {
      const lookupValue = Array.isArray(value) ? value : [value];
      const valueExists = columns.some((column: Column) =>
        lookupValue?.includes(column.column_name),
      );
      if (!valueExists) {
        resetColumnField();
      }
    }
  }, [columns, value, resetColumnField]);

  return (
    <Select
      mode={mode}
      value={mode === 'multiple' ? value || [] : value}
      ariaLabel={t('Column select')}
      loading={false}
      onChange={onChange}
      options={options}
      placeholder={t('Select a column')}
      notFoundContent={t('No compatible columns found')}
      showSearch
      allowClear={allowClear}
    />
  );
}
