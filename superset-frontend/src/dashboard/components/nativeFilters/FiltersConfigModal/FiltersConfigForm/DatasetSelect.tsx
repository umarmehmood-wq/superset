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
import { useMemo } from 'react';
import { t } from '@superset-ui/core';
import { Select } from '@superset-ui/core/components';
import {
  Dataset,
  DatasetSelectLabel,
} from 'src/features/datasets/DatasetSelectLabel';
import { Datasource } from 'src/dashboard/types';

interface DatasetSelectProps {
  onChange: (value: { label: string; value: number }) => void;
  value?: { label: string; value: number };
  datasets: Datasource[];
}

const DatasetSelect = ({ onChange, value, datasets }: DatasetSelectProps) => {
  const datasetOptions = useMemo(
    () =>
      datasets
        .map(dataset => ({
          label: DatasetSelectLabel(dataset as Dataset),
          value: dataset.id,
        }))
        .sort((a, b) => {
          // Sort by table name for consistency
          const labelA = typeof a.label === 'string' ? a.label : '';
          const labelB = typeof b.label === 'string' ? b.label : '';
          return labelA.localeCompare(labelB);
        }),
    [datasets],
  );

  return (
    <Select
      ariaLabel={t('Dataset')}
      value={value}
      options={datasetOptions}
      onChange={onChange}
      notFoundContent={t('No compatible datasets found')}
      placeholder={t('Select a dataset')}
      showSearch
      filterOption={(input, option) => {
        const label = typeof option?.label === 'string' ? option.label : '';
        return label.toLowerCase().includes(input.toLowerCase());
      }}
    />
  );
};

const MemoizedSelect = (props: DatasetSelectProps) =>
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useMemo(() => <DatasetSelect {...props} />, [props.datasets, props.value]);

export default MemoizedSelect;
