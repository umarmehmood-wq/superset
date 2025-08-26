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
import {
  render,
  screen,
  userEvent,
  waitFor,
} from 'spec/helpers/testing-library';
import { Column, JsonObject } from '@superset-ui/core';
import { Datasource } from 'src/dashboard/types';
import { ColumnSelect } from './ColumnSelect';

const mockDataset: Datasource = {
  id: 123,
  uid: '123__table',
  table_name: 'test_table',
  type: 'table',
  columns: [
    { column_name: 'column_name_01', is_dttm: true } as Column,
    { column_name: 'column_name_02', is_dttm: false } as Column,
    { column_name: 'column_name_03', is_dttm: false } as Column,
  ],
  metrics: [],
  column_types: [],
  column_formats: {},
  verbose_map: {},
  main_dttm_col: '',
  datasource_name: 'test_table',
  description: null,
} as Datasource;

const mockDataset2: Datasource = {
  id: 456,
  uid: '456__table',
  table_name: 'test_table_2',
  type: 'table',
  columns: [
    { column_name: 'column_name_04', is_dttm: false } as Column,
    { column_name: 'column_name_05', is_dttm: false } as Column,
    { column_name: 'column_name_06', is_dttm: false } as Column,
  ],
  metrics: [],
  column_types: [],
  column_formats: {},
  verbose_map: {},
  main_dttm_col: '',
  datasource_name: 'test_table_2',
  description: null,
} as Datasource;

const createProps = (extraProps: JsonObject = {}) => ({
  filterId: 'filterId',
  form: { getFieldValue: jest.fn(), setFields: jest.fn() },
  datasetId: 123,
  value: 'column_name_01',
  onChange: jest.fn(),
  dataset: mockDataset,
  ...extraProps,
});

test('Should render', async () => {
  const props = createProps();
  const { container } = render(<ColumnSelect {...(props as any)} />, {
    useRedux: true,
  });
  expect(container.children).toHaveLength(1);
  userEvent.type(screen.getByRole('combobox'), 'column_name');
  await waitFor(() => {
    expect(
      screen.getByRole('option', {
        name: 'column_name_01',
      }),
    ).toBeInTheDocument();
  });
  await waitFor(() => {
    expect(screen.getByTitle('column_name_02')).toBeInTheDocument();
  });
  await waitFor(() => {
    expect(screen.getByTitle('column_name_03')).toBeInTheDocument();
  });
});

test('Should call "setFields" when "datasetId" changes', () => {
  const props = createProps();
  const { rerender } = render(<ColumnSelect {...(props as any)} />, {
    useRedux: true,
  });
  expect(props.form.setFields).not.toHaveBeenCalled();

  props.datasetId = 456;
  props.dataset = mockDataset2;
  rerender(<ColumnSelect {...(props as any)} />);

  expect(props.form.setFields).toHaveBeenCalled();
});

test('Should handle missing dataset gracefully', async () => {
  const props = createProps({
    dataset: undefined,
    datasetId: 789,
  });

  const { container } = render(<ColumnSelect {...(props as any)} />, {
    useRedux: true,
  });
  expect(container.children).toHaveLength(1);
  expect(screen.queryByRole('option')).not.toBeInTheDocument();
});

test('Should filter results', async () => {
  const props = createProps({
    filterValues: (column: Column) => column.is_dttm,
  });
  const { container } = render(<ColumnSelect {...(props as any)} />, {
    useRedux: true,
  });
  expect(container.children).toHaveLength(1);
  userEvent.type(screen.getByRole('combobox'), 'column_name');
  await waitFor(() => {
    expect(
      screen.getByRole('option', {
        name: 'column_name_01',
      }),
    ).toBeInTheDocument();
  });
  await waitFor(() => {
    expect(screen.queryByTitle('column_name_02')).not.toBeInTheDocument();
  });
  await waitFor(() => {
    expect(screen.queryByTitle('column_name_03')).not.toBeInTheDocument();
  });
});
