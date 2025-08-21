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
import { render, screen, waitFor } from 'spec/helpers/testing-library';
import userEvent from '@testing-library/user-event';
import fetchMock from 'fetch-mock';
import ChartSelect from './ChartSelect';

const mockChartData = {
  count: 2,
  result: [
    {
      id: 1,
      slice_name: 'Test Chart 1',
      viz_type: 'table',
    },
    {
      id: 2,
      slice_name: 'Test Chart 2',
      viz_type: 'bar',
    },
  ],
};

describe('ChartSelect', () => {
  beforeEach(() => {
    fetchMock.reset();
  });

  afterEach(() => {
    fetchMock.reset();
  });

  it('renders without crashing', () => {
    const onChange = jest.fn();
    render(<ChartSelect onChange={onChange} />);
    expect(screen.getByRole('combobox')).toBeInTheDocument();
  });

  it('displays placeholder text', () => {
    const onChange = jest.fn();
    render(
      <ChartSelect onChange={onChange} placeholder="Custom placeholder" />,
    );
    expect(screen.getByText('Custom placeholder')).toBeInTheDocument();
  });

  it('loads and displays chart options', async () => {
    fetchMock.get('glob:*/api/v1/chart/*', mockChartData);
    const onChange = jest.fn();

    render(<ChartSelect onChange={onChange} />);

    const select = screen.getByRole('combobox');
    userEvent.click(select);

    await waitFor(() => {
      expect(screen.getByText('Test Chart 1 (table)')).toBeInTheDocument();
      expect(screen.getByText('Test Chart 2 (bar)')).toBeInTheDocument();
    });
  });

  it('filters charts by dataset ID when provided', async () => {
    const filteredUrl = /.*api\/v1\/chart.*datasource_id.*eq.*123/;
    fetchMock.get(filteredUrl, mockChartData);

    const onChange = jest.fn();
    render(<ChartSelect onChange={onChange} datasetId={123} />);

    const select = screen.getByRole('combobox');
    userEvent.click(select);

    await waitFor(() => {
      expect(fetchMock.called(filteredUrl)).toBe(true);
    });
  });

  it('calls onChange with chart ID when a chart is selected', async () => {
    fetchMock.get('glob:*/api/v1/chart/*', mockChartData);
    const onChange = jest.fn();

    render(<ChartSelect onChange={onChange} />);

    const select = screen.getByRole('combobox');
    userEvent.click(select);

    await waitFor(() => {
      expect(screen.getByText('Test Chart 1 (table)')).toBeInTheDocument();
    });

    userEvent.click(screen.getByText('Test Chart 1 (table)'));

    expect(onChange).toHaveBeenCalledWith(1);
  });

  it('calls onChange with null when cleared', async () => {
    fetchMock.get('glob:*/api/v1/chart/*', mockChartData);
    // Mock the individual chart fetch for the selected value
    fetchMock.get('glob:*/api/v1/chart/1', {
      result: {
        id: 1,
        slice_name: 'Test Chart 1',
        viz_type: 'table',
      },
    });
    const onChange = jest.fn();

    render(<ChartSelect value={1} onChange={onChange} clearable />);

    // Wait for the component to load the selected chart
    await waitFor(() => {
      expect(screen.getByText('Test Chart 1 (table)')).toBeInTheDocument();
    });

    // Find and click the clear button using the correct selector
    const clearButton = screen.getByLabelText('close-circle');
    await userEvent.click(clearButton);

    expect(onChange).toHaveBeenCalledWith(null);
  });

  it('searches charts when user types', async () => {
    fetchMock.get('glob:*/api/v1/chart/*', mockChartData);

    const onChange = jest.fn();
    render(<ChartSelect onChange={onChange} />);

    const select = screen.getByRole('combobox');
    await userEvent.type(select, 'test');

    // Just verify that the input receives the typed text
    await waitFor(() => {
      expect(select).toHaveValue('test');
    });
  });

  it('respects clearable prop', async () => {
    fetchMock.get('glob:*/api/v1/chart/*', mockChartData);
    // Mock the individual chart fetch for the selected value
    fetchMock.get('glob:*/api/v1/chart/1', {
      result: {
        id: 1,
        slice_name: 'Test Chart 1',
        viz_type: 'table',
      },
    });
    const onChange = jest.fn();

    // When clearable is false
    const { rerender } = render(
      <ChartSelect value={1} onChange={onChange} clearable={false} />,
    );

    await waitFor(() => {
      expect(screen.getByText('Test Chart 1 (table)')).toBeInTheDocument();
    });

    expect(screen.queryByLabelText('close-circle')).not.toBeInTheDocument();

    // When clearable is true
    rerender(<ChartSelect value={1} onChange={onChange} clearable />);

    await waitFor(() => {
      expect(screen.getByLabelText('close-circle')).toBeInTheDocument();
    });
  });

  it('uses custom aria label', () => {
    const onChange = jest.fn();
    render(<ChartSelect onChange={onChange} ariaLabel="Custom aria label" />);

    const select = screen.getByRole('combobox');
    expect(select).toHaveAttribute('aria-label', 'Custom aria label');
  });

  it('loads and displays selected chart name', async () => {
    // Mock the individual chart fetch for the selected value
    fetchMock.get('glob:*/api/v1/chart/2', {
      result: {
        id: 2,
        slice_name: 'Selected Chart',
        viz_type: 'bar',
      },
    });
    const onChange = jest.fn();

    render(<ChartSelect value={2} onChange={onChange} />);

    // Should show loading initially
    await waitFor(() => {
      expect(screen.getByText('Loading...')).toBeInTheDocument();
    });

    // Then should load and show the actual chart name
    await waitFor(() => {
      expect(screen.getByText('Selected Chart (bar)')).toBeInTheDocument();
    });
  });
});
