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
import { useCallback, useMemo, useState, useEffect } from 'react';
import { t, SupersetClient } from '@superset-ui/core';
import { AsyncSelect } from '@superset-ui/core/components';
import ControlHeader from 'src/explore/components/ControlHeader';
import rison from 'rison';

export interface ChartOption {
  value: number;
  label: string;
  viz_type?: string;
}

export interface ChartSelectProps {
  value?: number | null;
  onChange: (value: number | null) => void;
  datasetId?: number;
  placeholder?: string;
  clearable?: boolean;
  ariaLabel?: string;
  label?: React.ReactNode;
}

/**
 * A reusable chart selection component that loads charts from the API
 * @param value - The selected chart ID
 * @param onChange - Callback when selection changes
 * @param datasetId - Optional dataset ID to filter charts
 * @param placeholder - Optional placeholder text
 * @param clearable - Whether the selection can be cleared
 * @param ariaLabel - ARIA label for accessibility
 * @param label - Form label (passed by Field component)
 */
export default function ChartSelect({
  value,
  onChange,
  datasetId,
  placeholder = t('Select a chart'),
  clearable = true,
  ariaLabel = t('Select drill-to-details chart'),
  label,
}: ChartSelectProps) {
  const loadChartOptions = useCallback(
    async (input = '', page = 0, pageSize = 50) => {
      const filters: any[] = [];

      if (input) {
        filters.push({
          col: 'slice_name',
          opr: 'ct',
          value: input,
        });
      }

      if (datasetId) {
        filters.push({
          col: 'datasource_id',
          opr: 'eq',
          value: datasetId,
        });
        filters.push({
          col: 'datasource_type',
          opr: 'eq',
          value: 'table',
        });
      }

      const query = rison.encode({
        filters,
        page,
        page_size: pageSize,
        order_column: 'slice_name',
        order_direction: 'asc',
      });

      const response = await SupersetClient.get({
        endpoint: `/api/v1/chart/?q=${query}`,
      });

      const charts = response.json.result.map((chart: any) => ({
        value: chart.id,
        label: `${chart.slice_name} (${chart.viz_type})`,
        viz_type: chart.viz_type,
      }));

      return {
        data: charts,
        totalCount: response.json.count,
      };
    },
    [datasetId],
  );

  const [selectedChart, setSelectedChart] = useState<ChartOption | undefined>(
    undefined,
  );
  const [isLoadingSelected, setIsLoadingSelected] = useState(false);

  // Load the selected chart details when value changes
  useEffect(() => {
    if (!value) {
      setSelectedChart(undefined);
      return;
    }

    // If we already have the chart loaded, don't reload
    if (selectedChart && selectedChart.value === value) {
      return;
    }

    setIsLoadingSelected(true);
    SupersetClient.get({
      endpoint: `/api/v1/chart/${value}`,
    })
      .then(({ json }) => {
        const chart = json.result;
        setSelectedChart({
          value: chart.id,
          label: `${chart.slice_name} (${chart.viz_type})`,
          viz_type: chart.viz_type,
        });
      })
      .catch(error => {
        console.error('Failed to load selected chart:', error);
        // Fallback to showing the ID if we can't load the chart
        setSelectedChart({
          value,
          label: `Chart ${value}`,
        });
      })
      .finally(() => {
        setIsLoadingSelected(false);
      });
  }, [value]);

  const selectedValue = useMemo(() => {
    if (!value) return undefined;
    if (isLoadingSelected) {
      return { value, label: t('Loading...') };
    }
    return selectedChart || { value, label: t('Loading...') };
  }, [value, selectedChart, isLoadingSelected]);

  const handleChange = useCallback(
    (newValue: any) => {
      // AsyncSelect can pass the value in different formats
      if (newValue === null || newValue === undefined) {
        onChange(null);
      } else if (typeof newValue === 'object' && 'value' in newValue) {
        onChange(newValue.value);
      } else {
        onChange(newValue);
      }
    },
    [onChange],
  );

  return (
    <div>
      {label && <ControlHeader label={label} />}
      <AsyncSelect
        ariaLabel={ariaLabel}
        value={selectedValue}
        onChange={handleChange}
        options={loadChartOptions}
        placeholder={placeholder}
        allowClear={clearable}
        lazyLoading
      />
    </div>
  );
}
