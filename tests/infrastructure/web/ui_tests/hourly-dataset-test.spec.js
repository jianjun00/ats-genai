const { test, expect } = require('@playwright/test');

test.describe('Hourly Dataset Data Display Test', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to training EDA page
    await page.goto('http://localhost:3000/training-eda');
    
    // Wait for page to load
    await page.waitForLoadState('networkidle');
  });

  test('should detect hourly dataset data loading issue', async ({ page }) => {
    let apiResponse = null;
    let apiError = null;

    // Intercept API requests to capture responses
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && response.url().includes('/data')) {
        apiResponse = response;
        if (response.ok()) {
          try {
            const data = await response.json();
            // Store API response data for analysis
            await page.evaluate((responseData) => {
              window.lastApiResponse = responseData;
            }, data);
          } catch (e) {
            console.log('Failed to parse API response:', e);
          }
        } else {
          apiError = {
            status: response.status(),
            statusText: response.statusText(),
            url: response.url()
          };
        }
      }
    });

    // Wait for datasets to load
    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    
    // Find and select hourly dataset
    const datasetSelect = page.locator('.dataset-selection select');
    const options = await datasetSelect.locator('option').all();
    
    let hourlyDatasetFound = false;
    let hourlyDatasetValue = null;
    
    for (let option of options) {
      const text = await option.textContent();
      if (text && text.includes('hourly')) {
        hourlyDatasetFound = true;
        hourlyDatasetValue = await option.getAttribute('value');
        console.log(`Found hourly dataset: ${text} (value: ${hourlyDatasetValue})`);
        break;
      }
    }

    expect(hourlyDatasetFound, 'Should find hourly dataset in dropdown').toBeTruthy();
    expect(hourlyDatasetValue, 'Hourly dataset should have valid ID').toBeTruthy();

    // Select the hourly dataset
    await datasetSelect.selectOption(hourlyDatasetValue);
    
    // Wait for data to load
    await page.waitForTimeout(3000);
    
    // Check if API was called
    expect(apiResponse, 'API should be called for hourly dataset').toBeTruthy();
    
    if (apiError) {
      throw new Error(`API Error: ${apiError.status} ${apiError.statusText} for ${apiError.url}`);
    }

    // Get the API response data from page context
    const responseData = await page.evaluate(() => window.lastApiResponse);
    
    console.log('API Response Analysis:');
    console.log(`- Total Count: ${responseData?.total_count}`);
    console.log(`- Data Array Length: ${responseData?.data?.length}`);
    console.log(`- Current Page: ${responseData?.current_page}`);
    console.log(`- Total Pages: ${responseData?.total_pages}`);
    console.log(`- Dataset ID: ${responseData?.dataset_id}`);
    console.log(`- Date Range: ${responseData?.date_range?.start} to ${responseData?.date_range?.end}`);

    // CRITICAL TEST: Check for data loading mismatch
    if (responseData?.total_count > 0 && responseData?.data?.length === 0) {
      throw new Error(`HOURLY DATASET BUG DETECTED: API reports ${responseData.total_count} total records but returns empty data array. This indicates a data loading failure for hourly datasets.`);
    }

    // Check table content
    const tableRows = page.locator('table tbody tr');
    const rowCount = await tableRows.count();
    
    console.log(`Table rows displayed: ${rowCount}`);

    if (responseData?.total_count > 0) {
      expect(rowCount, 'Table should have data rows when total_count > 0').toBeGreaterThan(0);
      expect(responseData.data.length, 'API data array should not be empty when total_count > 0').toBeGreaterThan(0);
    }

    // Check for error messages in the UI
    const errorElements = await page.locator('.error, .alert-danger, [class*="error"]').all();
    if (errorElements.length > 0) {
      for (let errorElement of errorElements) {
        const errorText = await errorElement.textContent();
        if (errorText && errorText.trim()) {
          console.log(`UI Error detected: ${errorText}`);
        }
      }
    }

    // Verify pagination info
    if (responseData?.total_pages > 1) {
      await expect(page.locator('.table-pagination')).toBeVisible();
    }

    // Test data quality metrics
    const dataQualitySection = page.locator('.dataset-info');
    if (await dataQualitySection.count() > 0) {
      const qualityText = await dataQualitySection.textContent();
      console.log('Data quality info:', qualityText);
    }
  });

  test('should compare hourly dataset with working dataset', async ({ page }) => {
    // This test compares hourly dataset behavior with a known working dataset
    
    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    const options = await datasetSelect.locator('option').all();
    
    let hourlyDataset = null;
    let workingDataset = null;
    
    // Find both hourly and non-hourly datasets
    for (let option of options) {
      const text = await option.textContent();
      const value = await option.getAttribute('value');
      
      if (text && text.includes('hourly') && !hourlyDataset) {
        hourlyDataset = { text, value };
      } else if (text && !text.includes('hourly') && !workingDataset && value !== '') {
        workingDataset = { text, value };
      }
    }

    expect(hourlyDataset, 'Should find hourly dataset').toBeTruthy();
    expect(workingDataset, 'Should find working non-hourly dataset').toBeTruthy();

    console.log(`Comparing datasets:`);
    console.log(`- Hourly: ${hourlyDataset.text}`);
    console.log(`- Working: ${workingDataset.text}`);

    // Test working dataset first
    console.log('Testing working dataset...');
    await datasetSelect.selectOption(workingDataset.value);
    await page.waitForTimeout(2000);
    
    const workingRows = await page.locator('table tbody tr').count();
    console.log(`Working dataset table rows: ${workingRows}`);

    // Test hourly dataset
    console.log('Testing hourly dataset...');
    await datasetSelect.selectOption(hourlyDataset.value);
    await page.waitForTimeout(2000);
    
    const hourlyRows = await page.locator('table tbody tr').count();
    console.log(`Hourly dataset table rows: ${hourlyRows}`);

    // Compare results
    if (workingRows > 0 && hourlyRows === 0) {
      throw new Error(`ISSUE DETECTED: Working dataset shows ${workingRows} rows, but hourly dataset shows ${hourlyRows} rows. This indicates a specific issue with hourly dataset data loading.`);
    }
  });

  test('should verify hourly dataset file paths and metadata', async ({ page }) => {
    // This test checks if the issue is related to file paths or metadata
    
    let datasetDetails = null;

    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && !response.url().includes('/data') && response.ok()) {
        try {
          const data = await response.json();
          if (data.dataset_name && data.dataset_name.includes('hourly')) {
            datasetDetails = data;
          }
        } catch (e) {
          // Ignore parsing errors
        }
      }
    });

    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    
    // Find hourly dataset
    const options = await datasetSelect.locator('option').all();
    let hourlyValue = null;
    
    for (let option of options) {
      const text = await option.textContent();
      if (text && text.includes('hourly')) {
        hourlyValue = await option.getAttribute('value');
        break;
      }
    }

    expect(hourlyValue, 'Should find hourly dataset value').toBeTruthy();
    
    // Select hourly dataset to trigger API call
    await datasetSelect.selectOption(hourlyValue);
    await page.waitForTimeout(2000);

    // Check dataset metadata
    if (datasetDetails) {
      console.log('Hourly Dataset Metadata:');
      console.log(`- Features file: ${datasetDetails.features_file_path || 'MISSING'}`);
      console.log(`- Labels file: ${datasetDetails.labels_file_path || 'MISSING'}`);
      console.log(`- Metadata file: ${datasetDetails.metadata_file_path || 'MISSING'}`);
      console.log(`- File size: ${datasetDetails.file_size_mb || 0} MB`);
      
      // Check for missing file paths
      if (!datasetDetails.features_file_path || !datasetDetails.labels_file_path) {
        console.log('⚠️  WARNING: Missing file paths detected for hourly dataset');
      }
      
      if (!datasetDetails.metadata_file_path) {
        console.log('⚠️  WARNING: Missing metadata file path for hourly dataset');
      }
    }
  });
});