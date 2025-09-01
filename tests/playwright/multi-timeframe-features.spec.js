const { test, expect } = require('@playwright/test');

test.describe('Multi-Timeframe Training Features Test', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to training EDA page
    await page.goto('http://localhost:3000/training-eda');
    await page.waitForLoadState('networkidle');
  });

  test('should detect missing multi-timeframe features (5m,15m,1d,1w)', async ({ page }) => {
    let apiResponse = null;

    // Intercept API requests to capture dataset data
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && response.url().includes('/data')) {
        if (response.ok()) {
          try {
            const data = await response.json();
            await page.evaluate((responseData) => {
              window.lastDatasetResponse = responseData;
            }, data);
          } catch (e) {
            console.log('Failed to parse dataset API response:', e);
          }
        }
      }
    });

    // Wait for datasets to load and select hourly dataset
    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    const options = await datasetSelect.locator('option').all();
    
    let hourlyDatasetFound = false;
    let hourlyDatasetValue = null;
    
    for (let option of options) {
      const text = await option.textContent();
      if (text && text.includes('hourly')) {
        hourlyDatasetFound = true;
        hourlyDatasetValue = await option.getAttribute('value');
        console.log(`Found hourly dataset: ${text}`);
        break;
      }
    }

    expect(hourlyDatasetFound, 'Should find hourly dataset').toBeTruthy();
    
    // Select hourly dataset
    await datasetSelect.selectOption(hourlyDatasetValue);
    await page.waitForTimeout(3000);

    // Get API response data
    const responseData = await page.evaluate(() => window.lastDatasetResponse);
    expect(responseData, 'Should have dataset response').toBeTruthy();
    expect(responseData.data, 'Should have data array').toBeTruthy();
    expect(responseData.data.length, 'Should have data samples').toBeGreaterThan(0);

    // Analyze features in first sample
    const sampleData = responseData.data[0];
    const featureKeys = Object.keys(sampleData).filter(key => !['sequence_id', 'datetime'].includes(key));
    
    console.log('Available features:', featureKeys);

    // Check for multi-timeframe patterns
    const timeframePatterns = ['5m_', '15m_', '1h_', '1d_', '1w_'];
    const foundTimeframes = [];
    
    for (const pattern of timeframePatterns) {
      const matchingFeatures = featureKeys.filter(key => key.startsWith(pattern));
      if (matchingFeatures.length > 0) {
        foundTimeframes.push(pattern);
        console.log(`Found ${pattern} features:`, matchingFeatures.slice(0, 3));
      }
    }

    // Test assertion - currently expected to fail until multi-timeframe generation is fixed
    if (foundTimeframes.length === 0) {
      throw new Error(
        `❌ ISSUE CONFIRMED: Missing multi-timeframe features!\n` +
        `Expected timeframe patterns: ${timeframePatterns.join(', ')}\n` +
        `Available features: ${featureKeys.slice(0, 10).join(', ')}...\n` +
        `Total features: ${featureKeys.length}\n` +
        `This confirms training data is not generating multi-timeframe features as configured in Gin files.`
      );
    }

    console.log(`✅ Multi-timeframe features found: ${foundTimeframes.join(', ')}`);
  });

  test('should detect missing technical indicators (pldot, z1b, z2b, z5t, z6t)', async ({ page }) => {
    // Similar setup to get dataset data
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && response.url().includes('/data') && response.ok()) {
        try {
          const data = await response.json();
          await page.evaluate((responseData) => {
            window.lastDatasetResponse = responseData;
          }, data);
        } catch (e) {
          // Ignore parsing errors
        }
      }
    });

    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    
    // Select first dataset
    const options = await datasetSelect.locator('option').all();
    if (options.length > 1) {
      const firstValue = await options[1].getAttribute('value'); // Skip empty option
      await datasetSelect.selectOption(firstValue);
      await page.waitForTimeout(2000);
    }

    const responseData = await page.evaluate(() => window.lastDatasetResponse);
    if (!responseData?.data?.[0]) {
      throw new Error('No dataset data available for testing');
    }

    const sampleData = responseData.data[0];
    const featureKeys = Object.keys(sampleData);
    
    // Expected technical indicators
    const expectedIndicators = ['pldot', 'z1b', 'z2b', 'z5t', 'z6t'];
    const foundIndicators = [];
    const missingIndicators = [];

    for (const indicator of expectedIndicators) {
      // Check for direct match or as part of multi-timeframe features
      const directMatch = featureKeys.includes(indicator);
      const timeframeMatch = featureKeys.some(key => key.includes(indicator));
      
      if (directMatch || timeframeMatch) {
        foundIndicators.push(indicator);
      } else {
        missingIndicators.push(indicator);
      }
    }

    console.log('Found indicators:', foundIndicators);
    console.log('Missing indicators:', missingIndicators);

    if (missingIndicators.length > 0) {
      throw new Error(
        `❌ ISSUE CONFIRMED: Missing technical indicators!\n` +
        `Missing: ${missingIndicators.join(', ')}\n` +
        `Found: ${foundIndicators.join(', ')}\n` +
        `Available features: ${featureKeys.slice(0, 15).join(', ')}...\n` +
        `This confirms training data is not generating all configured technical indicators.`
      );
    }

    console.log(`✅ All technical indicators found: ${foundIndicators.join(', ')}`);
  });

  test('should verify envelope scaling is fixed', async ({ page }) => {
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && response.url().includes('/data') && response.ok()) {
        try {
          const data = await response.json();
          await page.evaluate((responseData) => {
            window.lastDatasetResponse = responseData;
          }, data);
        } catch (e) {
          // Ignore parsing errors
        }
      }
    });

    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    
    // Find and select hourly dataset
    const options = await datasetSelect.locator('option').all();
    for (let option of options) {
      const text = await option.textContent();
      if (text && text.includes('hourly')) {
        const value = await option.getAttribute('value');
        await datasetSelect.selectOption(value);
        break;
      }
    }
    
    await page.waitForTimeout(2000);

    const responseData = await page.evaluate(() => window.lastDatasetResponse);
    if (!responseData?.data?.[0]) {
      throw new Error('No hourly dataset data available for envelope scaling test');
    }

    // Check several samples for envelope scaling issues
    for (let i = 0; i < Math.min(3, responseData.data.length); i++) {
      const sample = responseData.data[i];
      const closePrice = sample.hour_close || sample.close;
      
      expect(closePrice, `Sample ${i} should have close price`).toBeGreaterThan(10);

      // Check envelope indicators if present
      if (sample.envelope_top !== undefined) {
        const envelopeTop = sample.envelope_top;
        
        // If envelope_top is a small number (like 3.0), it's likely a categorical code
        if (envelopeTop < 10) {
          throw new Error(
            `❌ ENVELOPE SCALING ISSUE: Sample ${i} has envelope_top=${envelopeTop} which appears to be a categorical code.\n` +
            `Expected: Price level near close=${closePrice}\n` +
            `This suggests categorical conversion is being applied incorrectly to envelope indicators.`
          );
        }

        console.log(`Sample ${i}: close=${closePrice}, envelope_top=${envelopeTop} ✅`);
      }

      if (sample.envelope_bot !== undefined) {
        const envelopeBot = sample.envelope_bot;
        
        // Allow 0 as valid, but small positive numbers indicate categorical codes
        if (envelopeBot > 0 && envelopeBot < 10) {
          throw new Error(
            `❌ ENVELOPE SCALING ISSUE: Sample ${i} has envelope_bot=${envelopeBot} which appears to be a categorical code.\n` +
            `Expected: Price level near close=${closePrice} or 0\n` +
            `This suggests categorical conversion is being applied incorrectly to envelope indicators.`
          );
        }
      }
    }

    console.log('✅ Envelope scaling appears to be fixed - no categorical codes detected');
  });

  test('should verify datetime metadata is included', async ({ page }) => {
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && response.url().includes('/data') && response.ok()) {
        try {
          const data = await response.json();
          await page.evaluate((responseData) => {
            window.lastDatasetResponse = responseData;
          }, data);
        } catch (e) {
          // Ignore parsing errors  
        }
      }
    });

    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    
    // Select first available dataset
    const options = await datasetSelect.locator('option').all();
    if (options.length > 1) {
      const firstValue = await options[1].getAttribute('value');
      await datasetSelect.selectOption(firstValue);
      await page.waitForTimeout(2000);
    }

    const responseData = await page.evaluate(() => window.lastDatasetResponse);
    if (!responseData?.data?.[0]) {
      throw new Error('No dataset data available for datetime test');
    }

    // Check all samples for datetime field
    for (let i = 0; i < responseData.data.length; i++) {
      const sample = responseData.data[i];
      
      expect(sample.datetime, `Sample ${i} should have datetime field`).toBeDefined();
      expect(typeof sample.datetime, `Sample ${i} datetime should be string`).toBe('string');
      
      // Should be parseable as ISO datetime
      const datetimeStr = sample.datetime;
      const parsedDate = new Date(datetimeStr);
      expect(parsedDate.getFullYear(), `Sample ${i} datetime should be valid year`).toBeGreaterThan(2020);
      
      console.log(`Sample ${i}: datetime=${datetimeStr} ✅`);
    }

    console.log(`✅ All ${responseData.data.length} samples include datetime metadata`);
  });

  test('should show feature count expectations vs reality', async ({ page }) => {
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets/') && response.url().includes('/data') && response.ok()) {
        try {
          const data = await response.json();
          await page.evaluate((responseData) => {
            window.lastDatasetResponse = responseData;
          }, data);
        } catch (e) {
          // Ignore parsing errors
        }
      }
    });

    await page.waitForSelector('.dataset-selection select', { timeout: 10000 });
    const datasetSelect = page.locator('.dataset-selection select');
    
    // Test multiple datasets if available
    const options = await datasetSelect.locator('option').all();
    const datasetResults = [];
    
    for (let i = 1; i < Math.min(options.length, 4); i++) { // Test up to 3 datasets
      const optionText = await options[i].textContent();
      const optionValue = await options[i].getAttribute('value');
      
      await datasetSelect.selectOption(optionValue);
      await page.waitForTimeout(1500);
      
      const responseData = await page.evaluate(() => window.lastDatasetResponse);
      if (responseData?.data?.[0]) {
        const sample = responseData.data[0];
        const featureCount = Object.keys(sample).filter(key => !['sequence_id', 'datetime'].includes(key)).length;
        
        datasetResults.push({
          name: optionText.trim(),
          features: featureCount
        });
      }
    }

    // Report findings
    console.log('\n📊 Feature Count Analysis:');
    console.log('Expected (full multi-timeframe): ~1000+ features');
    console.log('Expected (minimum viable): ~50+ features');
    console.log('');
    
    for (const result of datasetResults) {
      console.log(`Dataset: ${result.name}`);
      console.log(`Features: ${result.features}`);
      
      if (result.features < 50) {
        console.log(`❌ Too few features - likely basic OHLCV only`);
      } else if (result.features < 100) {
        console.log(`⚠️  Limited features - partial implementation`);
      } else if (result.features < 500) {
        console.log(`⚠️  Moderate features - missing some timeframes`);
      } else {
        console.log(`✅ Good feature count - likely full multi-timeframe`);
      }
      console.log('');
    }

    // Find the dataset with most features
    const maxFeatures = Math.max(...datasetResults.map(r => r.features));
    const minFeatures = Math.min(...datasetResults.map(r => r.features));
    
    console.log(`Feature range: ${minFeatures} - ${maxFeatures}`);
    
    // Set expectations based on current reality
    if (maxFeatures < 50) {
      throw new Error(
        `❌ CRITICAL ISSUE: All datasets have very few features (max: ${maxFeatures}).\n` +
        `This suggests training data generation is only creating basic OHLCV data, not multi-timeframe features.\n` +
        `Expected: 1000+ features for full multi-timeframe configuration.`
      );
    }
  });
});