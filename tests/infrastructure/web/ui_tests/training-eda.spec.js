// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Training EDA Dashboard Test Suite
 * 
 * Tests the training dataset EDA dashboard to detect issues with:
 * - Page loading
 * - Dataset dropdown population
 * - API endpoint availability
 * - Table row display
 * - Error handling
 */

test.describe('Training EDA Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    // Set up console logging to capture JavaScript errors
    page.on('console', msg => {
      if (msg.type() === 'error') {
        console.log('❌ Browser Console Error:', msg.text());
      } else if (msg.type() === 'log' && (msg.text().includes('❌') || msg.text().includes('✅'))) {
        console.log('📊 Dashboard Log:', msg.text());
      }
    });
    
    // Listen for network failures
    page.on('response', response => {
      if (!response.ok() && response.url().includes('/api/')) {
        console.log(`❌ API Error: ${response.status()} ${response.statusText()} - ${response.url()}`);
      }
    });
  });

  test('should load training EDA page successfully', async ({ page }) => {
    await page.goto('http://localhost:3000/training-eda');
    
    // Check page title and header
    await expect(page).toHaveTitle(/Training Dataset EDA Dashboard/);
    await expect(page.locator('h1')).toContainText('Training Dataset EDA Dashboard');
    
    // Check that main components are visible
    await expect(page.locator('.dataset-selection')).toBeVisible();
    await expect(page.locator('#training-dataset-select')).toBeVisible();
    
    console.log('✅ Training EDA page loaded successfully');
  });

  test('should detect API endpoint availability', async ({ page }) => {
    let apiError = null;
    let apiResponse = null;
    
    // Intercept API requests to capture responses
    page.on('response', async response => {
      if (response.url().includes('/api/v1/training-datasets')) {
        apiResponse = response;
        if (!response.ok()) {
          apiError = {
            status: response.status(),
            statusText: response.statusText(),
            url: response.url()
          };
        }
      }
    });
    
    await page.goto('http://localhost:3000/training-eda');
    
    // Wait for the API call to be made
    await page.waitForTimeout(2000);
    
    // Check if API endpoint exists
    if (apiError) {
      console.log('❌ DETECTED ISSUE: API endpoint not available');
      console.log(`   Status: ${apiError.status} ${apiError.statusText}`);
      console.log(`   URL: ${apiError.url}`);
      
      // This is the main issue we're detecting
      expect(apiError.status).toBe(404);
      expect(apiError.url).toContain('/api/v1/training-datasets');
    } else if (apiResponse && apiResponse.ok()) {
      console.log('✅ API endpoint is working');
    } else {
      console.log('⚠️ No API call detected - possible JavaScript error');
    }
  });

  test('should show correct error message when API is not available', async ({ page }) => {
    await page.goto('http://localhost:3000/training-eda');
    
    // Wait for JavaScript to load and make API calls
    await page.waitForTimeout(3000);
    
    // Check dropdown state
    const dropdown = page.locator('#training-dataset-select');
    const dropdownText = await dropdown.textContent();
    
    console.log('📋 Dropdown content:', dropdownText);
    
    // Should show error message in dropdown when API fails
    if (dropdownText?.includes('Error loading datasets')) {
      console.log('✅ Correct error message displayed in dropdown');
      await expect(dropdown).toContainText('Error loading datasets');
    } else if (dropdownText?.includes('No training datasets found')) {
      console.log('⚠️ No datasets found message (API worked but returned empty)');
      await expect(dropdown).toContainText('No training datasets found');
    } else if (dropdownText?.includes('Loading')) {
      console.log('⚠️ Still loading - API call may be hanging');
      // Wait a bit more
      await page.waitForTimeout(5000);
      const finalDropdownText = await dropdown.textContent();
      console.log('📋 Final dropdown content:', finalDropdownText);
    }
    
    // Check for error message in analysis content
    const analysisContent = page.locator('#analysis-content');
    const errorMessage = page.locator('.error');
    
    if (await errorMessage.isVisible()) {
      console.log('✅ Error message displayed in analysis content');
      await expect(errorMessage).toContainText('Failed to Load Training Datasets');
      await expect(errorMessage).toContainText('/api/v1/training-datasets');
    }
  });

  test('should detect empty table rows issue', async ({ page }) => {
    await page.goto('http://localhost:3000/training-eda');
    
    // Wait for page to load
    await page.waitForTimeout(3000);
    
    // Check if training data table section is visible
    const tableSection = page.locator('#training-data-table-section');
    const isTableVisible = await tableSection.isVisible();
    
    if (isTableVisible) {
      console.log('✅ Table section is visible');
      
      // Check table body content
      const tableBody = page.locator('#training-table-body');
      const tableRows = await tableBody.locator('tr').count();
      
      console.log(`📊 Table rows count: ${tableRows}`);
      
      if (tableRows === 0) {
        console.log('❌ DETECTED ISSUE: No table rows found');
      } else {
        // Check if rows contain actual data or just "No data available"
        const firstRowText = await tableBody.locator('tr').first().textContent();
        if (firstRowText?.includes('No data available')) {
          console.log('❌ DETECTED ISSUE: Table shows "No data available"');
        } else {
          console.log('✅ Table has data rows');
        }
      }
    } else {
      console.log('❌ DETECTED ISSUE: Training data table section is not visible');
    }
  });

  test('should test full user workflow', async ({ page }) => {
    await page.goto('http://localhost:3000/training-eda');
    
    console.log('🔍 Testing full user workflow...');
    
    // Step 1: Wait for datasets to load
    await page.waitForTimeout(3000);
    
    // Step 2: Check dropdown options
    const dropdown = page.locator('#training-dataset-select');
    const options = await dropdown.locator('option').count();
    
    console.log(`📋 Dropdown has ${options} options`);
    
    if (options > 1) {
      // Try to select the first real dataset (skip "Select..." option)
      const secondOption = dropdown.locator('option').nth(1);
      const optionValue = await secondOption.getAttribute('value');
      const optionText = await secondOption.textContent();
      
      if (optionValue && optionText && !optionText.includes('Error') && !optionText.includes('Loading')) {
        console.log(`🎯 Selecting dataset: ${optionText}`);
        
        await dropdown.selectOption(optionValue);
        
        // Wait for dataset analysis to load
        await page.waitForTimeout(2000);
        
        // Check if dataset info is displayed
        const datasetInfo = page.locator('#dataset-info');
        if (await datasetInfo.isVisible()) {
          console.log('✅ Dataset info is displayed');
          
          // Check if statistics are populated
          const totalSequences = await page.locator('#total-sequences').textContent();
          const featureCount = await page.locator('#feature-count').textContent();
          
          console.log(`📊 Total sequences: ${totalSequences}`);
          console.log(`📊 Feature count: ${featureCount}`);
          
          if (totalSequences !== '0' || featureCount !== '0') {
            console.log('✅ Dataset statistics are populated');
          } else {
            console.log('⚠️ Dataset statistics show zero values');
          }
          
          // Check if table section becomes visible
          await page.waitForTimeout(2000);
          const tableSection = page.locator('#training-data-table-section');
          if (await tableSection.isVisible()) {
            console.log('✅ Training data table section is visible');
            
            // Check table content
            const tableInfo = await page.locator('#training-table-info').textContent();
            console.log(`📊 Table info: ${tableInfo}`);
            
            if (tableInfo?.includes('❌ Error loading data')) {
              console.log('❌ DETECTED ISSUE: Error loading training data table');
            } else if (tableInfo?.includes('No training data available')) {
              console.log('⚠️ No training data available for selected dataset');
            } else {
              console.log('✅ Training data table loaded successfully');
            }
          } else {
            console.log('❌ DETECTED ISSUE: Training data table section not visible after dataset selection');
          }
        } else {
          console.log('❌ DETECTED ISSUE: Dataset info not displayed after selection');
        }
      } else {
        console.log('❌ DETECTED ISSUE: No valid datasets available for selection');
      }
    } else {
      console.log('❌ DETECTED ISSUE: No dataset options available (only default option)');
    }
  });

  test('should check network requests and responses', async ({ page }) => {
    const networkRequests = [];
    
    // Capture all network requests
    page.on('request', request => {
      if (request.url().includes('/api/')) {
        networkRequests.push({
          url: request.url(),
          method: request.method()
        });
      }
    });
    
    const networkResponses = [];
    
    // Capture all network responses  
    page.on('response', async response => {
      if (response.url().includes('/api/')) {
        networkResponses.push({
          url: response.url(),
          status: response.status(),
          statusText: response.statusText(),
          ok: response.ok()
        });
      }
    });
    
    await page.goto('http://localhost:3000/training-eda');
    await page.waitForTimeout(3000);
    
    console.log('🌐 Network Analysis:');
    console.log(`📤 API Requests made: ${networkRequests.length}`);
    networkRequests.forEach(req => {
      console.log(`   ${req.method} ${req.url}`);
    });
    
    console.log(`📥 API Responses received: ${networkResponses.length}`);
    networkResponses.forEach(res => {
      console.log(`   ${res.status} ${res.statusText} - ${res.url} (${res.ok ? 'OK' : 'FAILED'})`);
    });
    
    // Validate that the required API endpoint is being called
    const trainingDatasetsRequest = networkRequests.find(req => 
      req.url.includes('/api/v1/training-datasets') && req.method === 'GET'
    );
    
    if (trainingDatasetsRequest) {
      console.log('✅ Training datasets API endpoint is being called');
    } else {
      console.log('❌ DETECTED ISSUE: Training datasets API endpoint is not being called');
    }
    
    // Check for 404 responses
    const failedResponses = networkResponses.filter(res => !res.ok);
    if (failedResponses.length > 0) {
      console.log('❌ DETECTED ISSUES: Failed API responses:');
      failedResponses.forEach(res => {
        console.log(`   ${res.status} ${res.statusText} - ${res.url}`);
      });
    }
  });

  test('should validate database integration', async ({ page }) => {
    await page.goto('http://localhost:3000/training-eda');
    await page.waitForTimeout(3000);
    
    // Check if the page indicates database connection issues
    const errorElements = await page.locator('.error').count();
    
    if (errorElements > 0) {
      console.log('⚠️ Error messages found on page - checking for database issues');
      
      const errorTexts = await page.locator('.error').allTextContents();
      errorTexts.forEach(text => {
        console.log(`❌ Error: ${text}`);
        
        if (text.includes('database') || text.includes('connection')) {
          console.log('❌ DETECTED ISSUE: Database connection problem');
        }
        if (text.includes('404') || text.includes('endpoint')) {
          console.log('❌ DETECTED ISSUE: API endpoint missing');
        }
      });
    } else {
      console.log('ℹ️ No error messages visible - may indicate successful database connection');
    }
    
    // Additional check: try to access health endpoint to verify service is running
    const healthResponse = await page.request.get('http://localhost:3000/health');
    const healthData = await healthResponse.json();
    
    console.log('🏥 Health check result:', healthData);
    
    if (healthResponse.ok() && healthData.status === 'healthy') {
      console.log('✅ Analytics service is healthy');
    } else {
      console.log('❌ DETECTED ISSUE: Analytics service health check failed');
    }
  });

  test.afterEach(async ({ page }) => {
    // Clean up after each test
    await page.close();
  });
});