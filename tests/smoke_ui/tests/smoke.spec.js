const { test, expect } = require('@playwright/test');

test('Smoke Test: Login and View Case', async ({ page }) => {
  // Login
  await page.goto('/#/login'); // Assuming hash mode

  // Wait for login form
  const loginInput = page.locator('input[placeholder="Login"]');
  await expect(loginInput).toBeVisible();

  await loginInput.fill('admin@neuralvyuha.local');
  await page.locator('input[placeholder="Password"]').fill('secretPassword123!');
  await page.locator('button[type="submit"]').click();

  // Wait for login to complete (e.g. check for navbar or home)
  // If login failed (e.g. user not created), this will timeout.
  // We assume API test ran before and created the user.

  await expect(page.locator('.navbar-header')).toBeVisible(); // Common bootstrap class

  // Navigate to Cases
  await page.goto('/#/cases');

  // Verify Case List has the case created by API test
  await expect(page.locator('text=Smoke Test Case')).toBeVisible();

  // Open Case
  await page.locator('text=Smoke Test Case').first().click();

  // Verify Detail
  await expect(page.locator('text=Created by automated smoke test')).toBeVisible();
});
