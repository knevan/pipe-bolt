import { expect, test, type Page } from '@playwright/test'

import { E2E_PROJECT_ID, E2E_TOKEN, installApiMocks } from './fixtures/api'

async function submitLogin(page: Page): Promise<void> {
  await page.getByLabel('Bearer token').fill(E2E_TOKEN)
  await page.getByRole('button', { name: 'Open control plane' }).click()
}

test('login redirects to requested project dashboard and loads runtime state', async ({ page }) => {
  const api = await installApiMocks(page, { runtimeDelayMs: 500 })
  await page.goto(`/projects/${E2E_PROJECT_ID}`)
  await expect(page).toHaveURL(new RegExp(`/login\\?redirect=.*${E2E_PROJECT_ID}`))

  await submitLogin(page)
  await expect(page.getByLabel('Loading runtime status')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Runtime overview' })).toBeVisible()
  await expect(page.getByText('running', { exact: true })).toBeVisible()
  await expect(page).toHaveURL(`/projects/${E2E_PROJECT_ID}`)
  expect(api.unexpectedRequests).toEqual([])
})

test('select project, edit broker, and save config with OCC version', async ({ page }) => {
  const api = await installApiMocks(page)
  await page.goto('/login')
  await submitLogin(page)
  await page.getByLabel('Project ID', { exact: true }).fill(E2E_PROJECT_ID)
  await page.getByRole('button', { name: 'Open project' }).click()
  await expect(page.getByRole('heading', { name: 'Runtime overview' })).toBeVisible()

  await page.getByRole('link', { name: 'Configuration' }).click()
  await expect(page.getByRole('heading', { name: 'Configuration' })).toBeVisible()
  await page.getByRole('button', { name: /^Brokers/ }).click()
  const broker = page.getByRole('article', { name: 'Broker 1: Primary Broker' })
  await broker.getByLabel('Host').fill('mqtt.updated.test')
  await page.getByPlaceholder('Describe this config change').fill('E2E broker update')
  await page.getByRole('button', { name: 'Save config' }).click()

  await expect(page.getByRole('status')).toContainText('Configuration version 8 saved.')
  expect(api.lastConfigWrite).toMatchObject({
    config: { brokers: [{ host: 'mqtt.updated.test' }], project_id: E2E_PROJECT_ID },
    expected_version: 7,
    reason: 'E2E broker update',
  })
  expect(api.unexpectedRequests).toEqual([])
})

test('execute governed command and display queued receipt', async ({ page }) => {
  const api = await installApiMocks(page)
  await page.goto(`/projects/${E2E_PROJECT_ID}/commands`)
  await submitLogin(page)
  await expect(page.getByRole('heading', { name: 'Command gateway' })).toBeVisible()
  await page.getByRole('button', { name: 'Execute command' }).click()

  const dialog = page.getByRole('dialog', { name: 'Reboot device' })
  await dialog.getByLabel(/^device_id/).fill('device-1')
  await dialog.getByPlaceholder('Why is this command necessary?').fill('E2E controlled reboot')
  await dialog.getByRole('button', { name: 'Review command' }).click()
  await dialog.getByRole('button', { name: 'Confirm and queue' }).click()

  await expect(dialog.getByText('queued', { exact: true })).toBeVisible()
  await expect(dialog.getByText('command-exec-1', { exact: true })).toBeVisible()
  expect(api.lastCommandRequest).toEqual({
    params: { device_id: 'device-1' },
    reason: 'E2E controlled reboot',
  })
  await dialog.getByRole('button', { name: 'Close receipt' }).click()
  expect(api.unexpectedRequests).toEqual([])
})

test('resolve failure and refresh unresolved list', async ({ page }) => {
  const api = await installApiMocks(page)
  await page.goto(`/projects/${E2E_PROJECT_ID}/operations/failures`)
  await submitLogin(page)
  const row = page.getByRole('row').filter({ hasText: 'Webhook delivery timed out' })
  await expect(row).toBeVisible()
  await row.getByRole('button', { name: 'Resolve' }).click()

  const dialog = page.getByRole('dialog', { name: 'Close operational failure' })
  await dialog.getByLabel(/^Resolution note/).fill('Endpoint recovered and delivery verified.')
  await dialog.getByLabel(/^Audit reason/).fill('Incident PB-42 closed')
  await dialog.getByRole('button', { name: 'Resolve failure' }).click()

  await expect(dialog).toBeHidden()
  await expect(row).toBeHidden()
  expect(api.lastResolutionRequest).toEqual({
    reason: 'Incident PB-42 closed',
    resolution: 'Endpoint recovered and delivery verified.',
  })
  expect(api.unexpectedRequests).toEqual([])
})

test('observe normalized realtime event from SSE stream', async ({ page }) => {
  const api = await installApiMocks(page)
  await page.goto(`/projects/${E2E_PROJECT_ID}/realtime`)
  await submitLogin(page)

  await expect(page.getByRole('heading', { name: 'Realtime events' })).toBeVisible()
  await expect(page.getByText('temperature_update', { exact: true }).first()).toBeVisible()
  await expect(page.getByRole('button', { name: 'Clear buffer' })).toBeEnabled()
  await page.getByRole('button', { name: 'Disconnect' }).click()
  expect(api.unexpectedRequests).toEqual([])
})
