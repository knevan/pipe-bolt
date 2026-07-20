import { spawnSync } from 'node:child_process'
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createClient } from '@hey-api/openapi-ts'
import { createJiti } from 'jiti'

const scriptDirectory = dirname(fileURLToPath(import.meta.url))
const webRoot = resolve(scriptDirectory, '..')
const repositoryRoot = resolve(webRoot, '..', '..')
const localSpecPath = join(webRoot, 'open-api.json')
const generatedPath = join(webRoot, 'src', 'api', 'generated')

function exportBackendSpec() {
  const result = spawnSync(
    'cargo',
    [
      'run',
      '--quiet',
      '-p',
      'pipe-bolt-api',
      '--example',
      'export_openapi',
      '--features',
      'salvo-oapi',
    ],
    {
      cwd: repositoryRoot,
      encoding: 'utf8',
      maxBuffer: 20 * 1024 * 1024,
      windowsHide: true,
    },
  )
  if (result.error) throw new Error(`Backend OpenAPI export failed: ${result.error.message}`)
  if (result.status !== 0) {
    throw new Error(
      `Backend OpenAPI export exited with ${result.status ?? 'unknown status'}: ${result.stderr.trim()}`,
    )
  }
  try {
    return JSON.parse(result.stdout)
  } catch (error) {
    throw new Error('Backend OpenAPI exporter returned invalid JSON.', { cause: error })
  }
}

function escapePointerSegment(value) {
  return value.replaceAll('~', '~0').replaceAll('/', '~1')
}

function firstDifference(expected, actual, pointer = '') {
  if (Object.is(expected, actual)) return
  if (Array.isArray(expected) || Array.isArray(actual)) {
    if (!Array.isArray(expected) || !Array.isArray(actual)) return pointer || '/'
    if (expected.length !== actual.length) return `${pointer}/length`
    for (let index = 0; index < expected.length; index += 1) {
      const difference = firstDifference(expected[index], actual[index], `${pointer}/${index}`)
      if (difference) return difference
    }
    return
  }
  if (
    typeof expected !== 'object' ||
    expected === null ||
    typeof actual !== 'object' ||
    actual === null
  ) {
    return pointer || '/'
  }
  const expectedKeys = Object.keys(expected).sort()
  const actualKeys = Object.keys(actual).sort()
  if (expectedKeys.length !== actualKeys.length) return pointer || '/'
  for (let index = 0; index < expectedKeys.length; index += 1) {
    if (expectedKeys[index] !== actualKeys[index]) return pointer || '/'
  }
  for (const key of expectedKeys) {
    const difference = firstDifference(
      expected[key],
      actual[key],
      `${pointer}/${escapePointerSegment(key)}`,
    )
    if (difference) return difference
  }
}

async function listFiles(root, directory = root) {
  const entries = await readdir(directory, { withFileTypes: true })
  const files = []
  for (const entry of entries) {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) files.push(...(await listFiles(root, path)))
    else if (entry.isFile()) files.push(relative(root, path).replaceAll('\\', '/'))
  }
  return files.sort()
}

async function compareGenerated(expectedRoot, actualRoot) {
  const expectedFiles = await listFiles(expectedRoot)
  const actualFiles = await listFiles(actualRoot)
  const expectedSet = new Set(expectedFiles)
  const actualSet = new Set(actualFiles)
  const missing = expectedFiles.filter((file) => !actualSet.has(file))
  const extra = actualFiles.filter((file) => !expectedSet.has(file))
  if (missing.length || extra.length) {
    throw new Error(
      `Generated client file set drifted. Missing: ${missing.join(', ') || 'none'}. Extra: ${extra.join(', ') || 'none'}.`,
    )
  }
  for (const file of expectedFiles) {
    const [expected, actual] = await Promise.all([
      readFile(join(expectedRoot, file)),
      readFile(join(actualRoot, file)),
    ])
    if (!expected.equals(actual)) throw new Error(`Generated client drift detected in ${file}.`)
  }
}

async function main() {
  const localSpec = JSON.parse(await readFile(localSpecPath, 'utf8'))
  const backendSpec = exportBackendSpec()
  const specDifference = firstDifference(backendSpec, localSpec)
  if (specDifference) {
    throw new Error(`Local open-api.json differs from backend contract at ${specDifference}.`)
  }

  const temporaryRoot = await mkdtemp(join(tmpdir(), 'pipe-bolt-openapi-'))
  const temporaryOutput = join(temporaryRoot, 'generated')
  try {
    const jiti = createJiti(import.meta.url)
    const config = await jiti.import(join(webRoot, 'openapi-ts.config.ts'), { default: true })
    if (!config || typeof config !== 'object' || typeof config.output !== 'object') {
      throw new Error('OpenAPI generator config has an unsupported shape.')
    }
    await createClient({
      ...config,
      input: localSpecPath,
      output: {
        ...config.output,
        path: temporaryOutput,
        tsConfigPath: join(webRoot, 'tsconfig.json'),
      },
    })
    await compareGenerated(generatedPath, temporaryOutput)
  } finally {
    await rm(temporaryRoot, { force: true, recursive: true })
  }
  console.log('OpenAPI contract and generated client are synchronized.')
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error)
  process.exitCode = 1
})
