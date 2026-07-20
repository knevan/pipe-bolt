import { computed, readonly, shallowRef, toValue, watch, type MaybeRefOrGetter } from 'vue'
import { useMutation, useQuery, useQueryCache } from '@pinia/colada'

import type { PipeBoltApiDtoResolveFailureRequest } from '@/api/generated'
import { getApiErrorMessage } from '@/api/errors'
import {
  DEFAULT_OPERATION_LIMIT,
  clampOperationLimit,
  fetchAuditEvents,
  fetchDeliveryOutcomes,
  fetchFailures,
  submitFailureResolution,
} from '../operations.api'

interface OperationPage {
  items: ReadonlyArray<unknown>
  limit: number
  next_before?: string | null
}

function useCursorPagination() {
  const cursorHistory = shallowRef<ReadonlyArray<string>>([])
  const limit = shallowRef(DEFAULT_OPERATION_LIMIT)
  const before = computed(() => cursorHistory.value.at(-1))
  const pageNumber = computed(() => cursorHistory.value.length + 1)
  const canGoPrevious = computed(() => cursorHistory.value.length > 0)

  function goNext(cursor?: string | null): void {
    if (cursor) cursorHistory.value = [...cursorHistory.value, cursor]
  }

  function goPrevious(): void {
    if (cursorHistory.value.length > 0) cursorHistory.value = cursorHistory.value.slice(0, -1)
  }

  function reset(): void {
    cursorHistory.value = []
  }

  function setLimit(value: number): void {
    const nextLimit = clampOperationLimit(value)
    if (nextLimit === limit.value) return
    limit.value = nextLimit
    reset()
  }

  return {
    before,
    canGoPrevious,
    goNext,
    goPrevious,
    limit: readonly(limit),
    pageNumber,
    reset,
    setLimit,
  }
}

function canLoadNext(page?: OperationPage): boolean {
  if (!page?.next_before) return false
  return page.items.length === page.limit
}

export function useAuditLog(projectId: MaybeRefOrGetter<string | undefined>) {
  const pagination = useCursorPagination()
  const query = useQuery({
    key: () => [
      'project',
      toValue(projectId) ?? '',
      'operations',
      'audit',
      pagination.limit.value,
      pagination.before.value ?? '',
    ],
    enabled: () => Boolean(toValue(projectId)),
    query: ({ signal }) =>
      fetchAuditEvents(
        toValue(projectId) ?? '',
        { before: pagination.before.value, limit: pagination.limit.value },
        signal,
      ),
    staleTime: 5_000,
  })
  const items = computed(() => query.data.value?.items ?? [])
  const canGoNext = computed(() => canLoadNext(query.data.value))
  const errorMessage = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )

  watch(() => toValue(projectId), pagination.reset)
  watch(
    () => query.data.value,
    (page) => {
      if (page?.items.length === 0 && pagination.canGoPrevious.value) pagination.goPrevious()
    },
  )

  return { ...query, ...pagination, canGoNext, errorMessage, items }
}

export type FailureFilter = 'all' | 'unresolved'

export function useFailureLog(projectId: MaybeRefOrGetter<string | undefined>) {
  const pagination = useCursorPagination()
  const filter = shallowRef<FailureFilter>('unresolved')
  const query = useQuery({
    key: () => [
      'project',
      toValue(projectId) ?? '',
      'operations',
      'failures',
      filter.value,
      pagination.limit.value,
      pagination.before.value ?? '',
    ],
    enabled: () => Boolean(toValue(projectId)),
    query: ({ signal }) =>
      fetchFailures(
        toValue(projectId) ?? '',
        {
          before: pagination.before.value,
          limit: pagination.limit.value,
          unresolvedOnly: filter.value === 'unresolved',
        },
        signal,
      ),
    staleTime: 5_000,
  })
  const items = computed(() => query.data.value?.items ?? [])
  const canGoNext = computed(() => canLoadNext(query.data.value))
  const errorMessage = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )

  function setFilter(value: FailureFilter): void {
    if (value === filter.value) return
    filter.value = value
    pagination.reset()
  }

  watch(() => toValue(projectId), pagination.reset)
  watch(
    () => query.data.value,
    (page) => {
      if (page?.items.length === 0 && pagination.canGoPrevious.value) pagination.goPrevious()
    },
  )

  return {
    ...query,
    ...pagination,
    canGoNext,
    errorMessage,
    filter: readonly(filter),
    items,
    setFilter,
  }
}

export function useDeliveryOutcomes(projectId: MaybeRefOrGetter<string | undefined>) {
  const pagination = useCursorPagination()
  const query = useQuery({
    key: () => [
      'project',
      toValue(projectId) ?? '',
      'operations',
      'deliveries',
      pagination.limit.value,
      pagination.before.value ?? '',
    ],
    enabled: () => Boolean(toValue(projectId)),
    query: ({ signal }) =>
      fetchDeliveryOutcomes(
        toValue(projectId) ?? '',
        { before: pagination.before.value, limit: pagination.limit.value },
        signal,
      ),
    staleTime: 5_000,
  })
  const items = computed(() => query.data.value?.items ?? [])
  const canGoNext = computed(() => canLoadNext(query.data.value))
  const errorMessage = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )

  watch(() => toValue(projectId), pagination.reset)
  watch(
    () => query.data.value,
    (page) => {
      if (page?.items.length === 0 && pagination.canGoPrevious.value) pagination.goPrevious()
    },
  )

  return { ...query, ...pagination, canGoNext, errorMessage, items }
}

export function useResolveFailure(projectId: MaybeRefOrGetter<string | undefined>) {
  const queryCache = useQueryCache()
  const mutation = useMutation({
    mutation: ({
      body,
      failureId,
      projectId: mutationProjectId,
    }: {
      body: PipeBoltApiDtoResolveFailureRequest
      failureId: string
      projectId: string
    }) => submitFailureResolution(mutationProjectId, failureId, body),
  })
  const errorMessage = computed(() =>
    mutation.error.value ? getApiErrorMessage(mutation.error.value) : undefined,
  )

  watch(() => toValue(projectId), mutation.reset)

  async function resolve(
    failureId: string,
    body: PipeBoltApiDtoResolveFailureRequest,
  ): Promise<boolean> {
    mutation.reset()
    const currentProjectId = toValue(projectId)
    if (!currentProjectId) return false
    try {
      await mutation.mutateAsync({ body, failureId, projectId: currentProjectId })
      void Promise.allSettled([
        queryCache.invalidateQueries({
          key: ['project', currentProjectId, 'operations', 'failures'],
        }),
        queryCache.invalidateQueries({
          key: ['project', currentProjectId, 'operations', 'audit'],
        }),
      ])
      return true
    } catch {
      return false
    }
  }

  return {
    errorMessage,
    isLoading: mutation.isLoading,
    reset: mutation.reset,
    resolve,
  }
}
