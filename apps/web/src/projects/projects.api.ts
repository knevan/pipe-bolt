import { getRuntimeStatus, type PipeBoltApiDtoRuntimeStatusResponse } from '@/api/generated'

export async function fetchProjectRuntimeStatus(
  projectId: string,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoRuntimeStatusResponse> {
  const { data } = await getRuntimeStatus({
    path: { project_id: projectId },
    signal,
    throwOnError: true,
  })
  return data
}
