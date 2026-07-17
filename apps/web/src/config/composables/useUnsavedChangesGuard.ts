import { onMounted, onUnmounted, toValue, type MaybeRefOrGetter } from 'vue'
import { onBeforeRouteLeave, onBeforeRouteUpdate } from 'vue-router'

export function useUnsavedChangesGuard(
  isDirty: MaybeRefOrGetter<boolean>,
  clear: () => void,
): void {
  function beforeUnload(event: BeforeUnloadEvent): void {
    if (!toValue(isDirty)) return
    event.preventDefault()
    event.returnValue = ''
  }

  function confirmNavigation(): boolean {
    if (toValue(isDirty) && !window.confirm('Discard unsaved configuration changes?')) return false
    clear()
    return true
  }

  onBeforeRouteLeave(confirmNavigation)
  onBeforeRouteUpdate((to, from) => {
    if (to.params.projectId === from.params.projectId) return true
    return confirmNavigation()
  })
  onMounted(() => window.addEventListener('beforeunload', beforeUnload))
  onUnmounted(() => window.removeEventListener('beforeunload', beforeUnload))
}
