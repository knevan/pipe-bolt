<script setup lang="ts">
import type {
  PipeBoltDomainConfigDeviceIdExtraction,
  PipeBoltDomainConfigPayloadSchemaMapping,
  PipeBoltDomainConfigTopicRouteConfig,
} from '@/api/generated'

const props = defineProps<{
  brokers: ReadonlyArray<{ id: string; name: string }>
  modelValue: PipeBoltDomainConfigTopicRouteConfig[]
  schemaMappings: ReadonlyArray<Pick<PipeBoltDomainConfigPayloadSchemaMapping, 'id' | 'name'>>
}>()
const emit = defineEmits<{ 'update:modelValue': [value: PipeBoltDomainConfigTopicRouteConfig[]] }>()

function update(index: number, patch: Partial<PipeBoltDomainConfigTopicRouteConfig>): void {
  const next = structuredClone(props.modelValue)
  const current = next[index]
  if (!current) return
  next[index] = { ...current, ...patch }
  emit('update:modelValue', next)
}

function add(): void {
  emit('update:modelValue', [
    ...props.modelValue,
    {
      backpressure: 'reject',
      broker_id: props.brokers[0]?.id ?? '',
      codec: 'json',
      device_id: { type: 'none' },
      enabled: false,
      event_type: 'telemetry',
      id: `route-${crypto.randomUUID()}`,
      name: 'New route',
      qos: 'at_least_once',
      schema_mapping_id: null,
      topic_filter: 'devices/+/telemetry',
    },
  ])
}

function remove(index: number): void {
  emit(
    'update:modelValue',
    props.modelValue.filter((_, itemIndex) => itemIndex !== index),
  )
}

function changeDeviceType(
  index: number,
  type: PipeBoltDomainConfigDeviceIdExtraction['type'],
): void {
  const value: PipeBoltDomainConfigDeviceIdExtraction =
    type === 'static'
      ? { type, value: '' }
      : type === 'topic_wildcard_index'
        ? { index: 0, type }
        : type === 'payload_field'
          ? { path: 'device_id', type }
          : { type: 'none' }
  update(index, { device_id: value })
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">ROUTING</p>
        <h2>Topic routes</h2>
      </div>
      <button class="button button-secondary" type="button" @click="add">Add route</button>
    </div>
    <p v-if="!modelValue.length" class="config-empty">No routes configured.</p>
    <article v-for="(route, index) in modelValue" :key="route.id" class="config-item">
      <div class="config-item-heading">
        <div>
          <span class="config-index">{{ index + 1 }}</span
          ><strong>{{ route.name || 'Unnamed route' }}</strong>
        </div>
        <div class="config-item-actions">
          <label class="toggle-label"
            ><input
              :checked="route.enabled"
              type="checkbox"
              @change="
                update(index, { enabled: ($event.currentTarget as HTMLInputElement).checked })
              "
            />Enabled</label
          ><button class="danger-link" type="button" @click="remove(index)">Remove</button>
        </div>
      </div>
      <div class="config-form-grid config-form-grid-dense">
        <label class="field"
          ><span>ID</span
          ><input
            :value="route.id"
            @input="update(index, { id: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Name</span
          ><input
            :value="route.name"
            maxlength="160"
            @input="update(index, { name: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field field-wide"
          ><span>Topic filter</span
          ><input
            :value="route.topic_filter"
            maxlength="1024"
            @input="
              update(index, { topic_filter: ($event.currentTarget as HTMLInputElement).value })
            "
        /></label>
        <label class="field"
          ><span>Broker</span
          ><select
            :value="route.broker_id"
            @change="
              update(index, { broker_id: ($event.currentTarget as HTMLSelectElement).value })
            "
          >
            <option value="">Select broker</option>
            <option v-for="broker in brokers" :key="broker.id" :value="broker.id">
              {{ broker.name }} · {{ broker.id }}
            </option>
          </select></label
        >
        <label class="field"
          ><span>Event type</span
          ><input
            :value="route.event_type"
            maxlength="160"
            @input="
              update(index, { event_type: ($event.currentTarget as HTMLInputElement).value })
            "
        /></label>
        <label class="field"
          ><span>QoS</span
          ><select
            :value="route.qos"
            @change="
              update(index, {
                qos: ($event.currentTarget as HTMLSelectElement).value as typeof route.qos,
              })
            "
          >
            <option value="at_most_once">At most once</option>
            <option value="at_least_once">At least once</option>
            <option value="exactly_once">Exactly once</option>
          </select></label
        >
        <label class="field"
          ><span>Backpressure</span
          ><select
            :value="route.backpressure"
            @change="
              update(index, {
                backpressure: ($event.currentTarget as HTMLSelectElement)
                  .value as typeof route.backpressure,
              })
            "
          >
            <option value="drop_newest">Drop newest</option>
            <option value="drop_oldest">Drop oldest</option>
            <option value="reject">Reject</option>
            <option value="block_producer">Block producer</option>
          </select></label
        >
        <label class="field"
          ><span>Codec</span
          ><select
            :value="route.codec"
            @change="
              update(index, {
                codec: ($event.currentTarget as HTMLSelectElement).value as typeof route.codec,
              })
            "
          >
            <option value="json">JSON</option>
            <option value="raw">Raw</option>
          </select></label
        >
        <label class="field"
          ><span>Schema mapping</span
          ><select
            :value="route.schema_mapping_id ?? ''"
            @change="
              update(index, {
                schema_mapping_id: ($event.currentTarget as HTMLSelectElement).value || null,
              })
            "
          >
            <option value="">None</option>
            <option v-for="mapping in schemaMappings" :key="mapping.id" :value="mapping.id">
              {{ mapping.name }} · {{ mapping.id }}
            </option>
          </select></label
        >
        <label class="field"
          ><span>Device ID extraction</span
          ><select
            :value="route.device_id.type"
            @change="
              changeDeviceType(
                index,
                ($event.currentTarget as HTMLSelectElement)
                  .value as PipeBoltDomainConfigDeviceIdExtraction['type'],
              )
            "
          >
            <option value="none">None</option>
            <option value="static">Static</option>
            <option value="topic_wildcard_index">Topic wildcard index</option>
            <option value="payload_field">Payload field</option>
          </select></label
        >
        <label v-if="route.device_id.type === 'static'" class="field"
          ><span>Static device ID</span
          ><input
            :value="route.device_id.value"
            @input="
              update(index, {
                device_id: {
                  type: 'static',
                  value: ($event.currentTarget as HTMLInputElement).value,
                },
              })
            "
        /></label>
        <label v-else-if="route.device_id.type === 'topic_wildcard_index'" class="field"
          ><span>Wildcard index</span
          ><input
            :value="route.device_id.index"
            min="0"
            type="number"
            @input="
              update(index, {
                device_id: {
                  index: ($event.currentTarget as HTMLInputElement).valueAsNumber,
                  type: 'topic_wildcard_index',
                },
              })
            "
        /></label>
        <label v-else-if="route.device_id.type === 'payload_field'" class="field"
          ><span>Payload field path</span
          ><input
            :value="route.device_id.path"
            @input="
              update(index, {
                device_id: {
                  path: ($event.currentTarget as HTMLInputElement).value,
                  type: 'payload_field',
                },
              })
            "
        /></label>
      </div>
    </article>
  </section>
</template>
