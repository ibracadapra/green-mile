package com.syriatel.d3m.greenmile.profiling

import com.syriatel.d3m.greenmile.utils.materializedAsKeyValueStore
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KTable
import kotlin.reflect.jvm.internal.impl.descriptors.FieldDescriptor


fun StreamsBuilder.customerTags(): KTable<String, Set<String>> = stream<String, TagEvent>("tags", Consumed.with(serdeFor(), serdeFor()))
        .groupByKey().aggregate(
                { setOf() }, { _, v, a ->
            when (v.operation) {
                TagOperation.Add -> a + v.tag
                TagOperation.Remove -> a - v.tag
            }
        }, materializedAsKeyValueStore<String, Set<String>>("tags-store")
                .withKeySerde(serdeFor())
                .withValueSerde(serdeFor())
        )

class TagEvent(
        val tag: String,
        val operation: TagOperation = TagOperation.Add
)

enum class TagOperation {
    Add, Remove
}


