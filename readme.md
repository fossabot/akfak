# akfak

Iteration of some garbage I wrote forever ago. Has been superseded by the newer version that uses dpkp/kafka-python for SASL support.

## Usage

See the sample [config][] for a basic setup. You can set Zabbix alerting levels down to the topic & consumer pair level. Whatever is defined lowest is what will be used at the cluster/topic level.

For example, in the sample config, under dc01, there is a cluster level default for disaster, this replaces **all** of the default alerts and only places the single disaster alert. Further down we see there is alert levels set for mclogs-prod consumer (both disaster & high). This will replace all the alerts at the cluster level for dc01 just for mclogs-prod consumer.

This makes it possible to specify individual topics & consumers that should trigger instead of just a cluster wide, one-size-fits-all alerting level.

The possible alerting values are *average*, *high* and *disaster* (1-3, normal being 0).

Starting the actual fetching server is straightforward, use `$ akfak server` or specify the config location with `$ akfak server --config /path/to/config.yaml`. The 'API' portion is used for Zabbix auto discovery (not required if you're not using it for Zabbix auto discovery though). The API presents two endpoints `/zabbix` for querying Zabbix keys to retrieve topic & consumer pairs and `/lag` for lag related information (effectively what is being sent to graphite).

The `/lag` endpoint can also take a path down to individual keys within the final dictionary. For example, the path `/lag/dc01/mytopic/mytopic-prod` will give you all related info for the mytopic-prod consumer. You can even drill down to individual partitions with `/lag/.../parts/0/lag`.

## Notes

* To list multiple consumers, just define each as an empty dict in the YAML under the consumers for a topic (unless you need to specify alert levels for the consumer)
* If you do not wish to use graphite or zabbix, just leave them out of the top level settings dict, this will disable their output
* The default fetch & send intervals are set to 10 seconds, any fetch tasks that take longer than the timeout will be ignored, this is to avoid waiting on a hanging broker/consumer for metadata.
* Fetching & sending to outputs is separated; sending to graphite/zabbix should not affect the fetch cycle
* You can have **multiple clusters with the same name**. Doing so will create multiple pykafka KafkaClients. So if you find that the fetch cycle is taking too long, try splitting up the clusters into multiple of the same cluster and spread out the topics & consumers, this should let Akfak start multiple clients in parallel during a fetch cycle. The end results will be compiled together, so multiple clients under the same name will still have their stats available under the API portion just the same
* The offset fetch retry time is 10ms, the default is far too long, so even if there are issues grabbing the consumer offsets for a topic it will retry immediately
* Inactive partitions give back their offset as -1, which we interpret as zero lag to avoid weirdness where `current offset - -1 = current offset + 1` that some people were seeing


[config]: config.yaml
