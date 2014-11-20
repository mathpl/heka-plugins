heka-plugins
============

Heka plugins:
 - OpentsdbZabbixFilter: Generates ZabbixEncoded message from OpentsdbEncoded messages. (works with https://github.com/hynd/heka-tsutils-plugins/tree/master/opentsdb)
 - OpenTsdbToZabbixEncoder: Generates a single json encoded zabbix metric.
 - ZabbixOutput: Dual role: Batches Zabbix metric and filters what to send according to "active checks" list found on zabbix server.

hekad.tmol: example of a config send both the data to openstdb unfiltered and to zabbix with filter from a single opentsdb input.
