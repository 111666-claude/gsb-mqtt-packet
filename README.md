# mqtt-packet

解析 MQTT 报文体并按订阅分发：变长剩余长度、主题过滤通配符、投递 QoS 上限、未确认去重，以及订阅匹配的索引。只用标准库。

```
go test ./...
go vet ./...
go run ./cmd/mqttpkt --sample=varlen
go run ./cmd/mqttpkt --sample=wildcard
go run ./cmd/mqttpkt --sample=hash
go run ./cmd/mqttpkt --sample=qos
go run ./cmd/mqttpkt --sample=dup
go run ./cmd/mqttpkt --sample=work
```

## 口径

- **剩余长度**：变长整数，每字节低 7 位有效、最高位表示还有后续字节，最多 4 字节；声明长度超出实际字节报 `short`。
- **报文**：PUBLISH 的主题以 2 字节长度前缀开头；只有 QoS 大于 0 才带 2 字节包标识；QoS 0 的负载紧跟主题。
- **主题过滤**：`/` 分层；`+` 匹配恰好一层；`#` 匹配零层或多层，且必须是最后一段。
- **投递 QoS**：取发布 QoS 与订阅 QoS 的较小值。
- **去重**：同一客户端同一包标识在未确认前只投递一次。
- **匹配代价**：订阅很多时，一次发布只访问可能命中的分支；`scanned` 不随订阅数乘发布数放大。

## 不变量

- 每个包正文长度等于声明的剩余长度。
- 每次投递的 QoS 不超过发布与订阅的 QoS。
- 同一客户端的同一包标识不产生第二次投递。
- 八百条订阅上发布八百次的 `scanned` 不超过 6000。

## 输出契约

`Stream` 按序号取包；`Broker.Publish` 返回投递列表（每条含客户端、主题与 QoS）。
场景打印一行 JSON，出错打印 `{"error": ...}`。`--sample=work` 打印 `{"subs": N, "scanned": N}`。
