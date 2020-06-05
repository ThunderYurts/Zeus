# Zeus

Zues 是 ThunderYurts 系统的 Master服务，基于 Zookeeper 管理 Yurt。

## Mode

Zeus 支持两种模式，可以是可以是主从模式，也可以是 Replica 模式，也可以是两者同时开启。

**Master-Backup** : Zeus 支持主备，利用 Zookeeper 在 Zeus 崩溃的情况下能够实现快速的替换。替换流程如下：

1.  Zeus backup watch Zeus tmp，如果发现临时文件不存在，自行创建临时文件，并写入自己的IP和端口。如果发现临时文件已经存在，则 watch 相应节点。

2.  Zeus master在网络波动或其他的情况下，将会与 zookeeper 的 断开会话，导致临时文件失效。此时可能会出现 master 与 backup race 的情况，但是因 Zeus 是 stateless 的，因此谁成为master 另外一个都会是backup，不会出现多主的情况。

**Replica** : Zeus 支持 replica ，也是因起 stateless 的特性，能够让 Zeus 负责部分 Yurts 的管理。从而在一定程度上能够提升Availability。

## Task

1. 负责使用类似 Redis virtual slots 对已经注册 Yurt 进行工作的分配，分配的结果将记录在 Zookeeper 中。
2. 负责对于 Yurt 节点增加或减少情况的处理，需要协调数据的转移。

注意 Zeus 控制 Yurts 均是通过修改 zookeeper 中相应位置的数据来实现的，Yurt (Primary/Secondary) 均是通过阅读到了相应的数据发生修改之后，将会通过各种行动将自己的状态趋向 zookeeper 中所描述的状态，这一点和 Kubernetes 的设计理念相同。

## Dependency

Zookeeper：负责整个的 ThunderYurts 集群的管理

gRPC：负责与 Yurt 进行交互，例如控制数据转移等等。



