# ProcessForge 进程内复审执行组件

Go 库：执行由有向无环模板（最多 50 个节点）描述的复审流程。节点分普通任务与双人复核门两种；调用者身份可信，不做持久化与崩溃恢复。

## API

```go
inst, err := processforge.Create(tmpl /*, processforge.WithBeforePublishHook(h) */)
err  = inst.Act(nodeID, round, actor)   // 完成任务 / 复核门投一票
err  = inst.Recheck(gateID, round)      // 门及其传递后继轮次+1，结果与票据失效
snap := inst.View()                     // 全部节点轮次/状态/票据的一致快照
```

- 模板校验：DAG、节点数上限、门必须恰有两名不同审批人。
- 过期轮次的 `Act`/`Recheck` 返回 `ErrStaleRound` 且无任何副作用。
- 三个操作可任意并发调用，结果可解释为遵守调用先后关系的某个串行执行。

## 并发设计

- 每个节点状态是不可变结构，通过 `atomic.Pointer` 整体替换。
- `Act` 只对目标节点做 CAS；暂停校验后不持有任何锁，不影响同实例其他操作。
- `Recheck` 按拓扑规范序对受影响节点逐节点加锁（`Act`/`View` 不参与），持锁后重新校验再整体发布；无整实例锁、无全局锁、无单工作线程。
- `View` 借助回退开始/完成计数器避开发布窗口，绝不读到部分生效的回退；返回的快照为深拷贝，之后不被改写。

## 测试

```sh
go test -race ./...
```

交错场景用 channel 屏障与 `WithBeforePublishHook` 暂停点精确控制，不依赖 sleep。
