# ProcessForge

进程内复审执行组件（Go 库，无外部依赖）。处理有向无环模板（最多 50 个
节点），节点类型为普通任务（`KindTask`）与双人复核门（`KindGate`）。

## API

```go
tpl := processforge.Template{Nodes: []processforge.NodeSpec{
    {ID: "draft", Kind: processforge.KindTask},
    {ID: "review", Kind: processforge.KindGate,
        Approvers: [2]string{"alice", "bob"}, Deps: []string{"draft"}},
    {ID: "publish", Kind: processforge.KindTask, Deps: []string{"review"}},
}}

in, err := processforge.Create(tpl)          // 所有节点轮次从 1 开始
err = in.Act("draft", 1, "carol")            // 完成普通任务
err = in.Act("review", 1, "alice")           // 复核门投一票
err = in.Act("review", 1, "bob")             // 集齐两票，门通过
err = in.Recheck("review", 1)                // 退回复审：门及传递后继轮次 +1
snap := in.View()                            // 全部节点的一致快照
```

- `Act`/`Recheck` 携带的 `round` 必须等于目标节点当前轮次，否则返回
  `ErrStaleRound` 且无任何副作用。
- `Recheck` 使目标门及其全部传递后继轮次加一、完成结果与票据失效；
  其他节点（含汇合点另一分支的已完成结果）不受影响。
- `Act`、`Recheck`、`View` 可任意并发调用，结果可串行化解释。

## 并发设计

无实例级锁：每个节点一把 `sync.Mutex`，多节点临界区一律按拓扑序号
升序加锁（无环、无死锁）。操作为两阶段：校验（短暂持锁）→ 发布
（重新校验轮次后生效）。`Hooks` 提供测试暂停点，在校验完成、发布
之前触发，触发时不持有任何锁，因此暂停一个 `Act` 不会阻塞同实例的
其他操作。

## 测试

```sh
go test -race ./...
```

交错时序由 channel 屏障精确控制，不依赖 sleep。
