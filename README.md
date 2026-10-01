# tenantsched — 单 CPU 租户层级调度模拟器

纯 Go 实现，不启动任何真实作业；只在整数 tick 上推进一个确定性的
调度决策模型，并给出可重放的逐 tick 轨迹与超期证据。

## 模型

- **组树**：最多 20 个节点、最深 4 层（根为第 1 层），恰好一个根。
  每个组有每周期配额 `Quota`，周期固定为 `PeriodTicks = 10` 个 tick。
- **作业**：最多 40 个。字段为释放时刻 `ReleaseAt`、工作量 `Work`、
  截止 tick `Deadline`、所属组。单 CPU 每 tick 最多执行一个作业 1 单位。
- **每 tick 的顺序**（见 `stepLocked`）：
  1. 若 `tick % 10 == 0`，所有组本周期已用配额先清零；
  2. 处理自上一个 tick 以来已提交的变更（提交/迁组/取消），计入本 tick
     的 `Applied` 列表；变更提交即生效，因此“本 tick 之前提交”的作业
     在本 tick 就参与调度；
  3. 在**已释放且到根路径上每个祖先仍有配额**的就绪作业中，按
     `(Deadline, JobID)` 升序选一个执行，沿作业所在组到根的路径同时
     扣减每一级 1 个配额；
  4. 无可执行作业时记录空转：
     - `IDLE_NOT_READY`：没有已释放、仍有工作量的作业；
     - `IDLE_BLOCKED`：有就绪作业但全被配额挡住，`BlockedBy` 记录
       排序后第一个候选到根路径上**最深**（离作业最近）的耗尽祖先；
  5. 在截止 tick 结束时仍有剩余工作量的作业产出一条 `OverdueEvidence`
     （含剩余量、当时所属组、最深耗尽祖先），同一作业只记录一次。
- **迁组不退还配额**：此前在旧祖先路径上扣掉的用量保持不变。

## 修订号与并发

- 初始修订号为 0；每次成功的提交/迁组/取消 +1，每个执行的 tick +1。
- 所有写接口（`Submit`/`Migrate`/`Cancel`/`Advance`）都要求传入
  `expectedRevision`，不一致即返回包装了 `ErrConflict` 的错误且无副作用。
- `Advance(ticks, rev)` 在单把互斥锁内整体线性化：并发推进要么因修订号
  冲突被拒，要么串行执行互不相交的 tick 区间，**同一 tick 不可能执行
  两次**；失败调用不推进时钟、不产生轨迹。
- 修订号的参数校验顺序：先校验参数/存在性/作业状态，再比较修订号
  （陈旧客户端不会因参数错误而探测到状态差异）。

## API 速览

```go
s, _ := tenantsched.New([]tenantsched.GroupSpec{
    {ID: "root", Quota: 100},
    {ID: "a", Parent: "root", Quota: 4},
})

r, _ := s.Submit(tenantsched.JobSpec{
    ID: "j1", ReleaseAt: 0, Work: 6, Deadline: 9, Group: "a",
}, s.Revision())

res, _ := s.Advance(10, r.Revision)
fmt.Print(tenantsched.RenderTrace(res.Ticks))
fmt.Print(tenantsched.RenderOverdue(res.Overdue))

snap := s.Snapshot() // now/revision、作业、各组当前周期配额用量
```

错误哨兵：`ErrInvalidArgument`、`ErrLimitExceeded`、`ErrConflict`、
`ErrNotFound`、`ErrJobTerminal`，用 `errors.Is` 判定。

## 测试

- `reference_test.go`：独立重写的逐 tick 参考调度器（独立数据模型与
  决策函数），只在输出端映射到相同 DTO。
- `scheduler_test.go`：四个确定性脚本场景（四层祖先耗尽 + 周期重置 +
  超期、迁组不退还 + deadline/ID 决胜、空转与取消、同 tick 提交即调度），
  产品与参考实现并排执行、**逐 tick 逐字段**比对；另含修订号冲突、
  结构校验、40 作业上限、pending 变更归属等用例。
- `concurrency_test.go`：
  - 屏障测试：16 个 goroutine 持同一修订号同时 `Advance(1)`，每轮恰好
    1 个赢家、15 个冲突，无重复 tick；
  - 冲突后读最新修订号重试的多 worker 循环，成功次数恰等于 tick 数；
  - 陈旧修订号风暴：成功调用的 tick 区间两两不相交且恰好覆盖 `[0,now)`；
  - 屏障并发提交：30 个同修订号提交恰好 1 个成功，且每个提交只在某一个
    tick 的 `Applied` 中出现一次；
  - 轨迹事件流独立重放审计：扣减账目、配额上限、周期归零、超期剩余量、
    修订号 = 变更数 + tick 数、tick 连续无重复。

```bash
go test -race -count=5 ./...
```
