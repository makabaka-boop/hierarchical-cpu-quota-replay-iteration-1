# tenantsched — 单 CPU 租户层级调度模拟器

纯 Go 实现，不启动任何真实作业；只在整数 tick 上推进一个确定性的
调度决策模型，并给出可重放的逐 tick 轨迹与超期证据。

## 模型

- **组树**：最多 20 个节点、最深 4 层（根为第 1 层），恰好一个根。
  每个组有每周期配额 `Quota`，周期固定为 `PeriodTicks = 10` 个 tick。
- **作业**：最多 40 个。字段为释放时刻 `ReleaseAt`、工作量 `Work`、
  截止 tick `Deadline`、所属组。单 CPU 每 tick 最多执行一个作业 1 单位。
- **每 tick 的顺序**（见 `stepLocked`）：
  1. 若 `tick % 10 == 0`，所有组本周期已用配额先清零，上一周期的兄弟组
     临时转让全部失效、有效额度恢复为基础配额；
  2. 处理自上一个 tick 以来已提交的变更（提交/迁组/取消/转让），计入本 tick
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

## 同父兄弟组配额转让

- `TransferQuota(donor, recipient, amount, expectedRevision)` 把 donor 的
  `amount` 个**本周期**配额临时转给同一父组下的兄弟组 recipient。
- 校验顺序：未知组 / 同组 / 非同父兄弟 / `amount <= 0` 先判，再比较修订号
  （陈旧客户端不能借参数错误探测状态），最后要求
  `amount <= donor 当前周期未用额度（有效额度 - 已用）`，超额返回
  `ErrInvalidArgument` 且无副作用。
- 转让只改两组**本周期有效额度**（donor −amount、recipient +amount）：
  不增加父组或任何祖先额度，不改变任何组的基础配额，也不退还已执行 tick
  的扣减。周期边界（下一周期首个 tick）先归零并恢复基础额度，转让失效。
- 落在周期边界上的临界提交（如 `now == 10`）按**新周期**恢复后的基础
  额度计算未用量，绝不会读到上一周期余额；校验失败不会提前结算周期。
- 转让提交即修订号 +1、立即生效，并作为 `TRANSFER` 变更进入**下一 tick**
  的 `Applied`（字段 `FromGroup`/`Group`/`Amount`）。之后的配额阻挡与
  超期证据都按当时有效额度重放。
- 快照 `Quotas` 同时展示 `Quota`（基础额度）、`EffectiveQuota`（含本周期
  转让净值）与 `Used`（已用量）。

## 修订号与并发

- 初始修订号为 0；每次成功的提交/迁组/取消/转让 +1，每个执行的 tick +1。
- 所有写接口（`Submit`/`Migrate`/`Cancel`/`TransferQuota`/`Advance`）都要求传入
  `expectedRevision`，不一致即返回包装了 `ErrConflict` 的错误且无副作用。
- `Advance(ticks, rev)` 在单把互斥锁内整体线性化：并发推进要么因修订号
  冲突被拒，要么串行执行互不相交的 tick 区间，**同一 tick 不可能执行
  两次**；并发推进与并发转让（或两次转让）持同一旧修订号时最多一笔成功，
  失败调用不推进时钟、不产生轨迹、不改任何额度。
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

// 同父兄弟组之间临时转让本周期未用配额（跨周期自动失效）。
tr, _ := s.TransferQuota("idle-sibling", "a", 2, s.Revision())

res, _ := s.Advance(10, tr.Revision)
fmt.Print(tenantsched.RenderTrace(res.Ticks))
fmt.Print(tenantsched.RenderOverdue(res.Overdue))

snap := s.Snapshot() // now/revision、作业、各组基础额度/有效额度/已用量
```

错误哨兵：`ErrInvalidArgument`、`ErrLimitExceeded`、`ErrConflict`、
`ErrNotFound`、`ErrJobTerminal`，用 `errors.Is` 判定。

## 测试

- `reference_test.go`：独立重写的逐 tick 参考调度器（独立数据模型与
  决策函数），只在输出端映射到相同 DTO。
- `scheduler_test.go`：五个确定性脚本场景（四层祖先耗尽 + 周期重置 +
  超期、迁组不退还 + deadline/ID 决胜、空转与取消、同 tick 提交即调度、
  兄弟组转让 + 跨周期失效 + 临界提交），
  产品与参考实现并排执行、**逐 tick 逐字段**比对；另含修订号冲突、
  结构校验、40 作业上限、pending 变更归属等用例。
- `transfer_test.go`：转让主场景（救截止）、按有效额度重放阻挡/超期、
  祖先额度不增加、周期内未用额度上界、周期边界临界提交（失败不结算、
  成功按新周期余额）、整周期后失效、参数校验顺序、转让修订竞争屏障、
  推进与转让混合竞争、迁组+转让组合、轨迹重放审计、无转让时输出不变。
- `concurrency_test.go`：
  - 屏障测试：16 个 goroutine 持同一修订号同时 `Advance(1)`，每轮恰好
    1 个赢家、15 个冲突，无重复 tick；
  - 冲突后读最新修订号重试的多 worker 循环，成功次数恰等于 tick 数；
  - 陈旧修订号风暴：成功调用的 tick 区间两两不相交且恰好覆盖 `[0,now)`；
  - 屏障并发提交：30 个同修订号提交恰好 1 个成功，且每个提交只在某一个
    tick 的 `Applied` 中出现一次；
  - 轨迹事件流独立重放审计：扣减账目、按有效额度（含转让净值）的配额上限
    与阻挡判定、兄弟转让的捐出组未用额度上界、周期归零、超期剩余量、
    修订号 = 变更数 + tick 数、tick 连续无重复。

```bash
go test -race -count=5 ./...
```
