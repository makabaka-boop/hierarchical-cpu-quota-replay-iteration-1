package tenantsched_test

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"tenantsched"
)

// quotaMap 把快照中的配额视图按组 ID 索引，便于逐字段断言。
func quotaMap(snap tenantsched.Snapshot) map[string]tenantsched.QuotaView {
	out := make(map[string]tenantsched.QuotaView, len(snap.Quotas))
	for _, q := range snap.Quotas {
		out[q.GroupID] = q
	}
	return out
}

// TestSiblingTransferRelievesDeadline 是需求主场景：d 组本周期无作业，
// 相邻的 r 组基础额度将在截止 tick 前耗尽；d 把 2 个未用额度临时转给 r，
// r 的作业得以在截止 tick 内跑完。快照同时展示基础额度、有效额度与已用量，
// 转让作为一次已提交变更进入下一 tick 的 Applied，且父组额度纹丝不动。
func TestSiblingTransferRelievesDeadline(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 4},
		{ID: "r", Parent: "R", Quota: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jr", ReleaseAt: 0, Work: 5, Deadline: 4, Group: "r",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	rev := s.Revision()

	if _, err := s.Advance(3, rev); err != nil { // tick0..2 耗尽 r 的 3 个基础额度
		t.Fatal(err)
	}
	rev = s.Revision()

	tr, err := s.TransferQuota("d", "r", 2, rev)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	rev = tr.Revision

	// 提交后、下一 tick 前的快照：基础额度不变，有效额度只改 d/r 两组，
	// 已执行 tick 的扣减不退还（r 仍显示已用 3）。
	qs := quotaMap(s.Snapshot())
	if qd := qs["d"]; qd.Quota != 4 || qd.EffectiveQuota != 2 || qd.Used != 0 {
		t.Fatalf("donor view = %+v, want quota4/eff2/used0", qd)
	}
	if qr := qs["r"]; qr.Quota != 3 || qr.EffectiveQuota != 5 || qr.Used != 3 {
		t.Fatalf("recipient view = %+v, want quota3/eff5/used3", qr)
	}
	if qR := qs["R"]; qR.Quota != 100 || qR.EffectiveQuota != 100 || qR.Used != 3 {
		t.Fatalf("ancestor view = %+v, want quota100/eff100/used3", qR)
	}

	res, err := s.Advance(2, rev) // tick3 入账转让并执行；tick4 截止 tick 跑完
	if err != nil {
		t.Fatal(err)
	}
	if ap := res.Ticks[0].Applied; len(ap) != 1 || ap[0].Kind != tenantsched.ChangeTransfer ||
		ap[0].FromGroup != "d" || ap[0].Group != "r" || ap[0].Amount != 2 || ap[0].Revision != rev {
		t.Fatalf("tick3 applied = %+v, want one TRANSFER d->r x2 at committed revision", ap)
	}
	if len(res.Ticks[1].Applied) != 0 {
		t.Fatalf("transfer must appear in exactly one tick, got %+v", res.Ticks[1].Applied)
	}
	for i, e := range res.Ticks {
		if e.Kind != tenantsched.TickRan || e.JobID != "jr" {
			t.Fatalf("tick[%d] = %+v, want RAN jr", i, e)
		}
	}
	if len(res.Overdue) != 0 || len(s.Overdue()) != 0 {
		t.Fatalf("jr should meet deadline, got overdue %+v", s.Overdue())
	}

	qs = quotaMap(s.Snapshot())
	if qr := qs["r"]; qr.Used != 5 || qr.EffectiveQuota != 5 || qr.Quota != 3 {
		t.Fatalf("recipient after run = %+v, want quota3/eff5/used5", qr)
	}
	if qd := qs["d"]; qd.Used != 0 || qd.EffectiveQuota != 2 || qd.Quota != 4 {
		t.Fatalf("donor after run = %+v, want quota4/eff2/used0", qd)
	}
	if qR := qs["R"]; qR.Used != 5 || qR.EffectiveQuota != 100 {
		t.Fatalf("ancestor quota must not grow: %+v", qR)
	}
}

// TestTransferBlockingAndOverdueReplay 核对阻挡与超期证据必须按当时有效
// 额度重放：r 基础 3 + 受让 2 = 5，跑到 5 之后按有效额度判定为耗尽；
// 跨周期后转让失效，有效额度恢复基础 3。
func TestTransferBlockingAndOverdueReplay(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 4},
		{ID: "r", Parent: "R", Quota: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jr", ReleaseAt: 0, Work: 8, Deadline: 8, Group: "r",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(3, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferQuota("d", "r", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}

	// tick3 处理转让并执行（used4），tick4 再执行（used5，有效额度耗尽），
	// tick5..8 全部 IDLE_BLOCKED，最深阻挡祖先为 r。
	res, err := s.Advance(6, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if res.Ticks[0].Applied[0].Kind != tenantsched.ChangeTransfer {
		t.Fatalf("tick3 must apply transfer first, got %+v", res.Ticks[0].Applied)
	}
	for i := 2; i < len(res.Ticks); i++ {
		e := res.Ticks[i]
		if e.Kind != tenantsched.TickIdleBlocked || e.BlockedBy != "r" || e.ReadyCount != 1 {
			t.Fatalf("tick %d = %+v, want IDLE_BLOCKED by r", e.Tick, e)
		}
	}
	if len(res.Overdue) != 1 {
		t.Fatalf("overdue = %+v, want exactly one at tick8", res.Overdue)
	}
	ev := res.Overdue[0]
	// 已跑 5 个 tick（3 基础 + 2 受让），剩 3；阻挡按有效额度重放为 r。
	if ev.JobID != "jr" || ev.Remaining != 3 || ev.Blocker != "r" || ev.Group != "r" {
		t.Fatalf("overdue evidence = %+v", ev)
	}

	// tick9 继续阻挡；tick10 周期边界：先归零并恢复基础额度，转让失效。
	if _, err := s.Advance(2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	qs := quotaMap(s.Snapshot())
	if qr := qs["r"]; qr.Used != 1 || qr.EffectiveQuota != 3 || qr.Quota != 3 {
		t.Fatalf("post-boundary r = %+v, want quota3/eff3/used1", qr)
	}
	if qd := qs["d"]; qd.Used != 0 || qd.EffectiveQuota != 4 || qd.Quota != 4 {
		t.Fatalf("post-boundary d = %+v, want quota4/eff4/used0", qd)
	}
}

// TestTransferCannotRelieveAncestor 证明转让只改变兄弟两组的有效额度：
// 即使 b 给 a 转入 5，父组 R 只有 2 的额度，a 的作业在 R 处被挡，阻挡与
// 超期证据都指向真正耗尽的祖先 R。
func TestTransferCannotRelieveAncestor(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 2},
		{ID: "a", Parent: "R", Quota: 10},
		{ID: "b", Parent: "R", Quota: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "ja", ReleaseAt: 0, Work: 4, Deadline: 4, Group: "a",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferQuota("b", "a", 5, s.Revision()); err != nil {
		t.Fatal(err)
	}

	res, err := s.Advance(5, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	// tick0 先处理 SUBMIT+TRANSFER 再执行；tick0/1 ja 各跑一次后 R 耗尽；
	// tick2..4 阻挡。转让是 tick0 Applied 的最后一条。
	ap0 := res.Ticks[0].Applied
	last := ap0[len(ap0)-1]
	if last.FromGroup != "b" || last.Group != "a" || last.Amount != 5 || last.Kind != tenantsched.ChangeTransfer {
		t.Fatalf("tick0 last applied = %+v, want TRANSFER b->a x5", last)
	}
	for i := 2; i < 5; i++ {
		if e := res.Ticks[i]; e.Kind != tenantsched.TickIdleBlocked || e.BlockedBy != "R" {
			t.Fatalf("tick %d = %+v, want blocked by exhausted ancestor R", e.Tick, e)
		}
	}
	if ev := res.Overdue; len(ev) != 1 || ev[0].Remaining != 2 || ev[0].Blocker != "R" {
		t.Fatalf("overdue = %+v, want remaining 2 blocked by R", ev)
	}
	qs := quotaMap(s.Snapshot())
	if qR := qs["R"]; qR.Used != 2 || qR.EffectiveQuota != 2 {
		t.Fatalf("ancestor changed? %+v", qR)
	}
	if qa, qb := qs["a"], qs["b"]; qa.EffectiveQuota != 15 || qb.EffectiveQuota != 5 {
		t.Fatalf("sibling effective quotas wrong: a=%+v b=%+v", qa, qb)
	}
}

// TestTransferCapacityBoundWithinPeriod 校验额度上界随本周期已用与既有
// 转让收紧：捐出组用掉/转出多少，未用额度就少多少，超额拒绝且无副作用。
func TestTransferCapacityBoundWithinPeriod(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 4},
		{ID: "r", Parent: "R", Quota: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	// d 自己先跑掉 2 个额度。
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jd", ReleaseAt: 0, Work: 2, Deadline: 10, Group: "d",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	rev := s.Revision()

	if _, err := s.TransferQuota("d", "r", 3, rev); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("transfer 3 with only 2 unused: %v", err)
	}
	if s.Revision() != rev {
		t.Fatalf("rejected transfer bumped revision to %d", s.Revision())
	}
	if _, err := s.TransferQuota("d", "r", 2, rev); err != nil {
		t.Fatalf("transfer exactly unused 2: %v", err)
	}
	// d 现在 effective=2、used=2，未用为 0：任何正转让都必须失败。
	if _, err := s.TransferQuota("d", "r", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("donor with zero unused: %v", err)
	}
	qs := quotaMap(s.Snapshot())
	if qd := qs["d"]; qd.Used != 2 || qd.EffectiveQuota != 2 {
		t.Fatalf("d = %+v, want eff2/used2", qd)
	}
	if qr := qs["r"]; qr.Used != 0 || qr.EffectiveQuota != 5 {
		t.Fatalf("r = %+v, want eff5/used0", qr)
	}
}

// TestTransferBoundaryCommit 专门核对周期边界上的临界提交：now==10 时
// 上一周期的转让与已用都不能被误用——上一周期把 d 的 4 个额度全转出
// （上周期未用余额为 0），但新周期必须按恢复后的基础额度 4 重新判定。
// 失败的转让不得提前结算周期、不得改动任何账目。
func TestTransferBoundaryCommit(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 4},
		{ID: "r", Parent: "R", Quota: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jr", ReleaseAt: 0, Work: 5, Deadline: 9, Group: "r",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(3, s.Revision()); err != nil { // r used=3
		t.Fatal(err)
	}
	// 把 d 本周期 4 个未用额度全部转出：上一周期结束时 d 未用余额为 0，
	// r 有效额度 7，jr 还能跑 tick3/4 后被 r 挡住直至 tick9 超期。
	if _, err := s.TransferQuota("d", "r", 4, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(7, s.Revision()); err != nil { // tick3..9
		t.Fatal(err)
	}
	if s.Now() != 10 {
		t.Fatalf("now = %d, want 10", s.Now())
	}
	rev := s.Revision()

	// 超过新周期恢复后未用额度（4）的转让必须失败。
	if _, err := s.TransferQuota("d", "r", 5, rev); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("transfer 5 at boundary: %v", err)
	}
	// 失败不得提前结算周期：r 仍持有上周期账目（used=5、上周期 delta 仍在）。
	qs := quotaMap(s.Snapshot())
	if qr := qs["r"]; qr.Used != 5 || qr.EffectiveQuota != 3 {
		t.Fatalf("failed boundary transfer must not reset ledger: r=%+v", qr)
	}
	if qd := qs["d"]; qd.Used != 0 || qd.EffectiveQuota != 4 {
		t.Fatalf("failed boundary transfer must not reset ledger: d=%+v", qd)
	}
	if s.Revision() != rev {
		t.Fatalf("revision changed after rejection: %d", s.Revision())
	}

	// 关键断言：上一周期 d 未用余额为 0，但 amount=1 必须按新周期余额 4
	// 判定为合法。误用旧余额的实现会在这里错误拒绝。
	tr, err := s.TransferQuota("d", "r", 1, rev)
	if err != nil {
		t.Fatalf("boundary transfer must use new-period balance: %v", err)
	}
	rev = tr.Revision
	qs = quotaMap(s.Snapshot())
	if qd := qs["d"]; qd.Used != 0 || qd.EffectiveQuota != 3 || qd.Quota != 4 {
		t.Fatalf("d after boundary commit = %+v, want quota4/eff3/used0", qd)
	}
	if qr := qs["r"]; qr.Used != 0 || qr.EffectiveQuota != 4 || qr.Quota != 3 {
		t.Fatalf("r after boundary commit = %+v, want quota3/eff4/used0", qr)
	}

	// 转让作为下一 tick（新周期首个 tick）的已提交变更入账。
	res, err := s.Advance(1, rev)
	if err != nil {
		t.Fatal(err)
	}
	if e0 := res.Ticks[0]; e0.Tick != 10 || e0.Period != 1 ||
		len(e0.Applied) != 1 || e0.Applied[0].Kind != tenantsched.ChangeTransfer {
		t.Fatalf("tick10 = %+v, want TRANSFER applied in period 1", e0)
	}
}

// TestTransferExpiresAfterFullPeriod 转让只在提交时所在周期有效：周期 0
// 内有效额度含转让；进入周期边界后归零恢复基础额度，此后整个下一周期乃至
// 再跨一个边界都不再有任何残余。
func TestTransferExpiresAfterFullPeriod(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 4},
		{ID: "r", Parent: "R", Quota: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferQuota("d", "r", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	// 提交在周期 0：有效额度含转让。
	if qs := quotaMap(s.Snapshot()); qs["d"].EffectiveQuota != 2 || qs["r"].EffectiveQuota != 5 {
		t.Fatalf("period0 after commit: d=%+v r=%+v", qs["d"], qs["r"])
	}
	if _, err := s.Advance(5, s.Revision()); err != nil { // tick0..4 位于周期 0
		t.Fatal(err)
	}
	if qs := quotaMap(s.Snapshot()); qs["d"].EffectiveQuota != 2 || qs["r"].EffectiveQuota != 5 {
		t.Fatalf("mid period0 transfer must remain: d=%+v r=%+v", qs["d"], qs["r"])
	}
	if _, err := s.Advance(5, s.Revision()); err != nil { // 到 now==10：周期 0 已结束
		t.Fatal(err)
	}
	if qs := quotaMap(s.Snapshot()); qs["d"].EffectiveQuota != 4 || qs["r"].EffectiveQuota != 3 {
		t.Fatalf("transfer should expire at period 1: d=%+v r=%+v", qs["d"], qs["r"])
	}
	if _, err := s.Advance(10, s.Revision()); err != nil { // tick10..19 整个周期 1 无转让
		t.Fatal(err)
	}
	if qs := quotaMap(s.Snapshot()); qs["d"].EffectiveQuota != 4 || qs["r"].EffectiveQuota != 3 {
		t.Fatalf("period 2 view: d=%+v r=%+v", qs["d"], qs["r"])
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick20 结算
		t.Fatal(err)
	}
	qs := quotaMap(s.Snapshot())
	if qd, qr := qs["d"], qs["r"]; qd.EffectiveQuota != 4 || qr.EffectiveQuota != 3 || qd.Used != 0 || qr.Used != 0 {
		t.Fatalf("period2 ledgers = d:%+v r:%+v, want base quota and zero usage", qd, qr)
	}
}

// TestTransferValidation 覆盖结构性与参数校验，以及“参数先于修订号”的
// 校验顺序（陈旧客户端不能借参数错误探测状态）。
func TestTransferValidation(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 2},
		{ID: "x", Parent: "a", Quota: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	mustFail := func(name string, want error, fn func(uint64) error) {
		t.Helper()
		// 陈旧修订号 99 与当前修订号 0 都必须得到同样的参数/存在性结论。
		if err := fn(99); !errors.Is(err, want) {
			t.Fatalf("%s stale rev: err=%v want %v", name, err, want)
		}
		if err := fn(s.Revision()); !errors.Is(err, want) {
			t.Fatalf("%s current rev: err=%v want %v", name, err, want)
		}
	}
	mustFail("unknown donor", tenantsched.ErrNotFound, func(rev uint64) error {
		_, e := s.TransferQuota("ghost", "b", 1, rev)
		return e
	})
	mustFail("unknown recipient", tenantsched.ErrNotFound, func(rev uint64) error {
		_, e := s.TransferQuota("a", "ghost", 1, rev)
		return e
	})
	mustFail("same group", tenantsched.ErrInvalidArgument, func(rev uint64) error {
		_, e := s.TransferQuota("a", "a", 1, rev)
		return e
	})
	mustFail("non-siblings", tenantsched.ErrInvalidArgument, func(rev uint64) error {
		_, e := s.TransferQuota("a", "x", 1, rev) // a 与 x 是父子，不是兄弟
		return e
	})
	mustFail("zero amount", tenantsched.ErrInvalidArgument, func(rev uint64) error {
		_, e := s.TransferQuota("a", "b", 0, rev)
		return e
	})
	mustFail("negative amount", tenantsched.ErrInvalidArgument, func(rev uint64) error {
		_, e := s.TransferQuota("a", "b", -3, rev)
		return e
	})

	// 参数合法但修订号陈旧：必须是 ErrConflict，且无副作用。
	if _, err := s.TransferQuota("a", "b", 1, 99); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale conflict: %v", err)
	}
	if s.Revision() != 0 {
		t.Fatalf("revision changed without successful transfer: %d", s.Revision())
	}

	// 合法转让成功。
	if _, err := s.TransferQuota("a", "b", 1, s.Revision()); err != nil {
		t.Fatalf("valid transfer: %v", err)
	}
}

// TestTransferRevisionRaceBarrier：16 个 goroutine 持同一旧修订号并发
// 发起转让（一半 a->b，一半 b->a），最多成功一笔；失败不改修订号、
// 不改进度、不入轨迹。
func TestTransferRevisionRaceBarrier(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 5},
		{ID: "b", Parent: "R", Quota: 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(workers)

	var wins, conflicts int32
	var winMu sync.Mutex
	var winner struct{ from, to string }
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer done.Done()
			start.Wait()
			from, to := "a", "b"
			if w%2 == 1 {
				from, to = "b", "a"
			}
			r, err := s.TransferQuota(from, to, 1, 0)
			switch {
			case err == nil:
				atomic.AddInt32(&wins, 1)
				winMu.Lock()
				winner.from, winner.to = from, to
				_ = r
				winMu.Unlock()
			case errors.Is(err, tenantsched.ErrConflict):
				atomic.AddInt32(&conflicts, 1)
			default:
				t.Errorf("unexpected transfer error: %v", err)
			}
		}()
	}
	start.Done()
	done.Wait()

	if wins != 1 || conflicts != workers-1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	if s.Revision() != 1 {
		t.Fatalf("revision=%d, want 1", s.Revision())
	}
	qs := quotaMap(s.Snapshot())
	wantEff := map[string][2]int{"a": {4, 6}, "b": {6, 4}} // a->b 或 b->a
	we := wantEff[winner.from]
	if qs["a"].EffectiveQuota != we[0] || qs["b"].EffectiveQuota != we[1] {
		t.Fatalf("winner=%s->%s effective: a=%d b=%d, want %d/%d",
			winner.from, winner.to, qs["a"].EffectiveQuota, qs["b"].EffectiveQuota, we[0], we[1])
	}

	// 唯一一笔成功转让在下一 tick 的 Applied 中恰好出现一次。
	res, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if ap := res.Ticks[0].Applied; len(ap) != 1 ||
		ap[0].Kind != tenantsched.ChangeTransfer ||
		ap[0].FromGroup != winner.from || ap[0].Group != winner.to || ap[0].Amount != 1 {
		t.Fatalf("applied = %+v, winner was %s->%s", ap, winner.from, winner.to)
	}
}

// TestTransferMixedWithAdvanceBarrier：并发的推进与转让持同一旧修订号，
// 全局最多一笔成功；若推进赢，则该 tick 轨迹不含任何变更；若转让赢，则
// 时钟不动且转让成为下一 tick 的 Applied。
func TestTransferMixedWithAdvanceBarrier(t *testing.T) {
	for round := 0; round < 10; round++ {
		s, err := tenantsched.New([]tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
			{ID: "a", Parent: "R", Quota: 5},
			{ID: "b", Parent: "R", Quota: 5},
		})
		if err != nil {
			t.Fatal(err)
		}

		const workers = 16
		var start sync.WaitGroup
		start.Add(1)
		var done sync.WaitGroup
		done.Add(workers)
		var wins int32
		var kindMu sync.Mutex
		winnerKind := ""
		for w := 0; w < workers; w++ {
			w := w
			go func() {
				defer done.Done()
				start.Wait()
				if w%2 == 0 {
					if _, err := s.Advance(1, 0); err == nil {
						atomic.AddInt32(&wins, 1)
						kindMu.Lock()
						winnerKind = "advance"
						kindMu.Unlock()
					} else if !errors.Is(err, tenantsched.ErrConflict) {
						t.Errorf("advance: %v", err)
					}
				} else {
					if _, err := s.TransferQuota("a", "b", 1, 0); err == nil {
						atomic.AddInt32(&wins, 1)
						kindMu.Lock()
						winnerKind = "transfer"
						kindMu.Unlock()
					} else if !errors.Is(err, tenantsched.ErrConflict) {
						t.Errorf("transfer: %v", err)
					}
				}
			}()
		}
		start.Done()
		done.Wait()

		if wins != 1 {
			t.Fatalf("round %d: wins=%d", round, wins)
		}
		var appliedTransfers, ticks int
		if winnerKind == "advance" {
			if s.Now() != 1 {
				t.Fatalf("round %d: advance won but now=%d", round, s.Now())
			}
			ticks = 1
		} else {
			if s.Now() != 0 || s.Revision() != 1 {
				t.Fatalf("round %d: transfer won but now=%d rev=%d", round, s.Now(), s.Revision())
			}
			ticks = 1
			if _, err := s.Advance(1, s.Revision()); err != nil { // 让转让入账
				t.Fatal(err)
			}
		}
		for _, e := range s.Trace() {
			for _, a := range e.Applied {
				if a.Kind == tenantsched.ChangeTransfer {
					appliedTransfers++
				}
			}
		}
		wantTransfers := 0
		if winnerKind == "transfer" {
			wantTransfers = 1
		}
		if appliedTransfers != wantTransfers || len(s.Trace()) != ticks {
			t.Fatalf("round %d (%s): transfers=%d ticks=%d",
				round, winnerKind, appliedTransfers, len(s.Trace()))
		}
	}
}

// TestTransferWithMigration 组合迁组与转让：b 受让给 a 后有效额度被自己
// 用满，作业迁到 b 不能运行；迁组不退还 a 的扣减，阻挡证据按 b 的当时
// 有效额度（0）重放。
func TestTransferWithMigration(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 1},
		{ID: "b", Parent: "R", Quota: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jm", ReleaseAt: 0, Work: 3, Deadline: 4, Group: "a",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(2, s.Revision()); err != nil { // tick0 跑，tick1 被 a 挡
		t.Fatal(err)
	}
	if e1 := s.Trace()[1]; e1.Kind != tenantsched.TickIdleBlocked || e1.BlockedBy != "a" {
		t.Fatalf("tick1 = %+v, want blocked by a", e1)
	}
	// b 把唯一的 1 个额度让给 a：a eff=2 再跑一次，b eff=0。
	if _, err := s.TransferQuota("b", "a", 1, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick2 入账转让并执行
		t.Fatal(err)
	}
	// 迁到 b：b 有效额度 0，tick4 截止前在 b 上被挡。
	if _, err := s.Migrate("jm", "b", s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(2, s.Revision()) // tick3 阻挡，tick4 阻挡并超期
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range res.Ticks {
		if e.Kind != tenantsched.TickIdleBlocked || e.BlockedBy != "b" {
			t.Fatalf("tick %d = %+v, want blocked by b (effective 0)", e.Tick, e)
		}
	}
	if ev := res.Overdue; len(ev) != 1 || ev[0].Group != "b" || ev[0].Remaining != 1 || ev[0].Blocker != "b" {
		t.Fatalf("overdue = %+v", ev)
	}
	qs := quotaMap(s.Snapshot())
	if qa, qb := qs["a"], qs["b"]; qa.Used != 2 || qb.Used != 0 || qa.EffectiveQuota != 2 || qb.EffectiveQuota != 0 {
		t.Fatalf("post-migrate ledgers: a=%+v b=%+v", qa, qb)
	}

	// 渲染器必须能描述 TRANSFER（防止输出侧回归）。
	var rendered bool
	for _, line := range strings.Split(tenantsched.RenderTrace(s.Trace()), "\n") {
		if strings.Contains(line, "TRANSFER[b->a x1]") {
			rendered = true
		}
	}
	if !rendered {
		t.Fatalf("rendered trace missing TRANSFER line:\n%s", tenantsched.RenderTrace(s.Trace()))
	}
}

// TestTransferReplayAudit 构造跨周期、含迁让双方的脚本，再交给独立的
// 逐 tick 轨迹重放审计（auditReplay）：扣减账目、按有效额度的阻挡判定、
// 捐赠方未用额度上界、周期归零、超期剩余量、修订号公式全部重放核对。
func TestTransferReplayAudit(t *testing.T) {
	groups := []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 3},
	}
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}
	must := func(r tenantsched.CommitResult, e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	rev := s.Revision()

	must(s.Submit(tenantsched.JobSpec{ID: "jA", ReleaseAt: 0, Work: 5, Deadline: 4, Group: "a"}, rev))
	rev = s.Revision()
	// b 把本周期全部 3 个未用额度让给 a：a 有效额度 5，jA 恰好按期完成。
	must(s.TransferQuota("b", "a", 3, rev))
	rev = s.Revision()

	r1, err := s.Advance(5, rev) // tick0..4：tick0 入账 SUBMIT+TRANSFER 并执行
	if err != nil {
		t.Fatal(err)
	}
	if r1.Ticks[0].Applied[1].Kind != tenantsched.ChangeTransfer {
		t.Fatalf("tick0 applied = %+v", r1.Ticks[0].Applied)
	}
	for _, e := range r1.Ticks {
		if e.Kind != tenantsched.TickRan || e.JobID != "jA" {
			t.Fatalf("tick %d = %+v, want RAN jA", e.Tick, e)
		}
	}
	rev = r1.Revision

	must(s.Submit(tenantsched.JobSpec{ID: "jB", ReleaseAt: 5, Work: 3, Deadline: 8, Group: "b"}, rev))
	rev = s.Revision()
	r2, err := s.Advance(4, rev) // tick5..8：b 本周期有效额度 0，持续阻挡
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range r2.Ticks {
		if e.Kind != tenantsched.TickIdleBlocked || e.BlockedBy != "b" {
			t.Fatalf("tick %d = %+v, want blocked by donor b (effective 0)", e.Tick, e)
		}
	}
	if ev := r2.Overdue; len(ev) != 1 || ev[0].JobID != "jB" || ev[0].Remaining != 3 || ev[0].Blocker != "b" {
		t.Fatalf("jB overdue = %+v", ev)
	}
	rev = r2.Revision

	if _, err := s.Advance(1, rev); err != nil { // tick9 仍被 b 挡
		t.Fatal(err)
	}
	rev = s.Revision()

	// now==10 周期边界的临界提交：上一周期 a 有效额度 5 全部用完、未用为 0，
	// 新周期恢复基础 2，a 让出 1 给 b；误用旧余额的实现会拒绝或留下旧账目。
	must(s.TransferQuota("a", "b", 1, rev))
	rev = s.Revision()
	r3, err := s.Advance(4, rev) // tick10 入账转让并执行；11/12 完成；13 空转
	if err != nil {
		t.Fatal(err)
	}
	if e0 := r3.Ticks[0]; e0.Tick != 10 || e0.Period != 1 || e0.JobID != "jB" ||
		len(e0.Applied) != 1 || e0.Applied[0].Kind != tenantsched.ChangeTransfer ||
		e0.Applied[0].FromGroup != "a" || e0.Applied[0].Group != "b" {
		t.Fatalf("tick10 = %+v", e0)
	}
	if r3.Ticks[3].Kind != tenantsched.TickIdleNotReady {
		t.Fatalf("tick13 = %+v, want idle after jB completion", r3.Ticks[3])
	}

	qs := quotaMap(s.Snapshot())
	if qa, qb, qR := qs["a"], qs["b"], qs["R"]; qa.Used != 0 || qa.EffectiveQuota != 1 ||
		qb.Used != 3 || qb.EffectiveQuota != 4 ||
		qR.Used != 3 || qR.EffectiveQuota != 100 {
		t.Fatalf("final ledgers: a=%+v b=%+v R=%+v", qa, qb, qR)
	}

	auditReplay(t, s, groups)
}

// TestUnchangedBehaviorWithoutTransfers：没有任何转让时，快照的有效额度
// 恒等于基础额度，整个轨迹不出现 TRANSFER。
func TestUnchangedBehaviorWithoutTransfers(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "j", ReleaseAt: 0, Work: 2, Deadline: 5, Group: "R",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(12, s.Revision()); err != nil { // 跨过一个周期边界
		t.Fatal(err)
	}
	for _, q := range s.Snapshot().Quotas {
		if q.EffectiveQuota != q.Quota {
			t.Fatalf("effective %d != base %d without transfers", q.EffectiveQuota, q.Quota)
		}
	}
	for i, e := range s.Trace() {
		for _, a := range e.Applied {
			if a.Kind == tenantsched.ChangeTransfer {
				t.Fatalf("tick %d unexpected transfer: %+v", i, a)
			}
		}
	}
	if got := tenantsched.RenderTrace(s.Trace()[:1]); !strings.Contains(got, "tick=0") {
		t.Fatalf("render changed shape: %q", got)
	}
}
