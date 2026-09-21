#!/usr/bin/env bash
#
# test.sh — 手动测试脚本
#
# 覆盖本次改动的三个单元:
#   1. campaign 级别独立滑动窗口限流 (internal/manager/pipe.go, manager.go)
#   2. 限流检查与消息入队的原子化 (pipe.go slidingWindow.reserve)
#   3. UpdateCampaignStatus 支持暂停原因 (cmd/campaigns.go, core, queries, 前端展示)
#
# 前置条件:
#   - PostgreSQL 已启动, 且已用新版 schema.sql 初始化 (或执行过 --upgrade 迁移)
#   - config.toml 已配置, 且开启了滑动窗口限流, 例如:
#       [app]
#       ...
#     并在 Settings -> Performance 中:
#       Sliding window = on, Window duration = 60s, Window rate = 10
#   - listmonk 已编译并以 ./listmonk 运行
#
# 用法: bash test.sh
# 说明: 本脚本是"手动"测试向导, 每个步骤会打印操作与预期结果,
#       需要人工核对日志 / 数据库 / 前端页面。

set -u

BASE_URL="${BASE_URL:-http://localhost:9000}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-admin}"
DB="${DB:-listmonk}"
DBUSER="${DBUSER:-listmonk}"

pass() { echo "  [请确认] $1"; }
step() { echo; echo "==== $1 ===="; }

SQL() { psql -U "$DBUSER" -d "$DB" -c "$1"; }

echo "=============================================="
echo " listmonk 手动测试: 独立滑动窗口限流 + 暂停原因"
echo "=============================================="

# ----------------------------------------------------------------
step "0. 编译与静态检查"
echo "go build ./..."
go build ./... && echo "  编译通过" || { echo "  编译失败, 终止"; exit 1; }
echo "gofmt 检查:"
gofmt -l internal/manager/ cmd/ internal/core/ models/ internal/migrations/
pass "gofmt 无输出"

# ----------------------------------------------------------------
step "1. 数据库迁移: campaigns.status_reason 字段"
SQL "SELECT column_name, data_type FROM information_schema.columns
     WHERE table_name='campaigns' AND column_name='status_reason';"
pass "能查到 status_reason (text) 列; 若是老库, 先运行 ./listmonk --upgrade"

# ----------------------------------------------------------------
step "2. 单元一: 每个 campaign 独立的滑动窗口"
echo "操作:"
echo "  1) 创建两个列表 A、B, 各导入 >= 50 个订阅者"
echo "  2) 创建 campaign A 和 campaign B (不同列表), 同时启动 (status=running)"
echo "  3) 观察日志: tail -f 运行终端"
echo "预期:"
echo "  - 日志中限流提示带有各自的活动名:"
echo "      campaign (A) exceeded 10 messages for the window (1m0s). Sleeping for ..."
echo "  - A 触发窗口等待时, B 仍在继续发送 (两个窗口互不影响)"
echo "  - 改动前: A 的计数会挤占全局窗口, 导致 B 也被限流"
SQL "SELECT id, name, status, sent, to_send FROM campaigns ORDER BY id DESC LIMIT 5;"
pass "A、B 两个活动的 sent 各自按自己的窗口节奏增长"

# ----------------------------------------------------------------
step "3. 单元二: 窗口计数的原子性 (并发安全)"
echo "操作:"
echo "  1) config.toml 中将 concurrency 调大 (如 10), message_rate 调高"
echo "  2) 滑动窗口设为: 60s 内最多 10 条"
echo "  3) 启动一个订阅者较多的 campaign, 观察 60s 内的实际入队数"
echo "预期:"
echo "  - 任意 60s 窗口内, 该 campaign 的入队消息数严格 <= 窗口上限"
echo "  - 改动前: 检查与入队串行但未加锁, 并发下计数会超发/漏发"
echo "  - 可用日志时间戳统计: 相邻两条 'exceeded ... Sleeping' 之间的发送数"
pass "窗口内发送数不超过 SlidingWindowRate"

# ----------------------------------------------------------------
step "4. 单元三a: API 接受并保存暂停原因"
echo "先创建一个 running 状态的 campaign, 记录其 ID (设为 CAMP_ID):"
read -rp "  输入运行中的 campaign ID: " CAMP_ID
echo
echo ">> 带 reason 暂停:"
curl -s -u "$ADMIN_USER:$ADMIN_PASS" -X PUT "$BASE_URL/api/campaigns/$CAMP_ID/status" \
  -H 'Content-Type: application/json' \
  -d '{"status": "paused", "reason": "manual test pause"}' | tee /tmp/resp.json
echo
SQL "SELECT id, status, status_reason FROM campaigns WHERE id=$CAMP_ID;"
pass "status=paused 且 status_reason='manual test pause'"

echo
echo ">> 恢复运行后 reason 应被清空:"
curl -s -u "$ADMIN_USER:$ADMIN_PASS" -X PUT "$BASE_URL/api/campaigns/$CAMP_ID/status" \
  -H 'Content-Type: application/json' \
  -d '{"status": "running"}' > /dev/null
SQL "SELECT id, status, status_reason FROM campaigns WHERE id=$CAMP_ID;"
pass "status=running 且 status_reason 为 NULL"

echo
echo ">> 超长 reason 应被 400 拒绝:"
LONG=$(head -c 2100 < /dev/zero | tr '\0' 'x')
curl -s -o /dev/null -w "  HTTP %{http_code}\n" -u "$ADMIN_USER:$ADMIN_PASS" \
  -X PUT "$BASE_URL/api/campaigns/$CAMP_ID/status" \
  -H 'Content-Type: application/json' \
  -d "{\"status\": \"paused\", \"reason\": \"$LONG\"}"
pass "返回 400 (invalid fields: reason)"
# 恢复状态, 便于后续步骤
curl -s -u "$ADMIN_USER:$ADMIN_PASS" -X PUT "$BASE_URL/api/campaigns/$CAMP_ID/status" \
  -H 'Content-Type: application/json' -d '{"status": "running"}' > /dev/null

# ----------------------------------------------------------------
step "5. 单元三b: OnError 自动暂停记录原因"
echo "操作:"
echo "  1) 将 SMTP 配置改成一个必然失败的地址 (如 127.0.0.1:1), max_send_errors 保持默认"
echo "  2) 启动一个 campaign 让它连续发送失败"
echo "  3) 观察日志出现: 'error count exceeded ... pausing campaign ...'"
read -rp "  输入被自动暂停的 campaign ID: " ERR_CAMP_ID
SQL "SELECT id, status, status_reason FROM campaigns WHERE id=$ERR_CAMP_ID;"
pass "status=paused 且 status_reason='too many errors'"
pass "管理员收到暂停通知邮件/通知, 内容包含原因"

# ----------------------------------------------------------------
step "6. 单元三c: 前端展示暂停原因"
echo "操作:"
echo "  1) 打开 活动列表页 ($BASE_URL/admin/campaigns)"
echo "  2) 找到上一步被暂停的 campaign"
echo "预期:"
echo "  - 状态列 'Paused' 标签下方显示灰色原因文本 (带信息图标)"
echo "  - 点击进入活动详情页, 顶部状态标签旁同样显示原因"
echo "  - 恢复 running 后, 原因不再显示"
pass "列表页与详情页均展示 status_reason"

# ----------------------------------------------------------------
step "7. 回归: 状态机校验仍然生效"
echo ">> 对 draft 状态的 campaign 直接 pause 应失败:"
read -rp "  输入一个 draft 状态的 campaign ID: " DRAFT_ID
curl -s -o /dev/null -w "  HTTP %{http_code}\n" -u "$ADMIN_USER:$ADMIN_PASS" \
  -X PUT "$BASE_URL/api/campaigns/$DRAFT_ID/status" \
  -H 'Content-Type: application/json' -d '{"status": "paused"}'
pass "返回 400 (only running campaigns can be paused)"

echo
echo "=============================================="
echo " 全部手动步骤执行完毕, 请逐项确认 [请确认] 标记"
echo "=============================================="
