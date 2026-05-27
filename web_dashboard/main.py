"""
大模型 API 压测 Web 控制台 - 后端
支持自定义 stages、max_tokens、自动报告
启动: uvicorn main:app --reload --port 8080
"""

import asyncio
import json
import time
import os
import tempfile
import statistics
from collections import deque
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn

# ============================================================================
# FastAPI 应用
# ============================================================================
app = FastAPI(title="LLM Stress Test Dashboard", version="3.0.0")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORT_DIR, exist_ok=True)


# ============================================================================
# 动态 k6 脚本生成（含自定义指标采集）
# ============================================================================
def generate_k6_script(stages: list, max_tokens: int, fail_threshold: float = 0.05) -> str:
    stages_json = json.dumps(stages)
    return f"""import http from 'k6/http';
import {{ check, sleep }} from 'k6';
import {{ Trend, Counter }} from 'k6/metrics';

// ===== 自定义指标 =====
const tokenUsage = new Trend('token_usage');    // 每次请求消耗的总 token 数
const ttfb       = new Trend('ttfb');            // Time To First Byte ≈ TTFT
const http429    = new Counter('http_429');      // 429 Too Many Requests 计数
const http200    = new Counter('http_200');      // 200 成功计数

export const options = {{
    stages: {stages_json},
    thresholds: {{
        http_req_failed: [{{ threshold: 'rate<={fail_threshold}', abortOnFail: true }}],
    }},
}};

const API_URL    = __ENV.API_URL;
const API_KEY    = __ENV.API_KEY;
const MODEL_NAME = __ENV.MODEL_NAME;
const MAX_TOKENS = {max_tokens};

export default function () {{
    const payload = JSON.stringify({{
        model: MODEL_NAME,
        messages: [
            {{ role: 'user', content: '请生成一段约{max_tokens}字的技术性测试文本，用于评估系统吞吐性能。' }}
        ],
        max_tokens: MAX_TOKENS,
        temperature: 0.7,
        stream: false,
    }});

    const params = {{
        headers: {{
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${{API_KEY}}`,
        }},
        timeout: '30s',
    }};

    const res = http.post(API_URL, payload, params);

    // ===== 采集 TTFB (Time To First Byte) =====
    ttfb.add(res.timings.waiting);

    // ===== 采集 token 用量 & 状态码 =====
    if (res.status === 200) {{
        http200.add(1);
        try {{
            const body = res.json();
            if (body.usage && body.usage.total_tokens) {{
                tokenUsage.add(body.usage.total_tokens);
            }}
        }} catch (e) {{}}
    }} else if (res.status === 429) {{
        http429.add(1);
    }}

    check(res, {{
        'HTTP status is 200': (r) => r.status === 200,
    }});

    sleep(Math.random() * 1 + 1);
}}
"""


# ============================================================================
# 百分位数计算工具
# ============================================================================
def percentile(data: list, p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = int(len(s) * p / 100)
    return s[min(idx, len(s) - 1)]


# ============================================================================
# 压测管理器
# ============================================================================
class StressTestManager:
    def __init__(self):
        self.process: Optional[asyncio.subprocess.Process] = None
        self.running = False
        self.start_time: Optional[float] = None
        self._tmp_script: Optional[str] = None
        self._tmp_metrics: Optional[str] = None
        self._report_path: Optional[str] = None

        # 实时推送指标
        self.metrics = {
            "vus": 0, "http_req_failed_rate": 0.0,
            "http_req_duration_avg": 0.0, "http_req_duration_max": 0.0,
            "http_req_duration_p95": 0.0, "http_reqs_rate": 0.0,
            "total_requests": 0, "total_failures": 0, "elapsed_seconds": 0,
        }

        self.websockets: set[WebSocket] = set()

        # 通用延迟样本
        self.durations = deque(maxlen=2000)
        self._failed_count = 0
        self._success_count = 0

        # ===== 报告专用数据（全量持久） =====
        self._ttfb_all: list[float] = []       # 所有 TTFB 值
        self._token_all: list[int] = []        # 所有 token 值
        self._count_429 = 0
        self._count_200 = 0
        self._vus_samples: list[tuple[float, int]] = []  # (timestamp, vus)

        # 后台任务
        self._metrics_file_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._push_task: Optional[asyncio.Task] = None
        self._exit_watch_task: Optional[asyncio.Task] = None

    async def start(self, api_url: str, api_key: str, model_name: str,
                    stages: list, max_tokens: int) -> None:
        # 生成临时脚本
        script_content = generate_k6_script(stages, max_tokens)
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='_k6_test.js',
                                          delete=False, dir=BASE_DIR)
        tmp.write(script_content)
        tmp.close()
        self._tmp_script = tmp.name

        # 临时指标文件
        mt = tempfile.NamedTemporaryFile(mode='w', suffix='_k6_metrics.json',
                                         delete=False, dir=BASE_DIR)
        mt.close()
        self._tmp_metrics = mt.name

        cmd = [
            "k6", "run",
            "-e", f"API_URL={api_url}",
            "-e", f"API_KEY={api_key}",
            "-e", f"MODEL_NAME={model_name}",
            "--out", f"json={self._tmp_metrics}",
            self._tmp_script,
        ]

        await self._broadcast({"type": "log",
                               "data": f"📄 动态脚本: {os.path.basename(self._tmp_script)}"})
        await self._broadcast({"type": "log", "data": f"📋 Stages: {json.dumps(stages)}"})

        self.process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        self.running = True
        self.start_time = time.time()

        self._metrics_file_task = asyncio.create_task(self._read_metrics_file())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._push_task = asyncio.create_task(self._push_metrics_loop())
        self._exit_watch_task = asyncio.create_task(self._watch_exit())

    async def _watch_exit(self) -> None:
        """监听 k6 进程退出，自动生成报告"""
        if not self.process:
            return
        await self.process.wait()
        self.running = False
        await asyncio.sleep(1)  # 等最后一点数据写入
        await self._broadcast({"type": "log", "data": "⏹ k6 进程已退出，正在生成报告..."})
        await self._generate_report()
        await self._cleanup()

    async def stop(self) -> None:
        self.running = False
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
        await self._cleanup()

    async def _cleanup(self) -> None:
        for task in [self._metrics_file_task, self._stderr_task,
                     self._push_task, self._exit_watch_task]:
            if task and not task.done():
                task.cancel()
        for tmp_path in [self._tmp_script, self._tmp_metrics]:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
        self._tmp_script = None
        self._tmp_metrics = None

    async def _read_metrics_file(self) -> None:
        path = self._tmp_metrics
        if not path:
            return
        last_pos = 0
        while self.running and not os.path.exists(path):
            await asyncio.sleep(0.2)
        while self.running or (path and os.path.exists(path)):
            try:
                with open(path, 'r') as f:
                    f.seek(last_pos)
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            self._process_metric(data)
                        except json.JSONDecodeError:
                            pass
                    last_pos = f.tell()
            except (FileNotFoundError, PermissionError):
                pass
            await asyncio.sleep(0.5)

    async def _read_stderr(self) -> None:
        while self.running and self.process and self.process.stderr:
            line = await self.process.stderr.readline()
            if not line:
                break
            text = line.decode().strip()
            if text:
                await self._broadcast({"type": "log", "data": text})

    def _process_metric(self, data: dict) -> None:
        metric = data.get("metric")
        value = data.get("data", {}).get("value")

        if metric == "vus":
            self.metrics["vus"] = int(value)
            if self.start_time:
                self._vus_samples.append((time.time(), int(value)))
        elif metric == "http_req_duration":
            self.durations.append(value)
        elif metric == "http_req_failed":
            if value == 1:
                self._failed_count += 1
            else:
                self._success_count += 1
        elif metric == "ttfb":
            self._ttfb_all.append(value)
        elif metric == "token_usage":
            self._token_all.append(int(value))
        elif metric == "http_429":
            self._count_429 += int(value)
        elif metric == "http_200":
            self._count_200 += int(value)

    async def _push_metrics_loop(self) -> None:
        while self.running:
            await asyncio.sleep(1)
            total = self._failed_count + self._success_count
            self.metrics["total_requests"] = total
            self.metrics["total_failures"] = self._failed_count
            self.metrics["http_req_failed_rate"] = (self._failed_count / total * 100) if total > 0 else 0.0

            d = list(self.durations)
            if d:
                self.metrics["http_req_duration_avg"] = sum(d) / len(d)
                self.metrics["http_req_duration_max"] = max(d)
                self.metrics["http_req_duration_p95"] = percentile(d, 95)

            elapsed = time.time() - self.start_time if self.start_time else 1
            self.metrics["elapsed_seconds"] = int(elapsed)
            self.metrics["http_reqs_rate"] = total / elapsed if elapsed > 0 else 0.0
            await self._broadcast({"type": "metrics", "data": dict(self.metrics)})

    async def _generate_report(self) -> None:
        total = self._failed_count + self._success_count
        elapsed = time.time() - self.start_time if self.start_time else 1
        elapsed_min = elapsed / 60

        # 计算各项指标
        rpm = total / elapsed_min if elapsed_min > 0 else 0
        total_tokens = sum(self._token_all)
        tpm = total_tokens / elapsed_min if elapsed_min > 0 else 0
        tok_per_sec = total_tokens / elapsed if elapsed > 0 else 0

        ttft_p50 = percentile(self._ttfb_all, 50)
        ttft_p95 = percentile(self._ttfb_all, 95)
        latency_p50 = percentile(list(self.durations), 50) if self.durations else 0

        # 平均并发（按时间加权）
        avg_vus = 0
        if self._vus_samples:
            weights = []
            for i in range(1, len(self._vus_samples)):
                dt = self._vus_samples[i][0] - self._vus_samples[i - 1][0]
                weights.append(self._vus_samples[i - 1][1] * dt)
            avg_vus = sum(weights) / (self._vus_samples[-1][0] - self._vus_samples[0][0]) if len(self._vus_samples) > 1 else self._vus_samples[-1][1]

        max_vus = max((v for _, v in self._vus_samples), default=0)

        success_rate = (self._count_200 / total * 100) if total > 0 else 0
        rate_429 = (self._count_429 / total * 100) if total > 0 else 0

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        report = f"""# LLM API 压测报告

**生成时间**: {now_str}
**测试时长**: {elapsed:.0f}s ({elapsed_min:.1f}min)

## 总体概览

| 指标 | 值 |
|---|---|
| 总请求数 | {total} |
| 成功请求 (HTTP 200) | {self._count_200} |
| 失败请求 | {self._failed_count} |
| 429 (限流) | {self._count_429} |
| 平均并发 (VU) | {avg_vus:.1f} |
| 最大并发 (VU) | {max_vus} |
| 消耗总 Token | {total_tokens:,} |

## 核心指标

| 并发 | RPM | TPM | TTFT P50 | TTFT P95 | 延迟 P50 | 输出 tok/s | 成功率 | 429率 |
|---|---|---|---|---|---|---|---|---|
| {avg_vus:.0f} | {rpm:.0f} | {tpm:.0f} | {ttft_p50*1000:.0f}ms | {ttft_p95*1000:.0f}ms | {latency_p50*1000:.0f}ms | {tok_per_sec:.1f} | {success_rate:.1f}% | {rate_429:.1f}% |
"""
        # 保存报告
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._report_path = os.path.join(REPORT_DIR, f"report_{ts}.md")
        with open(self._report_path, 'w', encoding='utf-8') as f:
            f.write(report)

        await self._broadcast({"type": "log", "data": f"📄 报告已保存: {self._report_path}"})

        # 推送报告内容到前端
        await self._broadcast({
            "type": "report",
            "data": {
                "path": f"/api/report/download?ts={ts}",
                "content": report,
                "filename": f"report_{ts}.md",
            }
        })

    async def _broadcast(self, message: dict) -> None:
        stale = set()
        for ws in self.websockets:
            try:
                await ws.send_json(message)
            except Exception:
                stale.add(ws)
        self.websockets -= stale

    def reset(self) -> None:
        self.metrics = {k: (0 if isinstance(v, (int, float)) else v) for k, v in self.metrics.items()}
        self.durations.clear()
        self._failed_count = 0
        self._success_count = 0
        self._ttfb_all.clear()
        self._token_all.clear()
        self._count_429 = 0
        self._count_200 = 0
        self._vus_samples.clear()
        self._report_path = None


manager = StressTestManager()


# ============================================================================
# Pydantic 模型
# ============================================================================
class StageItem(BaseModel):
    duration: str
    target: int

class StartRequest(BaseModel):
    api_url: str
    api_key: str
    model_name: str = "gpt-4o"
    max_tokens: int = 500
    stages: list[StageItem] = []


# ============================================================================
# 路由
# ============================================================================
@app.get("/")
async def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    manager.websockets.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.websockets.discard(ws)

@app.post("/api/start")
async def start_test(req: StartRequest):
    if manager.running:
        return {"status": "error", "message": "测试已在进行中，请先停止"}
    if not req.stages:
        return {"status": "error", "message": "请至少添加一个压力阶段"}
    manager.reset()
    asyncio.create_task(manager.start(
        api_url=req.api_url, api_key=req.api_key,
        model_name=req.model_name,
        stages=[s.model_dump() for s in req.stages],
        max_tokens=req.max_tokens,
    ))
    return {"status": "started", "message": "压测已启动"}

@app.post("/api/stop")
async def stop_test():
    if not manager.running:
        return {"status": "error", "message": "当前没有运行中的测试"}
    await manager.stop()
    return {"status": "stopped", "message": "压测已停止"}

@app.get("/api/status")
async def get_status():
    return {"running": manager.running, "metrics": manager.metrics}

@app.get("/api/report/download")
async def download_report(ts: str = ""):
    """下载最近一次生成的报告"""
    if manager._report_path and os.path.exists(manager._report_path):
        return FileResponse(manager._report_path, media_type="text/markdown",
                            filename=os.path.basename(manager._report_path))
    # 按时间戳查找
    if ts:
        path = os.path.join(REPORT_DIR, f"report_{ts}.md")
        if os.path.exists(path):
            return FileResponse(path, media_type="text/markdown",
                                filename=f"report_{ts}.md")
    return {"status": "error", "message": "报告不存在"}

@app.get("/api/report/list")
async def list_reports():
    """列出所有历史报告"""
    files = sorted([f for f in os.listdir(REPORT_DIR) if f.endswith('.md')], reverse=True)
    return {"reports": files[:20]}


# ============================================================================
# 入口
# ============================================================================
if __name__ == "__main__":
    print("🚀 LLM Stress Test Dashboard v3.0 - 自动报告")
    print(f"📁 工作目录: {BASE_DIR}")
    print(f"📁 报告目录: {REPORT_DIR}")
    print("🌐 http://127.0.0.1:8080")
    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")
