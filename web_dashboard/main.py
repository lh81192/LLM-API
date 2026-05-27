"""
大模型 API 压测 Web 控制台 - 后端
支持动态自定义 stages、max_tokens
启动: uvicorn main:app --reload --port 8080
"""

import asyncio
import json
import time
import os
import tempfile
from collections import deque
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn

# ============================================================================
# FastAPI 应用
# ============================================================================
app = FastAPI(title="LLM Stress Test Dashboard", version="2.0.0")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ============================================================================
# 动态 k6 脚本生成
# ============================================================================
def generate_k6_script(stages: list, max_tokens: int, fail_threshold: float = 0.05) -> str:
    """根据用户配置动态生成 k6 测试脚本"""
    stages_json = json.dumps(stages)

    return f"""import http from 'k6/http';
import {{ check, sleep }} from 'k6';

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

    check(res, {{
        'HTTP status is 200': (r) => r.status === 200,
    }});

    sleep(Math.random() * 1 + 1);
}}
"""


# ============================================================================
# 压测管理器
# ============================================================================
class StressTestManager:
    """管理 k6 子进程生命周期、解析指标、WebSocket 推送"""

    def __init__(self):
        self.process: Optional[asyncio.subprocess.Process] = None
        self.running = False
        self.start_time: Optional[float] = None
        self._tmp_script: Optional[str] = None  # 临时脚本路径

        # 当前聚合指标
        self.metrics = {
            "vus": 0,
            "http_req_failed_rate": 0.0,
            "http_req_duration_avg": 0.0,
            "http_req_duration_max": 0.0,
            "http_req_duration_p95": 0.0,
            "http_reqs_rate": 0.0,
            "total_requests": 0,
            "total_failures": 0,
            "elapsed_seconds": 0,
        }

        # 已连接的 WebSocket 客户端
        self.websockets: set[WebSocket] = set()

        # 原始数据存储
        self.durations = deque(maxlen=2000)
        self._failed_count = 0
        self._success_count = 0

        # 后台任务
        self._stdout_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._push_task: Optional[asyncio.Task] = None

    async def start(self, api_url: str, api_key: str, model_name: str,
                    stages: list, max_tokens: int) -> None:
        """动态生成脚本并启动 k6 子进程"""
        # 生成临时脚本
        script_content = generate_k6_script(stages, max_tokens)
        # 写入临时文件（保留后缀名便于识别）
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='_k6_test.js',
                                          delete=False, dir=BASE_DIR)
        tmp.write(script_content)
        tmp.close()
        self._tmp_script = tmp.name

        cmd = [
            "k6", "run",
            "-e", f"API_URL={api_url}",
            "-e", f"API_KEY={api_key}",
            "-e", f"MODEL_NAME={model_name}",
            "--out", "json=/dev/stdout",
            self._tmp_script,
        ]

        await self._broadcast({"type": "log", "data": f"📄 动态脚本已生成: {os.path.basename(self._tmp_script)}"})
        await self._broadcast({"type": "log", "data": f"📋 Stages: {json.dumps(stages)}"})

        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        self.running = True
        self.start_time = time.time()

        # 启动后台协程
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._push_task = asyncio.create_task(self._push_metrics_loop())

    async def stop(self) -> None:
        """停止压测并清理临时文件"""
        self.running = False
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()

        # 取消后台任务
        for task in [self._stdout_task, self._stderr_task, self._push_task]:
            if task and not task.done():
                task.cancel()

        # 清理临时脚本
        if self._tmp_script and os.path.exists(self._tmp_script):
            try:
                os.unlink(self._tmp_script)
                await self._broadcast({"type": "log", "data": "🧹 临时脚本已清理"})
            except OSError:
                pass
            self._tmp_script = None

    async def _read_stdout(self) -> None:
        """读取 k6 JSON 指标输出流"""
        while self.running and self.process and self.process.stdout:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode().strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                self._process_metric(data)
            except json.JSONDecodeError:
                pass

    async def _read_stderr(self) -> None:
        """读取 k6 控制台日志并广播到前端"""
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
        elif metric == "http_req_duration":
            self.durations.append(value)
        elif metric == "http_req_failed":
            if value == 1:
                self._failed_count += 1
            else:
                self._success_count += 1

    async def _push_metrics_loop(self) -> None:
        """每秒聚合指标并推送至前端"""
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
                sorted_d = sorted(d)
                p95_idx = int(len(sorted_d) * 0.95)
                self.metrics["http_req_duration_p95"] = sorted_d[min(p95_idx, len(sorted_d) - 1)]

            elapsed = time.time() - self.start_time if self.start_time else 1
            self.metrics["elapsed_seconds"] = int(elapsed)
            self.metrics["http_reqs_rate"] = total / elapsed if elapsed > 0 else 0.0

            await self._broadcast({"type": "metrics", "data": dict(self.metrics)})

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


manager = StressTestManager()


# ============================================================================
# Pydantic 模型
# ============================================================================

class StageItem(BaseModel):
    duration: str  # 例如 "30s", "2m"
    target: int    # VU 目标数

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
        api_url=req.api_url,
        api_key=req.api_key,
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


# ============================================================================
# 入口
# ============================================================================
if __name__ == "__main__":
    print("🚀 LLM Stress Test Dashboard v2.0")
    print(f"📁 工作目录: {BASE_DIR}")
    print("🌐 http://127.0.0.1:8080")
    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")
