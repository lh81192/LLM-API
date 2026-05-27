import http from 'k6/http';
import { check, sleep } from 'k6';

// ============================================================================
// 1. 压力阶段与熔断阈值配置 (Configuration)
// ============================================================================
export const options = {
    stages: [
        { duration: '30s',  target: 10  },  // 30秒内线性爬坡至 10 VU
        { duration: '60s',  target: 10  },  // 保持 10 VU 运行 1 分钟
        { duration: '15s',  target: 20  },  // 15秒内爬坡至 20 VU
        { duration: '60s',  target: 20  },  // 保持 20 VU 运行 1 分钟
        { duration: '30s',  target: 50  },  // 30秒内爬坡至 50 VU
        { duration: '90s',  target: 50  },  // 保持 50 VU 运行 1.5 分钟
        { duration: '60s',  target: 100 },  // 60秒内爬坡至 100 VU
        { duration: '600s', target: 100 },  // 【验收窗口】保持 100 VU 稳定运行 10 分钟
        { duration: '60s',  target: 200 },  // 60秒内突发飙升至 200 VU
        { duration: '180s', target: 200 },  // 【尖峰冲击】保持 200 VU 运行 3 分钟
        { duration: '60s',  target: 0   },  // 60秒冷却降温期，释放连接
    ],
    thresholds: {
        // 安全熔断核心保护：如果全链路 HTTP 请求失败率（如 429、5xx、超时等）大于 5%，立刻自动中止整个压测
        http_req_failed: [{ threshold: 'rate<=0.05', abortOnFail: true }],
    },
};

// ============================================================================
// 2. 环境变量与硬编码参数 (Global Variables)
// ============================================================================
const API_URL   = __ENV.API_URL    || 'https://api.your-cloud-gateway.com/v1/chat/completions';
const API_KEY   = __ENV.API_KEY    || 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
const MODEL_NAME = __ENV.MODEL_NAME || 'gpt-4o';

// ============================================================================
// 3. 虚拟用户核心执行逻辑 (VU Lifecycle)
// ============================================================================
export default function () {
    // 构造大模型标准 Chat Payload
    const payload = JSON.stringify({
        model: MODEL_NAME,
        messages: [
            { role: 'user', content: '请生成一段100字左右的技术性测试文本，用于评估系统吞吐性能。' }
        ],
        max_tokens: 500,
        temperature: 0.7,
        stream: false  // 压测默认关闭流式，专注于评估整包吞吐性能
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${API_KEY}`,
        },
        timeout: '30s',  // 设定单次 API 响应超时为 30 秒，超时直接记为失败
    };

    // 发起 POST 请求
    const res = http.post(API_URL, payload, params);

    // 断言验证：检查 HTTP 状态码是否为 200
    check(res, {
        'HTTP status is 200': (r) => r.status === 200,
    });

    // 思考时间（Pacing）：每个虚拟用户请求完后随机休眠 1~2 秒，模拟真实业务调用间隔
    // 避免单一 VU 线程陷入零延迟死循环轰炸导致本地端口耗尽
    sleep(Math.random() * 1 + 1);
}
