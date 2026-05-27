# 大模型 API 压测客户端部署记录

## 环境
- **设备**: MacBook Air (Apple M4, 24GB)
- **系统**: macOS 26.5
- **工具**: k6 v2.0.0 (darwin/arm64)
- **脚本**: llm_stress_test.js (68行)

## 压测阶梯
| 阶段 | 并发数 | 持续时间 |
|------|--------|----------|
| 爬坡1 | 10 VU | 30s |
| 稳态1 | 10 VU | 60s |
| 爬坡2 | 20 VU | 15s |
| 稳态2 | 20 VU | 60s |
| 爬坡3 | 50 VU | 30s |
| 稳态3 | 50 VU | 90s |
| 爬坡4 | 100 VU | 60s |
| **验收窗口** | **100 VU** | **600s (10min)** |
| 爬坡5 | 200 VU | 60s |
| **尖峰冲击** | **200 VU** | **180s (3min)** |
| 冷却 | 0 VU | 60s |

## 熔断规则
- HTTP 错误率 > 5% → 自动中止 (`abortOnFail`)

## 启动命令
```bash
ulimit -n 65535
k6 run \
  -e API_URL="https://你的端点/v1/chat/completions" \
  -e API_KEY="sk-你的密钥" \
  -e MODEL_NAME="gpt-4o" \
  llm_stress_test.js
```
