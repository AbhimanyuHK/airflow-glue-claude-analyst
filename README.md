# Airflow Glue Claude Analyst 🤖

An AI-powered Slack bot that automatically diagnoses **Apache Airflow** and **AWS Glue** pipeline failures.

When a job fails and the error notification lands in Slack, an engineer simply replies `@bot analyze` in the thread. The bot fetches the relevant CloudWatch logs, sends them to **Claude AI**, and replies in the same thread with a structured root cause analysis, recommended fix, and the key error snippet — in under 30 seconds.

---

## How it works

```
Airflow DAG / Glue job fails
         │
         ▼
Error notification posted to Slack channel
         │
         ▼
Engineer replies: @yourbot analyze
         │
         ▼
Bot parses job name + run ID from original message
         │
         ▼
Fetches matching CloudWatch log stream (AWS)
         │
         ▼
Logs sent to Claude AI → structured JSON analysis
         │
         ▼
Rich Block Kit reply posted in the same Slack thread
  • Root cause  • Summary  • Fix steps
  • Affected components  • Key error snippet
```

---

## Features

- **Zero tab-switching** — everything happens inside the Slack thread
- **Supports Airflow (MWAA & self-hosted) and AWS Glue** out of the box
- **Smart log stream discovery** — finds the right stream even when the run ID is partial or missing
- **Intelligent truncation** — keeps the most diagnostic parts of large log files within Claude's context window
- **Retry with backoff** on AWS throttling
- **Structured Block Kit output** — severity badge, category label, fix steps
- **Socket Mode** — no public URL or reverse proxy required

---

## Project structure

```
airflow-glue-claude-analyst/
├── main.py                      # Entry point
├── src/
│   ├── bot.py                   # Slack event handlers, message parser, Block Kit formatter
│   ├── aws_logs.py              # CloudWatch log fetcher (Airflow + Glue)
│   └── claude_analyzer.py       # Claude AI root cause analysis
├── tests/
│   ├── test_bot.py              # Parser + formatter unit tests
│   ├── test_aws_logs.py         # Log fetcher unit tests (boto3 mocked)
│   └── test_claude_analyzer.py  # Analyzer unit tests (API mocked)
├── .github/workflows/ci.yml     # GitHub Actions CI
├── .env.example                 # Environment variable template
├── requirements.txt
├── requirements-dev.txt
├── Dockerfile
└── docker-compose.yml
```

---

## Quick start

### 1. Create a Slack App

Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** → **From scratch**.

**OAuth & Permissions → Bot Token Scopes:**

| Scope | Purpose |
|---|---|
| `app_mentions:read` | Detect `@bot analyze` mentions |
| `channels:history` | Read the original error message |
| `chat:write` | Post analysis replies |
| `groups:history` | Support private channels |

Install the app and copy the **Bot User OAuth Token** (`xoxb-...`).

**Basic Information → App-Level Tokens** — create a token with scope `connections:write`.  
Copy the **App-Level Token** (`xapp-...`).

**Socket Mode → Enable Socket Mode.**

**Event Subscriptions → Subscribe to bot events:** `app_mention`

---

### 2. IAM permissions

Attach this policy to the IAM user or role running the bot:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogStreams",
        "logs:GetLogEvents",
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:*:*:log-group:/aws/mwaa/*:*",
        "arn:aws:logs:*:*:log-group:/aws-glue/jobs/*:*"
      ]
    }
  ]
}
```

When running on EC2 or ECS, attach the policy to the instance/task IAM role — no access keys needed.

---

### 3. Configure

```bash
cp .env.example .env
# Open .env and fill in your values
```

Key variables:

| Variable | Required | Description |
|---|---|---|
| `SLACK_BOT_TOKEN` | ✅ | `xoxb-...` bot token |
| `SLACK_APP_TOKEN` | ✅ | `xapp-...` app-level token |
| `ANTHROPIC_API_KEY` | ✅ | Claude API key |
| `AWS_REGION` | ✅ | e.g. `us-east-1` |
| `AIRFLOW_LOG_GROUP` | ✅ | e.g. `/aws/mwaa/my-env` |
| `LOG_LOOKBACK_HOURS` | — | Default `2` |
| `MAX_LOG_CHARS` | — | Default `20000` |

---

### 4. Run

**Local:**
```bash
pip install -r requirements.txt
python main.py
```

**Docker:**
```bash
docker build -t airflow-glue-claude-analyst .
docker run --env-file .env airflow-glue-claude-analyst
```

**Docker Compose:**
```bash
docker-compose up -d
```

---

## Setting up error notifications

The bot parses alert messages automatically. Your Airflow/Glue notifications should include the job name and run ID.

### Airflow — failure callback

```python
import os, requests
from airflow import DAG

def slack_failure_callback(context):
    requests.post(os.environ["SLACK_WEBHOOK_URL"], json={
        "text": (
            f"🚨 *Airflow Task Failed*\n"
            f"DAG: `{context['dag'].dag_id}`\n"
            f"Task: `{context['task_instance'].task_id}`\n"
            f"Run ID: `{context['run_id']}`\n"
            f"Environment: production"
        )
    })

with DAG(
    "my_pipeline",
    on_failure_callback=slack_failure_callback,
    ...
):
    ...
```

### Glue — EventBridge rule

Create an EventBridge rule that matches Glue job state changes and sends them to Slack via SNS or a Lambda webhook:

```json
{
  "source": ["aws.glue"],
  "detail-type": ["Glue Job State Change"],
  "detail": {
    "state": ["FAILED", "ERROR", "TIMEOUT"]
  }
}
```

The message should include at minimum the job name and run ID (`jr_...`).

---

## Running tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v --cov=src
```

All AWS and Anthropic API calls are mocked — no real credentials needed to run the test suite.

---

## Deployment options

| Option | Best for |
|---|---|
| Docker on EC2 | Simple, attach IAM role to instance |
| ECS Fargate | Managed container, IAM task role |
| AWS Lambda (HTTP mode) | Serverless — swap Socket Mode for HTTP Events API |

For Lambda: replace `SocketModeHandler` with `SlackRequestHandler` from `slack_bolt.adapter.aws_lambda`.

---

## Configuration reference

All configuration is via environment variables (see `.env.example`).

| Variable | Default | Description |
|---|---|---|
| `SLACK_BOT_TOKEN` | — | Slack bot token (`xoxb-...`) |
| `SLACK_APP_TOKEN` | — | Slack app token (`xapp-...`) |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |
| `CLAUDE_MODEL` | `claude-sonnet-4-6` | Claude model to use |
| `CLAUDE_MAX_TOKENS` | `1500` | Max tokens for analysis |
| `AWS_REGION` | `us-east-1` | AWS region |
| `AIRFLOW_LOG_GROUP` | `/aws/mwaa/airflow` | MWAA CloudWatch log group |
| `GLUE_ERROR_LOG_GROUP` | `/aws-glue/jobs/error` | Glue error log group |
| `GLUE_OUTPUT_LOG_GROUP` | `/aws-glue/jobs/output` | Glue output log group |
| `LOG_LOOKBACK_HOURS` | `2` | How far back to fetch logs |
| `MAX_LOG_CHARS` | `20000` | Max log chars sent to Claude |

---

## License

MIT
