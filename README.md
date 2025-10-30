# 🧠 Quant & HFT Trading Bot

> A modular, high-performance framework for quantitative and high-frequency trading — built for research, backtesting, and live execution.

---

## 📈 Overview

This project provides a full **quantitative trading infrastructure**, including:

- **Data ingestion & preprocessing** (tick, bar, and fundamental data)
- **Alpha research** with feature engineering and signal generation
- **Backtesting engine** with realistic market simulation
- **Execution layer** for low-latency trading
- **Monitoring & logging** for live strategies

The goal is to create a scalable, testable, and extensible foundation for both **research** and **live HFT systems**.

---

## ⚙️ Installation

```bash
git clone https://github.com/alexkobz/invest_bot.git
cd invest_bot

# Create environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

(Optional, using Docker)

docker-compose up -d -f docker/docker-compose-airflow.yml
```

⸻

🧪 Example Usage

### TODO

⸻

📊 Strategies

### TODO


⸻

🧰 Tech Stack

Layer	Technology
Data	PostgreSQL, Parquet, Redis
Compute	Python, Pandas, NumPy
Streaming	Kafka, WebSocket
Backtesting	Vectorized (NumPy) + event-driven (todo)
Execution	Tinkoff API
Monitoring	Prometheus, Grafana, ELK (todo)


⸻

🧑‍💻 Authors

Alexander Kobzar

📧 alexanderkobzarrr@gmail.com

⸻

⚠️ Disclaimer

This project is for educational and research purposes only.
It does not constitute financial advice or an offer to trade any financial instrument.
Use at your own risk.
