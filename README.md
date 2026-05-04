# 🎮 Distributed Gaming Analytics System

A scalable gaming analytics platform that leverages **distributed computing with Ray** to perform **parallel player analysis and prediction** in real time.

---

## 🚀 Features

* ⚡ Parallel player analysis using Ray
* 🤖 Machine Learning-based prediction (Random Forest)
* 📊 Statistical performance evaluation
* 🌐 Flask-based web interface
* 📈 Scalable multi-node architecture

---

## 🧠 System Architecture

| Component          | Description                   |
| ------------------ | ----------------------------- |
| Frontend           | HTML-based user interface     |
| Backend            | Flask server                  |
| Distributed Engine | Ray cluster                   |
| Workers            | Execute tasks in parallel     |
| Models             | Loaded locally within workers |

---

## ⚙️ Installation

### 1. Clone the Repository

```bash
git clone <your-repo-link>
cd gaming_analytics
```

### 2. Create Virtual Environment

```bash
python3 -m venv ray_env
source ray_env/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🧑‍💻 Worker Node Requirements

To ensure proper distributed execution, **each worker node must be correctly configured**:

### ✅ 1. Same Codebase

* All nodes must contain the same project directory
* Folder structure must be identical

### ✅ 2. Python Environment

```bash
python3 -m venv ray_env
source ray_env/bin/activate
```

* Recommended: Python 3.10+

### ✅ 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### ✅ 4. Model Files

Workers must have access to:

```
model/model_*.pkl
```

> Models are loaded locally inside workers (not transferred over network)

### ✅ 5. Dataset (Optional but Recommended)

```
data/cs2_dataset.csv
```

### ✅ 6. Network Connectivity

* All nodes must be on the same network
* Workers must reach master via IP and port

### ✅ 7. Connect to Ray Cluster

```bash
ray start --address='MASTER_IP:6379'
```

---

### ⚠️ Important Notes

* Disable firewall if blocking connection
* Use the same port as master
* Start workers **before running Flask app**

---

### 💡 Key Insight

Workers execute tasks independently and load models locally, reducing network overhead and improving scalability.

---

## 🧪 Running the Project (Distributed Setup)

### Step 1: Start Master Node

```bash
ray start --head --port=6379
```

---

### Step 2: Connect Worker Nodes

```bash
ray start --address='MASTER_IP:6379'
```

---

### Step 3: Run Application (Master Node)

```bash
python app.py
```

---

### Step 4: Open in Browser

```
http://MASTER_IP:5000
```

---

## ⚡ How It Works

1. User inputs player data
2. Flask converts each player into a task
3. Tasks are distributed using Ray
4. Workers execute tasks in parallel
5. Results are aggregated and returned

---

## 🧩 Project Structure

```
gaming_analytics/
│
├── app.py
├── ray_tasks.py
├── train_model.py
├── utils.py
│
├── model/
├── data/
├── templates/
│
└── README.md
```

---

## 🔥 Optimization Techniques

* Lazy initialization of shared data
* Minimal data transfer (only averages shared)
* Local model loading in workers
* Task-level parallelism

---

## 📌 Future Work

* Cloud deployment (multi-node scaling)
* Real-time streaming analytics
* Improved ML model performance

---

## 👥 Contributors

* **Member 1**

  * Machine Learning models
  * Statistical analysis
  * UI design

* **Member 2**

  * Ray cluster setup
  * Flask backend
  * Distributed task scheduling

---

## 📄 License

This project is intended for **academic and educational purposes only**.

---
