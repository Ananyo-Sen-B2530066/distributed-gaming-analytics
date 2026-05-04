\# 🎮 Distributed Gaming Analytics System



A scalable gaming analytics platform that uses \*\*distributed computing (Ray)\*\* to perform \*\*parallel player analysis and prediction\*\*.



\---



\## 🚀 Features



\* Parallel player analysis using Ray

\* Machine Learning-based prediction (Random Forest)

\* Statistical performance evaluation

\* Flask-based web interface

\* Scalable multi-node architecture



\---



\## 🧠 System Architecture



\* \*\*Frontend\*\*: HTML UI

\* \*\*Backend\*\*: Flask

\* \*\*Distributed Engine\*\*: Ray

\* \*\*Workers\*\*: Execute tasks in parallel

\* \*\*Models\*\*: Loaded locally in workers



\---



\## ⚙️ Installation



\### 1. Clone the repository



```bash

git clone <your-repo-link>

cd gaming\_analytics

```



\### 2. Create virtual environment



```bash

python3 -m venv ray\_env

source ray\_env/bin/activate

```



\### 3. Install dependencies



```bash

pip install -r requirements.txt

```



\---



## 🧑‍💻 Worker Node Requirements

To ensure proper distributed execution, each worker node must satisfy the following conditions:

### ✅ 1. Same Codebase

* All worker nodes must have a copy of the project directory
* Directory structure should be identical to the master node

---

### ✅ 2. Python Environment

* Python version should match the master node (recommended: Python 3.10+)
* Virtual environment must be created and activated:

```bash
python3 -m venv ray_env
source ray_env/bin/activate
```

---

### ✅ 3. Install Dependencies

* Install all required packages using:

```bash
pip install -r requirements.txt
```

---

### ✅ 4. Model Files

* Workers must have access to trained models:

```
model/model_*.pkl
```

* Models are loaded locally inside each worker (not shared via network)

---

### ✅ 5. Dataset (Optional but Recommended)

* Required if statistical analysis is used
* Place dataset in:

```
data/cs2_dataset.csv
```

---

### ✅ 6. Network Connectivity

* All nodes must be on the same network
* Workers must be able to reach the master node via IP and port

---

### ✅ 7. Ray Setup

Run the following command on each worker:

```bash
ray start --address='MASTER_IP:6379'
```

---

### ⚠️ Important Notes

* Ensure no firewall blocks the connection
* Use the same port as the master node
* All workers must be started before running the Flask app

---

### 💡 Key Insight

Workers execute tasks independently and load models locally, which reduces data transfer and improves performance.




\## 🧪 Running the Project (Distributed Setup)



\### Step 1: Start Master Node



```bash

ray start --head --port=6379

```



\---



\### Step 2: Connect Worker Nodes



Run on each worker machine:



```bash

ray start --address='MASTER\_IP:6379'

```



\---



\### Step 3: Run Application (Master Node)



```bash

python app.py

```



\---



\### Step 4: Open in Browser



```

http://MASTER\_IP:5000

```



\---



\## ⚡ How It Works



\* User inputs player data

\* Flask converts each player into a task

\* Tasks are distributed using Ray

\* Workers execute tasks in parallel

\* Results are aggregated and returned



\---



\## 🧩 Project Structure



```

gaming\_analytics/

│

├── app.py

├── ray\_tasks.py

├── train\_model.py

├── utils.py

│

├── model/

├── data/

├── templates/

│

└── README.md

```



\---



\## 🔥 Optimization Techniques



\* Lazy initialization of shared data

\* Minimal data transfer (only averages shared)

\* Local model loading in workers

\* Task-level parallelism



\---



\## 📌 Future Work



\* Cloud deployment (multi-node scaling)

\* Real-time streaming analytics

\* Improved ML models



\---



\## 👥 Contributors



\* \*\*Member 1\*\*: ML models, statistical analysis, UI

\* \*\*Member 2\*\*: Ray setup, Flask backend, distributed execution



\---



\## 📄 License



This project is for academic and educational use.



