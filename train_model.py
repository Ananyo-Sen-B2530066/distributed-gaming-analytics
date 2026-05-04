import os
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from utils import load_and_transform

df = load_and_transform("data/cs2_dataset.csv")

# Features never include the target itself
FEATURES = ["deaths", "assists", "adr", "kast", "kddiff"]

# Train one model for each predictable target
TARGETS = ["kills", "assists", "adr", "kast", "kddiff"]

os.makedirs("model", exist_ok=True)

for target in TARGETS:
    # Build feature list: exclude the target from inputs
    features = [f for f in FEATURES if f != target]

    X = df[features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=50, max_depth=8, random_state=42)
    model.fit(X_train, y_train)

    model_path = f"model/model_{target}.pkl"
    with open(model_path, "wb") as f:
        pickle.dump((model, features), f)

    print(f"Trained model for target '{target}' → saved to {model_path}")

print("\nAll models trained successfully!")
