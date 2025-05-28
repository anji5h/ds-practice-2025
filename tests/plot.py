import pandas as pd
import matplotlib.pyplot as plt
import os

# Load stats history
csv_dir = os.path.join(os.getcwd(), "tests")
csv_file = os.path.join(csv_dir, "csv", "_stats_history.csv")
plot_dir = os.path.join(csv_dir, "plots")

# Create plot directory if it doesn't exist
os.makedirs(plot_dir, exist_ok=True)

# Read the CSV
df = pd.read_csv(csv_file)

# Remove irrelevant rows
df = df[~df["Name"].isin(["Aggregated", "Total"])]

# Plot per task name
for name in df["Name"].unique():
    task_df = df[df["Name"] == name]

    plt.figure(figsize=(12, 6))
    plt.plot(task_df["Timestamp"], task_df["Total Average Response Time"], label="Avg Response Time")
    plt.plot(task_df["Timestamp"], task_df["Total Median Response Time"], label="Median Response Time")
    plt.plot(task_df["Timestamp"], task_df["Total Min Response Time"], label="Min Response Time")
    plt.plot(task_df["Timestamp"], task_df["Total Max Response Time"], label="Max Response Time")

    plt.title(f"Response Time Metrics for Task: {name}")
    plt.xlabel("Timestamp")
    plt.ylabel("Time (ms)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    output_path = os.path.join(plot_dir, f"{name}.png")
    plt.savefig(output_path)
    plt.close()