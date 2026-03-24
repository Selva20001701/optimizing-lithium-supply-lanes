from pathlib import Path
import pandas as pd

# Get the project root based on where this script lives
# BASE_DIR = Path(__file__).resolve().parent.parent
BASE_DIR = Path("./02_reference:")
lane_file = BASE_DIR / "lane_master.xlsx"
carrier_file = BASE_DIR/ "carrier_master.xlsx"
logic_file = BASE_DIR / "shipment_logic.xlsx"

print("Lane file path:", lane_file)
print("Carrier file path:", carrier_file)
print("Logic file path:", logic_file)

lane_df = pd.read_excel(lane_file)
carrier_df = pd.read_excel(carrier_file)
logic_df = pd.read_excel(logic_file)

print("\nLane Master")
print(lane_df.head())

print("\nCarrier Master")
print(carrier_df.head())

print("\nShipment Logic")
print(logic_df.head())

print("\nAll files loaded successfully.")
print("Lane rows:", len(lane_df))
print("Carrier rows:", len(carrier_df))
print("Logic rows:", len(logic_df))