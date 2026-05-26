import sys
from pathlib import Path

from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parent.parent))

from api_calls import register_fixtures
from utils import generate_date_list


# registering fixtures in date interval
start_date = "2026-04-16"
end_date = "2026-04-30"
dates = generate_date_list(start_date, end_date)

for date in tqdm(dates):
    register_fixtures(date)