import json
import pandas as pd

class Movies:
    def __init__(self, id, title, release_date, budget, revenue):
        self.id = id
        self.title = title
        self.release_date = release_date
        self.budget = budget
        self.revenue = revenue

    @staticmethod
    def from_csv_row(row):
        return Movies(
            id=row['id'],
            title=row['title'],
            release_date=row['release_date'],
            budget=pd.to_numeric(row.get('budget'), errors='coerce') or 0,  # Handle invalid values
            revenue=pd.to_numeric(row.get('revenue'), errors='coerce') or 0,  # Handle invalid values
        )

    def show_details(self):
        print(f"ID: {self.id}")
        print(f"Title: {self.title}")
        print(f"Release Date: {self.release_date}")
        print(f"Budget: {self.budget}")
        print(f"Revenue: {self.revenue}")