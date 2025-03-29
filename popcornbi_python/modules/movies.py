import json

class Movies:
    def __init__(self, id, title, release_date, budget, revenue, genres=None, production_companies=None, production_countries=None, spoken_languages=None):
        self.id = id
        self.title = title
        self.release_date = release_date
        self.budget = budget
        self.revenue = revenue
        self.genres = genres
        self.production_companies = production_companies
        self.production_countries = production_countries
        self.spoken_languages = spoken_languages

    @staticmethod
    def from_csv_row(row):
        def parse_json_field(field):
            if isinstance(field, str):
                try:
                    return json.loads(field.replace("'", '"'))
                except json.JSONDecodeError:
                    return None
            return None

        return Movies(
            id=row['id'],
            title=row['title'],
            release_date=row['release_date'],
            budget=float(row['budget']) if row.get('budget') else None,
            revenue=float(row['revenue']) if row.get('revenue') else None,
            genres=row['genres'].split(',') if isinstance(row.get('genres'), str) else None,
            production_companies=row['production_companies'].split(',') if isinstance(row.get('production_companies'), str) else None,
            production_countries=parse_json_field(row.get('production_countries')),
            spoken_languages=parse_json_field(row.get('spoken_languages'))
        )

    def show_details(self):
        print(f"ID: {self.id}")
        print(f"Title: {self.title}")
        print(f"Release Date: {self.release_date}")
        print(f"Budget: {self.budget}")
        print(f"Revenue: {self.revenue}")
        print(f"Genres: {self.genres}")
        print(f"Production Companies: {self.production_companies}")
        print(f"Production Countries: {self.production_countries}")
        print(f"Spoken Languages: {self.spoken_languages}")
        print("\n")
