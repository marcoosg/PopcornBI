class Rating:
    def __init__(self, movie_id, avg_rating, total_ratings, std_dev, last_rated):
        self.movie_id = movie_id
        self.avg_rating = avg_rating
        self.total_ratings = total_ratings
        self.std_dev = std_dev
        self.last_rated = last_rated

    @staticmethod
    def from_json(json_data):
        return Rating(
            movie_id=json_data['movie_id'],
            avg_rating=json_data['ratings_summary']['avg_rating'],
            total_ratings=json_data['ratings_summary']['total_ratings'],
            std_dev=json_data['ratings_summary'].get('std_dev'),
            last_rated=json_data['last_rated']
        )

    def show_details(self):
        print(f"Movie ID: {self.movie_id}")
        print(f"Average Rating: {self.avg_rating}")
        print(f"Total Ratings: {self.total_ratings}")
        print(f"Standard Deviation: {self.std_dev}")
        print(f"Last Rated: {self.last_rated}")
        print("\n")
