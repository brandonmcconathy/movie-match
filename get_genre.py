import pandas as pd

df = pd.read_csv("data.csv", encoding="utf8")
data = df["Genre"]
genres = set()
for row in data:
    row_data = row.replace(" ", "").split(',')
    for genre in row_data:
        genres.add(genre)

sorted_genres = []
for genre in genres:
    sorted_genres.append(genre)

sorted_genres.sort()
print(sorted_genres)
