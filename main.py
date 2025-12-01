import pandas as pd
import zmq
import json

def display_title():
    print("\n\n\n __  __  _____     _____ _____   __  __    _  _____ ____ _   _\n" 
    "|  \\/  |/ _ \\ \\   / /_ _| ____| |  \\/  |  / \\|_   _/ ___| | | |\n"
    "| |\\/| | | | \\ \\ / / | ||  _|   | |\\/| | / _ \\ | || |   | |_| |\n"
    "| |  | | |_| |\\ V /  | || |___  | |  | |/ ___ \\| || |___|  _  |\n"
    "|_|  |_|\\___/  \\_/  |___|_____| |_|  |_/_/   \\_\\_| \\____|_| |_|\n\n\n")

def display_description():
    print("Welcome to Movie Match! This program will let you enter parameters "
    "about movies and it will return a list of movies that best meet those parameters. "
    "The list of recommended movies will be very long if you leave some parameters blank. "
    "Enter more parameters to shorten the length of recommended movies."
    "\n\n")

def display_instructions():
    print("Chose a command and press enter.\n"
          "g - Genre Entry\n"
          "m - Display Movie List\n"
          "p - Show Parameters\n"
          'd - Show Differences\n'
          "s - Summarize Genres\n"
          "q - Quit\n")
    
def display_goodbye():
    print("\nThank you for using Movie Match. Have a good day!\n")

def display_parameters(genre:str):
    print("\n\nCurrent Set Parameters\n"
          f"Genre: {genre}\n")
    
def get_data() -> pd.core.frame.DataFrame:
    return pd.read_csv("data.csv", encoding="utf8")

def get_user_input() -> str:
    return input("Enter a command: ")

def update_genre(current_genre:str) -> str:
    print("\n\nGenre Entry\n")
    print(f"Current Genre: {current_genre}\n")
    valid_genres = ['Action', 'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 
                    'Drama', 'Family', 'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music', 
                    'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Sport', 'Thriller', 'War', 
                    'Western']
    
    # Print valid genres
    print("Valid Genres:")
    for genre in valid_genres:
        print(genre + ", ", end="")
    print("\n")
    
    while (True):
        user_input = input("Enter a new genre or enter b to go back: ")
        if user_input == 'b':
            return current_genre
        if user_input not in valid_genres:
            print("Invalid category. Enter a valid category or enter b to go back.")
        else:
            return user_input
        
def display_movies(data:pd.core.frame.DataFrame, genre:str):
    movies = []
    if genre == '':
        data = data.sort_values(by="IMDB_Rating", ascending=False)
        res = []
        for index, row in data.iterrows():

            #Genre
            if genre != "":
                genre_string = row["Genre"]
                row_genres = genre_string.replace(" ", "").split(',')
                if genre not in row_genres:
                    continue

            # Passes all checks add to res list
            res.append(index)
        for index in res:
            movies.append(data.iloc[index]["Series_Title"])
    else:
        res = summarize_groups(data)
        movies = res[genre]

    print("\n\nRecommended Movies\n")
    if len(res) == 0:
        print("No movies match those parameters.")
    else:        
        movies = shuffle_movies(movies)
        for movie in movies:
            print(movie)
        return movies
    
def diff_display(data:pd.core.frame.DataFrame, genre:str, prev_movies):
    data = data.sort_values(by="IMDB_Rating", ascending=False)
    res = []
    for index, row in data.iterrows():

        #Genre
        if genre != "":
            genre_string = row["Genre"]
            row_genres = genre_string.replace(" ", "").split(',')
            if genre not in row_genres:
                continue

        # Passes all checks add to res list
        res.append(index)

    print("\n\nDifferent Movies\n")
    if len(res) == 0:
        print("No movies match those parameters.")
    else:
        movies = []
        for index in res:
            movies.append(data.iloc[index]["Series_Title"])
        formatted_prev_movies = []
        for movie in prev_movies:
            formatted_prev_movies.append({"name": movie, "id": movie})
        formatted_movies = []
        for movie in movies:
            formatted_movies.append({"name": movie, "id": movie})
        movies = find_differences(formatted_prev_movies, formatted_movies)
        for movie in movies:
            print(movie["name"])
        return movies
    
def summarize_genres(data:pd.core.frame.DataFrame):
    res = []
    for index, row in data.iterrows():
        res.append(index)

    print("\n\nMovie Summary\n")
    if len(res) == 0:
        print("No movies match those parameters.")
    else:
        movies = []
        for index in res:
            movies.append({"id":data.iloc[index]["Series_Title"], "category":data.iloc[index]["Genre"].split(',')[0]})
        summary = get_summary(movies)
        for key, value in summary.items():
            print(f"{key}: {value}")

def summarize_groups(data:pd.core.frame.DataFrame):
    res = []
    for index, row in data.iterrows():
        res.append(index)

    print("\n\nGroup Summary\n")
    if len(res) == 0:
        print("No movies match those parameters.")
    else:
        movies = []
        for index in res:
            temp_categories = data.iloc[index]["Genre"].split(',')
            categories = []
            for category in temp_categories:
                categories.append(category.strip())
            movies.append({"id":data.iloc[index]["Series_Title"], "category":categories})
        return send_grouping_request(movies)


def confirm_quit():
    while True:
        ans = input("Are you sure you want to quit? (y/n): ")
        ans = ans.lower()
        if ans == 'y':
            return True
        if ans == 'n':
            break
        print("Invalid response. Please try again.")

# Shuffle service
def connect_client(port: int = 5555):
    """Create a ZeroMQ REQ socket connected to the given port."""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    address = f"tcp://localhost:{port}"
    socket.connect(address)
    return context, socket


def send_request(socket, payload: dict):
    """Send a JSON request and receive the JSON response."""

    socket.send_json(payload)
    response = socket.recv_json()
    res = response['result']['items']
    return res


def shuffle_no_seed(socket, items):
    """Test basic shuffle (no seed)."""
    payload = {
        "action": "shuffle",
        "items": items
    }
    return send_request(socket, payload)

def shuffle_movies(movies):
    port = 5555
    context, socket = connect_client(port)
    return_vals = shuffle_no_seed(socket, movies)
    socket.close()
    context.term()
    return return_vals


# Diff service
def send_diff_request(payload, label=None):
    """Send a request to the Change & Diff Tracker microservice."""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{5563}")

    socket.send(json.dumps(payload).encode("utf-8"))
    reply = socket.recv().decode("utf-8")
    res = json.loads(reply)

    socket.close()
    context.term()
    return res["removed_items"]


def find_differences(old_data, new_data):
    return send_diff_request(
        {
            "request_type": "change_diff",
            "id_field": "id",
            "fields_to_compare": ["name"],
            "before": old_data,
            "after": new_data
        }
    )

# Summary Service
def get_summary(movies):
    request_body = {
        "request_type": "category_tag_summary",
        "items": movies
    }

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5559")

    msg = json.dumps(request_body).encode("utf-8")
    socket.send(msg)

    reply = socket.recv().decode("utf-8")
    res = json.loads(reply)
    return res["category_summary"]

# Grouping Summary
def send_grouping_request(payload, label=None):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{5564}")

    socket.send(json.dumps(payload).encode("utf-8"))
    reply = socket.recv().decode("utf-8")

    socket.close()
    context.term()

    return json.loads(reply)


if __name__ == "__main__":
    display_title()
    display_description()
    data = get_data()
    prev_data = ''
    display_instructions()

    genre = ""

    while(True):
        user_input = get_user_input().lower()
        match user_input:
            case 'g':
                genre = update_genre(genre)
                print()
                display_instructions()
            case 'm':
                prev_data = display_movies(data, genre=genre)
                print()
                display_instructions()
            case 'p':
                display_parameters(genre=genre)
                display_instructions()
            case 'd':
                diff_display(data, genre=genre, prev_movies=prev_data)
                print()
                display_instructions()
            case 's':
                summarize_genres(data)
                print()
                display_instructions()
            case 'q':
                if confirm_quit():
                    break
            case _:
                print("Invalid command please try again.")

    display_goodbye()
