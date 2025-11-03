import pandas as pd

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

    print("\n\nRecommended Movies\n")
    if len(res) == 0:
        print("No movies match those parameters.")
    else:
        for index in res:
            print(data.iloc[index]["Series_Title"])

def confirm_quit():
    while True:
        ans = input("Are you sure you want to quit? (y/n): ")
        ans = ans.lower()
        if ans == 'y':
            return True
        if ans == 'n':
            break
        print("Invalid response. Please try again.")


if __name__ == "__main__":
    display_title()
    display_description()
    data = get_data()
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
                display_movies(data, genre=genre)
                print()
                display_instructions()
            case 'p':
                display_parameters(genre=genre)
                display_instructions()
            case 'q':
                if confirm_quit():
                    break
            case _:
                print("Invalid command please try again.")

    display_goodbye()
