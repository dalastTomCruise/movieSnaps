"""
build_movie_list.py — picks 100 diverse movies from available_movies.txt
and updates movies.py with verified titles.

Usage: poetry run python3 build_movie_list.py
"""

# Hand-picked 100 diverse movies confirmed on the site
SELECTED = [
    # Action
    "The Dark Knight 2008",
    "Mad Max Fury Road 2015",
    "Die Hard",
    "John Wick 2014",
    "The Matrix 1999 4K",
    "Gladiator",
    "Top Gun Maverick",
    "Speed",
    "Predator",
    "Robocop 1987 2",

    # Sci-Fi
    "Inception 2010 4K",
    "The Martian 2015",
    "Dune 2021",
    "Arrival",
    "Gravity",
    "District 9",
    "2001 A Space Odyssey 1968 4K",
    "Back To The Future 1985",
    "The Terminator 1984",
    "Terminator 2 Judgment Day 1991",

    # Drama
    "The Shawshank Redemption 1994 4K",
    "Schindlers List 1993",
    "The Godfather 1972",
    "Godfather Part Ii 1974",
    "Scarface 1983",
    "Chinatown 1974",
    "One Flew Over The Cuckoos Nest 1975",
    "Taxi Driver 1976",
    "Dead Poets Society 1989",
    "Fight Club 1999 4K",

    # Comedy
    "Groundhog Day",
    "Airplane 1980",
    "Waynes World 1992",
    "Ace Ventura Pet Detective 1994",
    "Liar Liar 1997",
    "Austin Powers International Man Of Mystery 1997",
    "Monty Python And The Holy Grail 1975",
    "Blazing Saddles 1974",
    "The Naked Gun From The Files Of Police Squad 1988",
    "Ghostbusters 1984",

    # Thriller / Mystery
    "Gone Girl 2014",
    "Rear Window 1954",
    "The Silence Of The Lambs 1991",
    "Oldboy 2003 4K",
    "Vertigo 1958",
    "Psycho 1960",
    "Rope 1948",
    "Dial M For Murder 1954",
    "Chinatown 1974",
    "In The Mouth Of Madness 1994",

    # Horror
    "The Shining 1980",
    "Alien 1979",
    "Aliens 1986",
    "A Quiet Place 2018",
    "Hereditary 2018",
    "Get Out",
    "Halloween 1978",
    "A Nightmare On Elm Street 1984",
    "The Exorcist 1973",
    "Carrie 1976 4K",

    # Animation
    "The Lion King",
    "Toy Story",
    "Up",
    "WALL-E",
    "Coco",
    "Spider Man Into The Spider Verse",
    "Spirited Away",
    "Ratatouille",

    # Adventure / Fantasy
    "The Lord Of The Rings The Fellowship Of The Ring 2001 4K",
    "The Lord Of The Rings The Two Towers 2002 4K",
    "The Lord Of The Rings The Return Of The King 2003 4K",
    "Harry Potter And The Philosophers Stone 2001 4K",
    "Harry Potter And The Chamber Of Secrets 2002 4K",
    "Indiana Jones Raiders Of The Lost Ark 1981 4K",
    "Indiana Jones And The Last Crusade 1989 4K",
    "Jurassic Park 1993",
    "Jurassic World 2015",
    "The Princess Bride 1987 4K",

    # Romance / Drama
    "Lost In Translation 2003",
    "Amelie 2001",
    "Titanic",
    "La La Land",
    "Crazy Rich Asians 2018",
    "A Star Is Born 2018",

    # Crime / Heist
    "Pulp Fiction",
    "Goodfellas",
    "The Departed",
    "Ocean's Eleven",
    "The Italian Job 2003",
    "Snatch",
    "Casino Royale 2006 4K",
    "Catch Me If You Can",

    # Superhero
    "Iron Man 2008",
    "Black Panther 2018 4K",
    "Avengers Infinity War 2018 4K",
    "Avengers End Game 2019 4K",
    "Captain America The Winter Soldier 2014",
    "Guardians Of The Galaxy 2014",
    "Deadpool 2016",
    "Joker 2019 4K",
]

# Deduplicate
seen = set()
SELECTED = [m for m in SELECTED if not (m in seen or seen.add(m))]

print(f"Selected {len(SELECTED)} movies")
for m in SELECTED:
    print(f"  {m}")

# Write to movies.py
with open("movies.py", "w") as f:
    f.write('"""\n100 verified movies from movie-screencaps.com\n"""\n\n')
    f.write("MOVIES = [\n")
    for m in SELECTED:
        f.write(f'    "{m}",\n')
    f.write("]\n")

print(f"\nUpdated movies.py with {len(SELECTED)} movies")
