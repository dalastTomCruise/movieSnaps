"""
100 diverse movies across genres for the screencap guessing game.
"""

MOVIES = [
    # Action
    "The Dark Knight",
    "Mad Max Fury Road",
    "Die Hard",
    "John Wick",
    "Mission Impossible Fallout",
    "The Matrix",
    "Gladiator",
    "Top Gun Maverick",
    "Speed",
    "Heat",

    # Sci-Fi
    "Inception",
    "Interstellar",
    "Blade Runner 2049",
    "The Martian",
    "Arrival",
    "Ex Machina",
    "Gravity",
    "District 9",
    "Edge of Tomorrow",
    "Dune",

    # Drama
    "The Shawshank Redemption",
    "Schindler's List",
    "Forrest Gump",
    "The Godfather",
    "A Beautiful Mind",
    "Good Will Hunting",
    "The Social Network",
    "Whiplash",
    "Parasite",
    "No Country for Old Men",

    # Comedy
    "The Grand Budapest Hotel",
    "Superbad",
    "Bridesmaids",
    "Groundhog Day",
    "The Big Lebowski",
    "Knives Out",
    "Game Night",
    "What We Do in the Shadows",
    "Crazy Rich Asians",
    "The Nice Guys",

    # Thriller / Mystery
    "Gone Girl",
    "Prisoners",
    "Zodiac",
    "Memento",
    "Rear Window",
    "Se7en",
    "Oldboy",
    "The Silence of the Lambs",
    "Nightcrawler",
    "Hereditary",

    # Horror
    "Get Out",
    "A Quiet Place",
    "The Shining",
    "It",
    "Midsommar",
    "The Witch",
    "Us",
    "Alien",
    "The Conjuring",
    "28 Days Later",

    # Animation
    "Spirited Away",
    "WALL-E",
    "The Lion King",
    "Toy Story",
    "Spider-Man Into the Spider-Verse",
    "Up",
    "Coco",
    "Howl's Moving Castle",
    "Ratatouille",
    "Kubo and the Two Strings",

    # Adventure / Fantasy
    "The Lord of the Rings The Fellowship of the Ring",
    "Harry Potter and the Sorcerer's Stone",
    "Pirates of the Caribbean",
    "Indiana Jones Raiders of the Lost Ark",
    "Jurassic Park",
    "The Princess Bride",
    "Pan's Labyrinth",
    "Everything Everywhere All at Once",
    "The Revenant",
    "Life of Pi",

    # Romance / Drama
    "La La Land",
    "Eternal Sunshine of the Spotless Mind",
    "Before Sunrise",
    "Her",
    "The Notebook",
    "Titanic",
    "Call Me by Your Name",
    "Brokeback Mountain",
    "Lost in Translation",
    "Amélie",

    # Crime / Heist
    "Pulp Fiction",
    "Ocean's Eleven",
    "The Departed",
    "Catch Me If You Can",
    "Heat",
    "Goodfellas",
    "Baby Driver",
    "Snatch",
    "Inside Man",
    "The Italian Job",
]

# Deduplicate while preserving order
seen = set()
MOVIES = [m for m in MOVIES if not (m in seen or seen.add(m))]
