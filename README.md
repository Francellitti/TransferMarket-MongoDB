# Data Analysis with MongoDB - Football Data Analysis Project
This repository contains the work done as part of a project focusing on data analysis using MongoDB, applied to football data. The aim of the project is to analyze football player and team statistics over multiple seasons, with the objective of helping sports directors make decisions about player recruitment and team rejuvenation.

### Authors:
- Francesco Cellitti
- Edoardo Negroni
## 1. Project Overview
The project is centered around analyzing a large set of football data using MongoDB, a NoSQL database. The dataset includes over 60,000 games from various football seasons, 400+ clubs, 30,000+ players, and millions of market valuations and player appearances. The main goal is to simulate the role of a sports director tasked with rejuvenating a football team by replacing older players with younger ones that have similar characteristics.

The database structure consists of multiple collections:

- Competitions: Information about the major European football competitions.
- Games: Data on games played in these competitions, including match results and other details.
- Clubs: Information about football clubs in these competitions.
- Players: Data on players, including personal details and statistics.
- Appearances: Data on individual player appearances, such as minutes played and events during the match.
## 2. Database Structure
The MongoDB database is structured as follows:

- Competitions: Contains information about the main European football competitions.
- Games: All the games played in these competitions with match details.
- Clubs: All the clubs participating in the competitions from 2012 onwards.
- Players: Player information and statistics.
- Appearances: Details about each player’s appearance in games, including minutes played, goals, assists, etc.

## 3. Data Cleaning and Preprocessing
To ensure the dataset is focused on the last four seasons (2019-2022), the following steps were taken:

Players who retired before 2022 were excluded from the analysis.
Games from the 2012-2018 seasons were removed.
Example query used for removing unwanted data:

```javascript
db.Appearences.deleteMany({
  'date': { $regex: '^2018' }
});
```


## 4. Research Question and Analysis
The main research question is based on the role of a sports director trying to rejuvenate a team while maintaining a high level of performance. The analysis focuses on identifying older players in teams and finding suitable younger replacements with similar attributes.

Key Steps:
- Identify Oldest Players: The two teams with the oldest average player ages in the Serie A 2022/2023 season were selected: Inter Milan and AC Milan.
- Filter Older Players: From each team, the 8 oldest non-goalkeeper players were identified.
- Add Player Statistics: Total statistics (yellow cards, red cards, goals, assists, minutes played) for these players were aggregated across the last seasons (2019-2022).
## 5. Advanced Analysis
To improve the analysis, additional statistical features were calculated and merged from different collections:

Aggregated Player Stats:

A new collection, AggregatedPlayers, was created to store the total number of yellow/red cards, goals, assists, and minutes played for each player.
Merge Appearances with Game Data:

Data from Appearances and Games collections were merged to calculate additional player statistics (goals scored and conceded in home/away games).
Indexes were created to optimize query performance.
Example index creation:

```javascript
db.Appearences.createIndex({ game_id: 1 });
db.Games.createIndex({ game_id: 1 });
```

## 6. MongoDB Operations and Optimizations
Key MongoDB Operations:
- group: Used to aggregate player statistics.
- lookup: Used to join data from different collections.
- unwind: Flatten arrays resulting from $lookup operations.
- project: Used to reshape the output data.
## 7. How to Run the Code
Prerequisites:

- MongoDB installed and running.
- Studio 3T IDE for MongoDB (or any MongoDB client).
- Dataset (can be imported using Studio 3T or MongoDB CLI).
- Setup:
  - Import the datasets into MongoDB using Studio 3T or the MongoDB import tools.
  - Follow the queries provided in the repository to recreate the database structure and perform the analysis.
- Running Queries:
  - Use MongoDB's aggregation framework and indexing to efficiently run queries, such as identifying the oldest players in a team or aggregating statistics for players over multiple seasons.
## 8. Conclusion
This project demonstrates the power of MongoDB in handling large and complex datasets for sports analytics. By leveraging MongoDB’s flexibility and performance features, we were able to clean and process the data, perform advanced statistical analysis, and create actionable insights for team rejuvenation strategies.

## 9. License
This project is licensed under the MIT License - see the LICENSE file for details.

## Credits
The data used in this project was collected by David Cereijo, and includes various football statistics sourced from Transfermarkt.

## Contact
If you have any questions or would like to contribute, feel free to open an issue or pull request.
