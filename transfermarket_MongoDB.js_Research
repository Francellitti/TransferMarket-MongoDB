/// Select teams from Serie A championship 2022-2023 sorted by average age of players in that team

db.Clubs.find({'domestic_competition_id': 'IT1','last_season':2022}, {'club_id':1,'name':1,'average_age':1,'net_transfer_record':1}).sort({'average_age':-1}).limit(10)

/// Informations about the first 8 Milan's current players sorted by age

var old_Milan = db.Players.find({
  'current_club_id': 5,
  'last_season': { $eq: 2022 },
  'position': { $ne: 'Goalkeeper' }
}, {
  'name': 1,
  'date_of_birth': 1,
  'sub_position': 1,
  'foot': 1,
  'market_value_in_eur': 1,
  'contract_expiration_date':1,
  'height_in_cm':1,
   'player_id':1
}).sort({
  'date_of_birth': 1
}).limit(8);


var documents = old_Milan.toArray();

/// Create a new collection and insert documents
db.createCollection('Milan');
db.Milan.insertMany(documents);

/// Informations about the first 8 Inter's current players sorted by age

var old_Inter = db.Players.find({
  'current_club_id': 46,
  'last_season': { $eq: 2022 },
  'position': { $ne: 'Goalkeeper' }
}, {
  'name': 1,
  'date_of_birth': 1,
  'sub_position': 1,
  'foot': 1,
  'market_value_in_eur': 1,
  'contract_expiration_date':1,
  'height_in_cm':1,
  'player_id':1
}).sort({
  'date_of_birth': 1
}).limit(8);

var documents = old_Inter.toArray();

/// Create a new collection and insert documents
db.createCollection('Inter');
db.Inter.insertMany(documents);

db.Appearences.find({'player_id':db.Players.find({
  'current_club_id': 46,
  'last_season': { $eq: 2022 },
  'position': { $ne: 'Goalkeeper' }
}, {
  'player_id':1
}).sort({
  'date_of_birth': 1
}).limit(8)});

/// Delete some years
db.Appearences.deleteMany({
  'date': {$regex:'^2018'}
});

/// Delete null matches only for saving matches from 2019 to 2022

db.Appearences.deleteMany(
  {'date': null}
);

/// Delete some retired players
db.Players.deleteMany({
  'last_season': {$eq:2018}
});

/// Milan's statistics of the 8 players from 2019

db.Appearences.aggregate([
  {
    $group: {
      _id: "$player_id",
      total_yellow_cards: { $sum: "$yellow_cards" },
      total_red_cards: { $sum: "$red_cards" },
      total_goals: { $sum: "$goals" },
      total_assists: { $sum: "$assists" },
      total_minutes_played: { $sum: "$minutes_played" },
      presence: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "Milan",
      localField: "_id",
      foreignField: "player_id",
      as: "playerInfo"
    }
  },
  {
    $unwind: "$playerInfo"
  },
  {
    $project: {
      player_id: "$player_id",
      player_name: "$playerInfo.name",
      date_of_birth: "$playerInfo.date_of_birth",
      sub_position: "$playerInfo.sub_position",
      foot: "$playerInfo.foot",
      height_in_cm: "$playerInfo.height_in_cm",
      market_value_in_eur: "$playerInfo.market_value_in_eur",
      contract_expiration_date: "$playerInfo.contract_expiration_date",
      total_yellow_cards: 1,
      total_red_cards: 1,
      total_goals: 1,
      total_assists: 1,
      total_minutes_played: 1,
      presence: 1
    }
  },
  {
      $out:'Milan'
  }
])



/// Inter's statistics of the 8 players from 2019

db.Appearences.aggregate([
  {
    $group: {
      _id: "$player_id",
      total_yellow_cards: { $sum: "$yellow_cards" },
      total_red_cards: { $sum: "$red_cards" },
      total_goals: { $sum: "$goals" },
      total_assists: { $sum: "$assists" },
      total_minutes_played: { $sum: "$minutes_played" },
      presence: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "Inter",
      localField: "_id",
      foreignField: "player_id",
      as: "playerInfo"
    }
  },
  {
    $unwind: "$playerInfo"
  },
  {
    $project: {
      player_id: "$player_id",
      player_name: "$playerInfo.name",
      date_of_birth: "$playerInfo.date_of_birth",
      sub_position: "$playerInfo.sub_position",
      foot: "$playerInfo.foot",
      height_in_cm: "$playerInfo.height_in_cm",
      market_value_in_eur: "$playerInfo.market_value_in_eur",
      contract_expiration_date: "$playerInfo.contract_expiration_date",
      total_yellow_cards: 1,
      total_red_cards: 1,
      total_goals: 1,
      total_assists: 1,
      total_minutes_played: 1,
      presence: 1
    }
  },
  {
      $out:'Inter'
  }
])


/// Statistics we need for all the Players

db.createCollection('AggregatedPlayers');

db.Appearences.aggregate([
  {
    $group: {
      _id: "$player_id",
      total_yellow_cards: { $sum: "$yellow_cards" },
      total_red_cards: { $sum: "$red_cards" },
      total_goals: { $sum: "$goals" },
      total_assists: { $sum: "$assists" },
      total_minutes_played: { $sum: "$minutes_played" },
      presence: { $sum: 1 }
      
    }
  },
  {
    $lookup: {
      from: "Players",
      localField: "_id",
      foreignField: "player_id",
      as: "playerInfo"
    }
  },
  {
    $unwind: "$playerInfo"
  },
  {
    $project: {
      player_id: "$player_id",
      player_name: "$playerInfo.name",
      date_of_birth: "$playerInfo.date_of_birth",
      sub_position: "$playerInfo.sub_position",
      foot: "$playerInfo.foot",
      height_in_cm: "$playerInfo.height_in_cm",
      market_value_in_eur: "$playerInfo.market_value_in_eur",
      contract_expiration_date: "$playerInfo.contract_expiration_date",
      current_team: "$playerInfo.current_club_id",
      last_seas:"$playerInfo.last_season",
      citizen:"$playerInfo.country_of_citizenship",
      current_club_dom_id:"$current_club_domestic_id",
      total_yellow_cards: 1,
      total_red_cards: 1,
      total_goals: 1,
      total_assists: 1,
      total_minutes_played: 1,
      presence: 1
    }
  },
  {
      $out:'AggregatedPlayers'
  }
])


/// Insert the name_code of the the team with a certain id


db.AggregatedPlayers.aggregate([
  {
    $lookup: {
      from: "Clubs",
      localField: "current_team",
      foreignField: "club_id",
      as: "clubInfo"
    }
  },
  {
    $unwind: "$clubInfo"
  },
  {
    $project: {
      player_id: 1,
      player_name: 1,
      date_of_birth: 1,
      sub_position: 1,
      foot: 1,
      height_in_cm: 1",
      market_value_in_eur: 1,
      contract_expiration_date: 1,
      current_team: 1",
      last_seas:1,
      citizen:1",
      current_club_dom_id:"$current_team",
      current_club_code:"$clubInfo.club_code",
      total_yellow_cards: 1,
      total_red_cards: 1,
      total_goals: 1,
      total_assists: 1,
      total_minutes_played: 1,
      presence:1

    }
  },
  {
      $out:'AggregatedPlayersClub'
  }
])

/// Add the aggregate statistics for the player

db.AggregatedPlayersClub.aggregate([{
    $addFields: {
        goals_min: {$divide: ['$total_goals',"$total_minutes_played"]},
        assists_min: {$divide: ['$total_assists',"$total_minutes_played"]},
        yc_min: {$divide: ['$total_yellow_cards',"$total_minutes_played"]},
        rc_min: {$divide: ['$total_red_cards',"$total_minutes_played"]}
    }
}])


//// Generating indexes


db.Appearences.createIndex({ game_id: 1 });
db.Games.createIndex({ game_id: 1 }); 


/// Merge datasets Appearences and Games conditionally on team_home_id/team_away_id with team_id

db.Appearences.aggregate([
  {
    $lookup: {
      from: "Games",
      localField: "game_id",
      foreignField: "game_id",
      as: "gameInfo"
    }
  },
  {
    $unwind: "$gameInfo"
  },
  {
    $group: {
      _id: "$player_id",
      goals_scored: {
        $sum: {
          $cond: [
            {
              $eq: ["$player_club_id", "$gameInfo.home_club_id"]
            },
            "$gameInfo.home_club_goals",
            "$gameInfo.away_club_goals"
          ]
        }
      },
      goals_conceded: {
        $sum: {
          $cond: [
            {
              $eq: ["$player_club_id", "$gameInfo.home_club_id"]
            },
            "$gameInfo.away_club_goals",
            "$gameInfo.home_club_goals"
          ]
        }
      }
    }
  },
  {
    $lookup: {
      from: "AggregatedPlayersClub",
      localField: "_id",
      foreignField: "_id",
      as: "playerClubInfo"
    }
 },
 {
     $unwind: "$playerClubInfo"
 },
 {
     $project: {
      player_id: "$_id",
      total_yellow_cards: "$playerClubInfo.total_yellow_cards",
      total_red_cards: "$playerClubInfo.total_red_cards",
      total_goals: "$playerClubInfo.total_goals",
      total_assists: "$playerClubInfo.total_assists",
      total_minutes_played: "$playerClubInfo.total_minutes_played",
      presence: "$playerClubInfo.presence",
      goals_scored: 1,
      goals_conceded: 1,
      player_name: "$playerClubInfo.player_name",
      date_of_birth: "$playerClubInfo.date_of_birth",
      sub_position: "$playerClubInfo.sub_position",
      foot: "$playerClubInfo.foot",
      height_in_cm: "$playerClubInfo.height_in_cm",
      market_value_in_eur: "$playerClubInfo.market_value_in_eur",
      contract_expiration_date: "$playerClubInfo.contract_expiration_date",
      last_seas: "$playerClubInfo.last_seas",
      citizen: "$playerClubInfo.citizen",
      current_club_dom_id: "$playerClubInfo.current_club_dom_id",
      current_club_code: "$playerClubInfo.current_club_code"   
    }
 },
 {
     $out: "Final_Players"
 }
])


db.Final_Players.aggregate([ {
    $addFields: {
        min_90: {$divide: ["$total_minutes_played",90]}    }
}, {
    $merge: {
      into: "Final_Players",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  } ])


db.Final_Players.aggregate([
  {
    $addFields: {
      goals_90: {
        $divide: [
          
          { $toDouble: "$total_goals" },
          "$min_90"
        ]
      },
      
      assists_90: {
          $divide: [
          
          { $toDouble: "$total_assists" },
          "$min_90"
        ]
      },
      
      conceded_90: {
          $divide: [
          
          { $toDouble: "$goals_conceded" },
          "$min_90"
        ]
      },
      
      scored_90: {
          $divide: [
          
          { $toDouble: "$goals_scored" },
          "$min_90"
        ]
      }
      
    }
  },
  {
    $merge: {
      into: "Final_Players",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  }
]);


/// Index also for Milan and Inter database:

/// Select Milan players
/// Obtain all the _id from "Milan"
var Milan_id = db.Milan.distinct("_id");
print(Milan_id)
/// Use the _id to filter the documents in "Final_Players"
db.Milan.createIndex({ _id: 1 });
db.Final_Players.createIndex({ _id: 1 });

db.Final_Players.aggregate([
  {
    $match: {
      _id: {
        $in: Milan_id
      }
    }
  },
  {
      $out: "Milan_final"
  }
]);

db.Milan_final.aggregate([ {
    $addFields: {
        min_90: {$divide: ["$total_minutes_played",90]}    }
}, {
    $merge: {
      into: "Milan_final",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  } ])


db.Milan_final.aggregate([
  {
    $addFields: {
      goals_90: {
        $divide: [
          
          { $toDouble: "$total_goals" },
          "$min_90"
        ]
      },
      
      assists_90: {
          $divide: [
          
          { $toDouble: "$total_assists" },
          "$min_90"
        ]
      },
      
      conceded_90: {
          $divide: [
          
          { $toDouble: "$goals_conceded" },
          "$min_90"
        ]
      },
      
      scored_90: {
          $divide: [
          
          { $toDouble: "$goals_scored" },
          "$min_90"
        ]
      }
      
    }
  },
  {
    $merge: {
      into: "Milan_final",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  }
]);


/// Select Inter players
/// Obtain all the _id from "Inter"
var Inter_id = db.Inter.distinct("_id");
print(Inter_id)
/// Use the _id to filter the documents in "Final_Players"
db.Inter.createIndex({ _id: 1 });

db.Final_Players.aggregate([
  {
    $match: {
      _id: {
        $in: Inter_id
      }
    }
  },
  {
      $out: "Inter_final"
  }
]);

/// Update some values

db.Inter_final.aggregate([ {
    $addFields: {
        min_90: {$divide: ["$total_minutes_played",90]}    }
}, {
    $merge: {
      into: "Inter_final",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  } ])


db.Inter_final.aggregate([
  {
    $addFields: {
      goals_90: {
        $divide: [
          
          { $toDouble: "$total_goals" },
          "$min_90"
        ]
      },
      
      assists_90: {
          $divide: [
          
          { $toDouble: "$total_assists" },
          "$min_90"
        ]
      },
      
      conceded_90: {
          $divide: [
          
          { $toDouble: "$goals_conceded" },
          "$min_90"
        ]
      },
      
      scored_90: {
          $divide: [
          
          { $toDouble: "$goals_scored" },
          "$min_90"
        ]
      }
      
    }
  },
  {
    $merge: {
      into: "Inter_final",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  }
]);

/// Matching for parameters of interest (general trial)

db.Final_Players.find({
  'last_seas': { $eq: 2022 },
  'sub_position': { $ne: 'Goalkeeper' },
  'market_value_in_eur': { $gte: 1000000 },
  'height_in_cm': { $gte: 190 },  /// Convert into number
  'foot': 'right'
});

/// Substitute of Ibrahimovic

var playerReference = db.Final_Players.findOne({
  'player_id': 3455,
  'last_seas': 2022
});

var characteristics = {
   'market_value_in_eur': { $gt: 10000000 },
   'date_of_birth': { $gt: new Date('1997-01-01') },
  'sub_position': playerReference.sub_position,
  'height_in_cm': { $gte: playerReference.height_in_cm},
  'foot': playerReference.foot,
  'goals_90': {$gt: 0.6}
  
};

db.Final_Players.find(characteristics); /// Gianluca Scamacca


/// Substitute of Dzeko

var playerReference = db.Final_Players.findOne({
  'player_id': 28396,
  'last_seas': 2022
});

var characteristics = {
      $and: [
        { 'market_value_in_eur': { $gt: 10000000 } },
        { 'market_value_in_eur': { $lt: 50000000 } }
      ],
   'date_of_birth': { $gt: new Date('1998-01-01') },
  'sub_position': playerReference.sub_position,
  'height_in_cm': { $lte: playerReference.height_in_cm},
  'foot': playerReference.foot,
  'goals_90': {$gt: 0.8},
  'presence': {$gt:30},
  
};

db.Final_Players.find(characteristics); /// Brobbey


/// substitute of Acerbi

var playerReference = db.Final_Players.findOne({
  'player_id': 131075,
  'last_seas': 2022
});

var characteristics = {
     
   $and: [
        { 'market_value_in_eur': { $gt: 10000000 } },
        { 'market_value_in_eur': { $lt: 22000000 } }
      ],     
   'date_of_birth': { $gt: new Date('2000-01-01') },
  'sub_position': playerReference.sub_position,
  'height_in_cm': { $lte: playerReference.height_in_cm},
  'foot': playerReference.foot,
  'conceded_90': {$lt: 2},
  'presence': {$gt:30}
  
};

db.Final_Players.find(characteristics); /// Ignacio


/// substitute of Kjaer

var playerReference = db.Final_Players.findOne({
  'player_id': 48859,
  'last_seas': 2022
});

var characteristics = {
     
   'market_value_in_eur': { $lt: 10000000 },
   'date_of_birth': { $gt: new Date('2000-01-01') },
  'sub_position': playerReference.sub_position,
  'foot': playerReference.foot,
  'conceded_90': {$lt: 2},
  'citizen': 'Italy',
  'presence':{$gt: 10}
  
};

db.Final_Players.find(characteristics); /// Lovato


/// substitute of Mykitharian

var playerReference = db.Final_Players.findOne({
  'player_id': 55735,
  'last_seas': 2022
});

var characteristics = {
     
   'market_value_in_eur': { $gt: 20000000 },
   'date_of_birth': { $gt: new Date('1999-01-01') },
  'sub_position': playerReference.sub_position,
  'foot': playerReference.foot,
  'scored_90': {$gt: 2},
  'presence':{$gt: 10},
  'assists_90':{$gt: 0.3}
  
};

db.Final_Players.find(characteristics); /// Cherki


/// substitute of Rebic

var playerReference = db.Final_Players.findOne({
  'player_id': 187587,
  'last_seas': 2022
});

var characteristics = {
    $and: [
        { 'market_value_in_eur': { $gt: 20000000 } },
        { 'market_value_in_eur': { $lt: 40000000 } }
      ],
   'date_of_birth': { $gt: new Date('2000-01-01') },
  'sub_position': playerReference.sub_position,
  'foot': playerReference.foot,
  'scored_90': {$gt: 3},
  'presence':{$gt: 20},
  'goals_90':{$gt:0.5}
  
};

db.Final_Players.find(characteristics); ///Ansu Fati
