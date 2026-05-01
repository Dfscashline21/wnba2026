-- SQL script to create tables in Supabase
-- Run this in your Supabase SQL editor

-- Create the wnba schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS wnba;

-- Create PLAYER_GAME_LOGS table
CREATE TABLE IF NOT EXISTS wnba.PLAYER_GAME_LOGS (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    team_id INTEGER,
    minutes_played NUMERIC,
    points INTEGER,
    rebounds INTEGER,
    assists INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fouls INTEGER,
    field_goals_made INTEGER,
    field_goals_attempted INTEGER,
    three_pointers_made INTEGER,
    three_pointers_attempted INTEGER,
    free_throws_made INTEGER,
    free_throws_attempted INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create injuries table
CREATE TABLE IF NOT EXISTS wnba.injuries (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    team_id INTEGER,
    injury_type TEXT,
    status TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create TEAMS table
CREATE TABLE IF NOT EXISTS wnba.TEAMS (
    id SERIAL PRIMARY KEY,
    team_name TEXT,
    team_abbreviation TEXT,
    conference TEXT,
    division TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create PLAYERS table
CREATE TABLE IF NOT EXISTS wnba.PLAYERS (
    id SERIAL PRIMARY KEY,
    player_name TEXT,
    team_id INTEGER,
    position TEXT,
    jersey_number INTEGER,
    height TEXT,
    weight INTEGER,
    birth_date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create Games table
CREATE TABLE IF NOT EXISTS wnba.Games (
    id SERIAL PRIMARY KEY,
    game_date DATE,
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create wowy table
CREATE TABLE IF NOT EXISTS wnba.wowy (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    wowy_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create pace table
CREATE TABLE IF NOT EXISTS wnba.pace (
    id SERIAL PRIMARY KEY,
    team_id INTEGER,
    pace_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create underdog table
CREATE TABLE IF NOT EXISTS wnba.underdog (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    prop_type TEXT,
    line_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create draftkings table
CREATE TABLE IF NOT EXISTS wnba.draftkings (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    prop_type TEXT,
    line_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create prizepicks table
CREATE TABLE IF NOT EXISTS wnba.prizepicks (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    prop_type TEXT,
    line_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create betmgm table
CREATE TABLE IF NOT EXISTS wnba.betmgm (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    prop_type TEXT,
    line_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create caesars table
CREATE TABLE IF NOT EXISTS wnba.caesars (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    prop_type TEXT,
    line_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create projmins table
CREATE TABLE IF NOT EXISTS wnba.projmins (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    projected_minutes NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create todaysmins table
CREATE TABLE IF NOT EXISTS wnba.todaysmins (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    game_id INTEGER,
    actual_minutes NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);
