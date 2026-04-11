-- SQL script to create tables in Supabase
-- Generated from AWS RDS structure
-- Run this in your Supabase SQL editor

-- Create the wnba schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS wnba;

-- Create PLAYER_GAME_LOGS table
CREATE TABLE IF NOT EXISTS wnba.PLAYER_GAME_LOGS (
    season_year TEXT,
    player_id BIGINT,
    player_name TEXT,
    nickname TEXT,
    team_id BIGINT,
    team_abbreviation TEXT,
    team_name TEXT,
    game_id TEXT,
    game_date TEXT,
    matchup TEXT,
    wl TEXT,
    min NUMERIC,
    fgm BIGINT,
    fga BIGINT,
    fg_pct NUMERIC,
    fg3m BIGINT,
    fg3a BIGINT,
    fg3_pct NUMERIC,
    ftm BIGINT,
    fta BIGINT,
    ft_pct NUMERIC,
    oreb BIGINT,
    dreb BIGINT,
    reb BIGINT,
    ast BIGINT,
    tov BIGINT,
    stl BIGINT,
    blk BIGINT,
    blka BIGINT,
    pf BIGINT,
    pfd BIGINT,
    pts BIGINT,
    plus_minus BIGINT,
    nba_fantasy_pts NUMERIC,
    dd2 BIGINT,
    td3 BIGINT,
    wnba_fantasy_pts NUMERIC,
    gp_rank BIGINT,
    w_rank BIGINT,
    l_rank BIGINT,
    w_pct_rank BIGINT,
    min_rank BIGINT,
    fgm_rank BIGINT,
    fga_rank BIGINT,
    fg_pct_rank BIGINT,
    fg3m_rank BIGINT,
    fg3a_rank BIGINT,
    fg3_pct_rank BIGINT,
    ftm_rank BIGINT,
    fta_rank BIGINT,
    ft_pct_rank BIGINT,
    oreb_rank BIGINT,
    dreb_rank BIGINT,
    reb_rank BIGINT,
    ast_rank BIGINT,
    tov_rank BIGINT,
    stl_rank BIGINT,
    blk_rank BIGINT,
    blka_rank BIGINT,
    pf_rank BIGINT,
    pfd_rank BIGINT,
    pts_rank BIGINT,
    plus_minus_rank BIGINT,
    nba_fantasy_pts_rank BIGINT,
    dd2_rank BIGINT,
    td3_rank BIGINT,
    wnba_fantasy_pts_rank BIGINT,
    available_flag BIGINT,
    GameKey TEXT
);

-- Create injuries table
CREATE TABLE IF NOT EXISTS wnba.injuries (
    player TEXT,
    team TEXT
);

-- Create TEAMS table
CREATE TABLE IF NOT EXISTS wnba.TEAMS (
    TeamName TEXT,
    TeamABB TEXT
);

-- Create PLAYERS table
CREATE TABLE IF NOT EXISTS wnba.PLAYERS (
    Player TEXT,
    Position TEXT,
    "Player.1" TEXT,
    "Unnamed: 3" TEXT,
    "Unnamed: 4" TEXT,
    "Unnamed: 5" NUMERIC,
    "Unnamed: 6" NUMERIC,
    "Unnamed: 7" NUMERIC,
    "Unnamed: 8" TEXT
);

-- Create Games table
CREATE TABLE IF NOT EXISTS wnba.Games (
    Home_abb TEXT,
    Away_abb TEXT
);

-- Create wowy table
CREATE TABLE IF NOT EXISTS wnba.wowy (
    Name TEXT,
    usage_boost NUMERIC,
    ast_boost NUMERIC,
    reb_boost NUMERIC
);

-- Create pace table
CREATE TABLE IF NOT EXISTS wnba.pace (
    TEAM_ID BIGINT,
    TEAM_NAME TEXT,
    PACE NUMERIC
);

-- Create underdog table
CREATE TABLE IF NOT EXISTS wnba.underdog (
    name TEXT,
    sport_id TEXT,
    assists TEXT,
    blks_stls TEXT,
    double_doubles TEXT,
    fantasy_points TEXT,
    field_goals_att TEXT,
    free_throws_made TEXT,
    game_high_scorer TEXT,
    offensive_rebounds TEXT,
    period_1_2_assists TEXT,
    period_1_2_points TEXT,
    period_1_2_pts_rebs_asts TEXT,
    period_1_2_rebounds TEXT,
    period_1_points TEXT,
    points TEXT,
    pts_asts TEXT,
    pts_rebs TEXT,
    pts_rebs_asts TEXT,
    rebounds TEXT,
    rebs_asts TEXT,
    steals TEXT,
    three_points_made TEXT,
    triple_doubles TEXT,
    turnovers TEXT
);

-- Create draftkings table
CREATE TABLE IF NOT EXISTS wnba.draftkings (
    Position TEXT,
    "Name + ID" TEXT,
    Name TEXT,
    ID BIGINT,
    Salary BIGINT,
    "Game Info" TEXT,
    TeamAbbrev TEXT,
    AvgPointsPerGame TEXT,
    min_x NUMERIC,
    min_y NUMERIC
);

-- Create prizepicks table
CREATE TABLE IF NOT EXISTS wnba.prizepicks (
    "attributes.name" TEXT,
    "attributes.team" TEXT,
    "attributes.odds_type" TEXT,
    "3-PT Attempted" NUMERIC,
    "3-PT Made" NUMERIC,
    Assists NUMERIC,
    "Blks+Stls" NUMERIC,
    "Blocked Shots" NUMERIC,
    "Defensive Rebounds" NUMERIC,
    "FG Attempted" NUMERIC,
    "Fantasy Score" NUMERIC,
    "Free Throws Made" NUMERIC,
    "Offensive Rebounds" NUMERIC,
    Points NUMERIC,
    "Pts+Asts" NUMERIC,
    "Pts+Rebs" NUMERIC,
    "Pts+Rebs+Asts" NUMERIC,
    Rebounds NUMERIC,
    "Rebs+Asts" NUMERIC,
    Steals NUMERIC,
    Turnovers NUMERIC
);

-- Create betmgm table
CREATE TABLE IF NOT EXISTS wnba.betmgm (
    player TEXT,
    date TEXT,
    "3-Pointers" NUMERIC,
    Assists NUMERIC,
    Points NUMERIC,
    "Points + Assists" NUMERIC,
    "Points + Rebounds" NUMERIC,
    Rebounds NUMERIC
);

-- Create caesars table
CREATE TABLE IF NOT EXISTS wnba.caesars (
    player TEXT,
    date TEXT,
    "3-Pointers" NUMERIC,
    Assists NUMERIC,
    Points NUMERIC,
    Rebounds NUMERIC,
    sportsbook TEXT,
    last_updated TEXT
);

-- Create projmins table
CREATE TABLE IF NOT EXISTS wnba.projmins (
    Name TEXT,
    TeamAbbrev TEXT,
    min NUMERIC
);

-- Create todaysmins table
CREATE TABLE IF NOT EXISTS wnba.todaysmins (
    Name TEXT,
    TeamAbbrev TEXT,
    min NUMERIC,
    gamedate TEXT
);
