#import necessary libraries
import pandas as pd
import json

def main():
    # Reading JSON data for match results
    with open('t20_wc_match_results.json') as f:
        data = json.load(f)

    # Converting json data to dataframe
    df_match = pd.DataFrame(data[0]['matchSummary'])
    df_match.head()

    df_match.shape
    #Result - (45, 7)

    # Change a column name as required ofr further processing
    df_match.rename({'scorecard': 'match_id'}, axis=1, inplace=True)
    df_match.head()

    #### Create a match ids dictionary that maps team names to a unique match id. This will be useful later on to link with other tables ####
    # Creating a dictionary with team1 and team2 combinations (with reverse combination as well)
    match_ids_dict = {}

    for index, row in df_match.iterrows():
        key1 = row['team1'] + ' Vs ' + row['team2']
        key2 = row['team2'] + ' Vs ' + row['team1']
        match_ids_dict[key1] = row['match_id']
        match_ids_dict[key2] = row['match_id']
    match_ids_dict

    """
    Result :-
     {'Namibia Vs Sri Lanka': 'T20I # 1823',
     'Sri Lanka Vs Namibia': 'T20I # 1823',
     'Netherlands Vs U.A.E.': 'T20I # 1825',
     'U.A.E. Vs Netherlands': 'T20I # 1825',
     'Scotland Vs West Indies': 'T20I # 1826',
     'West Indies Vs Scotland': 'T20I # 1826',
     'Ireland Vs Zimbabwe': 'T20I # 1828',
     'Zimbabwe Vs Ireland': 'T20I # 1828',
     'Namibia Vs Netherlands': 'T20I # 1830',
     'Netherlands Vs Namibia': 'T20I # 1830',
     'Sri Lanka Vs U.A.E.': 'T20I # 1832',
     'U.A.E. Vs Sri Lanka': 'T20I # 1832',
     'Ireland Vs Scotland': 'T20I # 1833',
     'Scotland Vs Ireland': 'T20I # 1833',
     'West Indies Vs Zimbabwe': 'T20I # 1834',
     'Zimbabwe Vs West Indies': 'T20I # 1834',
     'Netherlands Vs Sri Lanka': 'T20I # 1835',
     'Sri Lanka Vs Netherlands': 'T20I # 1835',
     'Namibia Vs U.A.E.': 'T20I # 1836',
     'U.A.E. Vs Namibia': 'T20I # 1836',
     'Ireland Vs West Indies': 'T20I # 1837',
     'West Indies Vs Ireland': 'T20I # 1837',
     'Scotland Vs Zimbabwe': 'T20I # 1838',
     'Zimbabwe Vs Scotland': 'T20I # 1838',
     'Australia Vs New Zealand': 'T20I # 1839',
     'New Zealand Vs Australia': 'T20I # 1839',
     'Afghanistan Vs England': 'T20I # 1840',
     'England Vs Afghanistan': 'T20I # 1840',
     'Ireland Vs Sri Lanka': 'T20I # 1841',
     'Sri Lanka Vs Ireland': 'T20I # 1841',
     'India Vs Pakistan': 'T20I # 1842',
     'Pakistan Vs India': 'T20I # 1842',
     'Bangladesh Vs Netherlands': 'T20I # 1843',
     'Netherlands Vs Bangladesh': 'T20I # 1843',
     'South Africa Vs Zimbabwe': 'T20I # 1844',
     'Zimbabwe Vs South Africa': 'T20I # 1844',
     'Australia Vs Sri Lanka': 'T20I # 1845',
     'Sri Lanka Vs Australia': 'T20I # 1845',
     'England Vs Ireland': 'T20I # 1846',
     'Ireland Vs England': 'T20I # 1846',
     'Afghanistan Vs New Zealand': 'T20I # 1846a',
     'New Zealand Vs Afghanistan': 'T20I # 1846a',
     'Bangladesh Vs South Africa': 'T20I # 1847',
     'South Africa Vs Bangladesh': 'T20I # 1847',
     'India Vs Netherlands': 'T20I # 1848',
     'Netherlands Vs India': 'T20I # 1848',
     'Pakistan Vs Zimbabwe': 'T20I # 1849',
     'Zimbabwe Vs Pakistan': 'T20I # 1849',
     'Afghanistan Vs Ireland': 'T20I # 1849a',
     'Ireland Vs Afghanistan': 'T20I # 1849a',
     'Australia Vs England': 'T20I # 1849b',
     'England Vs Australia': 'T20I # 1849b',
     'New Zealand Vs Sri Lanka': 'T20I # 1850',
     'Sri Lanka Vs New Zealand': 'T20I # 1850',
     'Bangladesh Vs Zimbabwe': 'T20I # 1851',
     'Zimbabwe Vs Bangladesh': 'T20I # 1851',
     'Netherlands Vs Pakistan': 'T20I # 1852',
     'Pakistan Vs Netherlands': 'T20I # 1852',
     'India Vs South Africa': 'T20I # 1853',
     'South Africa Vs India': 'T20I # 1853',
     'Australia Vs Ireland': 'T20I # 1855',
     'Ireland Vs Australia': 'T20I # 1855',
     'Afghanistan Vs Sri Lanka': 'T20I # 1856',
     'Sri Lanka Vs Afghanistan': 'T20I # 1856',
     'England Vs New Zealand': 'T20I # 1858',
     'New Zealand Vs England': 'T20I # 1858',
     'Netherlands Vs Zimbabwe': 'T20I # 1859',
     'Zimbabwe Vs Netherlands': 'T20I # 1859',
     'Bangladesh Vs India': 'T20I # 1860',
     'India Vs Bangladesh': 'T20I # 1860',
     'Pakistan Vs South Africa': 'T20I # 1861',
     'South Africa Vs Pakistan': 'T20I # 1861',
     'Ireland Vs New Zealand': 'T20I # 1862',
     'New Zealand Vs Ireland': 'T20I # 1862',
     'Australia Vs Afghanistan': 'T20I # 1864',
     'Afghanistan Vs Australia': 'T20I # 1864',
     'England Vs Sri Lanka': 'T20I # 1867',
     'Sri Lanka Vs England': 'T20I # 1867',
     'Netherlands Vs South Africa': 'T20I # 1871',
     'South Africa Vs Netherlands': 'T20I # 1871',
     'Bangladesh Vs Pakistan': 'T20I # 1872',
     'Pakistan Vs Bangladesh': 'T20I # 1872',
     'India Vs Zimbabwe': 'T20I # 1873',
     'Zimbabwe Vs India': 'T20I # 1873',
     'New Zealand Vs Pakistan': 'T20I # 1877',
     'Pakistan Vs New Zealand': 'T20I # 1877',
     'England Vs India': 'T20I # 1878',
     'India Vs England': 'T20I # 1878',
     'England Vs Pakistan': 'T20I # 1879',
     'Pakistan Vs England': 'T20I # 1879'}
     """
    df_match.to_csv('dim_match_summary.csv', index=False)

    ##### Process Batting Summary #####
    # Reading JSON data for batting summary
    with open('t20_wc_batting_summary.json') as f:
        data = json.load(f)
        all_records = []
        for rec in data:
            all_records.extend(rec['battingSummary'])

    df_batting = pd.DataFrame(all_records)
    df_batting.head(11)

    # Converting the "dismissal" column to "out/not_out" where null records are not_out and non null records are out
    df_batting['out/not_out'] = df_batting.dismissal.apply(lambda x: "out" if len(x) > 0 else "not_out")
    df_batting.head(11)

    # Creating new column based on the match_ids_dict dictionary and mapping it against the match table
    df_batting['match_id'] = df_batting['match'].map(match_ids_dict)
    df_batting.head()

    # Dropping unwanted column
    df_batting.drop(columns=["dismissal"], inplace=True)
    df_batting.head(10)

    # Removing special characters from "batsmanName" column
    df_batting['batsmanName'] = df_batting['batsmanName'].apply(lambda x: x.replace('â€', ''))
    df_batting['batsmanName'] = df_batting['batsmanName'].apply(lambda x: x.replace('\xa0', ''))
    df_batting['batsmanName'] = df_batting['batsmanName'].apply(lambda x: x.replace('†', ''))
    df_batting['batsmanName'] = df_batting['batsmanName'].apply(lambda x: x.replace('(c)', ''))
    df_batting.head(15)

    df_batting.shape
    #Result - (699, 11)

    df_batting.to_csv('fact_bating_summary.csv', index=False)


    ##### Process Bowling Summary #####
    # Reading JSON data for bowling results
    with open('t20_wc_bowling_summary.json') as f:
        data = json.load(f)
        all_records = []
        for rec in data:
            all_records.extend(rec['bowlingSummary'])
    all_records[:2]

    """
    [{'match': 'Namibia Vs Sri Lanka',
    'bowlingTeam': 'Sri Lanka',
    'bowlerName': 'Maheesh Theekshana',
    'overs': '4',
    'maiden': '0',
    'runs': '23',
    'wickets': '1',
    'economy': '5.75',
    '0s': '7',
    '4s': '0',
    '6s': '0',
    'wides': '2',
    'noBalls': '0'},
    {'match': 'Namibia Vs Sri Lanka',
    'bowlingTeam': 'Sri Lanka',
    'bowlerName': 'Dushmantha Chameera',
    'overs': '4',
    'maiden': '0',
    'runs': '39',
    'wickets': '1',
    'economy': '9.75',
    '0s': '6',
    '4s': '3',
    '6s': '1',
    'wides': '2',
    'noBalls': '0'}]
    """

    # Converting bowling data into dataframe
    df_bowling = pd.DataFrame(all_records)
    print(df_bowling.shape)
    df_bowling.head()

    # Mapping the bowling data against the match_ids_dict dictionary created
    df_bowling['match_id'] = df_bowling['match'].map(match_ids_dict)
    df_bowling.head()

    df_bowling.to_csv('fact_bowling_summary.csv', index=False)


    ##### Process Players Information #####
    # Reading JSON data for players information
    with open('t20_wc_player_info.json') as f:
        data = json.load(f)

    df_players = pd.DataFrame(data)

    print(df_players.shape)
    df_players.head(10)

    df_players['name'] = df_players['name'].apply(lambda x: x.replace('â€', ''))
    df_players['name'] = df_players['name'].apply(lambda x: x.replace('†', ''))
    df_players['name'] = df_players['name'].apply(lambda x: x.replace('\xa0', ''))
    df_players.head(10)

    df_players[df_players['team'] == 'India']

    df_players.to_csv('dim_players_no_images.csv', index=False)

main()