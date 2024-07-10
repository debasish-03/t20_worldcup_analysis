from typing import Dict, Any, Set, Union
from datetime import datetime

import json
import pandas as pd
import json
import pandas as pd
from typing import Dict, Any
from datetime import datetime


def normalize_match(data: Dict[str, Any]) -> pd.DataFrame:
    return pd.json_normalize(data)

def normalize_series_matches(data: Dict[str, Any]) -> Union[pd.DataFrame, set]:
    matches = []
    match_ids = set()

    # Extract match details, skipping any "adDetail" entries
    for detail in data["matchDetails"]:
        if "matchDetailsMap" in detail:
            match_date = detail["matchDetailsMap"]["key"]
            if isinstance(detail["matchDetailsMap"]["match"], list):
                for match in detail["matchDetailsMap"]["match"]:
                    match_info = match.get("matchInfo", {})
                    match_score = match.get("matchScore", {})

                    # Convert start and end dates from epoch to human-readable format
                    match_info["startDate"] = datetime.fromtimestamp(int(match_info.get("startDate", 0)) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    match_info["endDate"] = datetime.fromtimestamp(int(match_info.get("endDate", 0)) / 1000).strftime('%Y-%m-%d %H:%M:%S')

                    # Add matchId to the set
                    match_id = match_info.get("matchId", "")
                    if match_id:
                        match_ids.add(match_id)

                    matches.append({
                        "matchDate": match_date,
                        "matchId": match_id,
                        "seriesId": match_info.get("seriesId", ""),
                        "seriesName": match_info.get("seriesName", ""),
                        "matchDesc": match_info.get("matchDesc", ""),
                        "matchFormat": match_info.get("matchFormat", ""),
                        "startDate": match_info.get("startDate", ""),
                        "endDate": match_info.get("endDate", ""),
                        "state": match_info.get("state", ""),
                        "status": match_info.get("status", ""),
                        "team1.teamId": match_info["team1"].get("teamId", "") if "team1" in match_info else "",
                        "team1.teamName": match_info["team1"].get("teamName", "") if "team1" in match_info else "",
                        "team1.teamSName": match_info["team1"].get("teamSName", "") if "team1" in match_info else "",
                        "team2.teamId": match_info["team2"].get("teamId", "") if "team2" in match_info else "",
                        "team2.teamName": match_info["team2"].get("teamName", "") if "team2" in match_info else "",
                        "team2.teamSName": match_info["team2"].get("teamSName", "") if "team2" in match_info else "",
                        "venueInfo.ground": match_info["venueInfo"].get("ground", "") if "venueInfo" in match_info else "",
                        "venueInfo.city": match_info["venueInfo"].get("city", "") if "venueInfo" in match_info else "",
                        "venueInfo.timezone": match_info["venueInfo"].get("timezone", "") if "venueInfo" in match_info else "",
                        "currBatTeamId": match_info.get("currBatTeamId", ""),
                        "team1Score": match_score["team1Score"].get("inngs1", {}) if "team1Score" in match_score else {},
                        "team2Score": match_score["team2Score"].get("inngs1", {}) if "team2Score" in match_score else {}
                    })
    
    df = pd.DataFrame(matches)
    return df, match_ids


