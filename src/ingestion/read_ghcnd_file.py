
def get_station_to_state_dict():
    us_state_codes = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]
    mapping_dict = {}
    with open("ghcnd_stations.txt") as f:
        
        for line in f:
            line_info = line.split()
            if line_info[4] in us_state_codes:
                mapping_dict[line_info[0]] = line_info[4]
    return mapping_dict


if __name__ == "__main__":
    get_station_to_state_dict()
