
from datetime import datetime
import pandas as pd


def get_issue(row):
	description, affected_system = row['description'], row['affected_system']
	issue = "Other"
	if 'loading' in description.lower():
		issue = "Loading"
	elif 'crashing' in description.lower():
		issue = "Crashing"
	return issue + "/" + affected_system


def get_weekday(creation_date):
	weekdays = {
		0: "Monday",
		1: "Tuesday",
		2: "Wednesday",
		3: "Thursday",
		4: "Friday",
		5: "Saturday",
		6: "Sunday"
	}
	# creation_date = datetime.strptime("%Y-%m-%d %H:%M:%S", creation_date)
	creation_date = datetime.strptime(creation_date[:19], "%Y-%m-%dT%H:%M:%S")
	return weekdays[creation_date.weekday()]


def add_features(data):
	return pd.DataFrame({
		'number': data['number'],
		'requestor': data['requestor'],
		'description': data['description'],
		'affected_system': data['affected_system'],
		'creation_date': data['creation_date'],
		'weekday': data['creation_date'].apply(get_weekday),
		'issue': data[['description', 'affected_system']].apply(get_issue, axis=1)
	})


def get_output_schema():   
	return pd.DataFrame({
		'number': prep_string(),
		'requestor': prep_string(),
		'description': prep_string(),
		'affected_system': prep_string(),
		'creation_date': prep_datetime(),
		'weekday': prep_string(),
		'issue': prep_string()
	})

