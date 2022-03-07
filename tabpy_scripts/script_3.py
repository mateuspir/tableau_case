
from datetime import datetime
import pandas as pd


def get_action(short_description):
	action = "Other"
	if any([i in short_description.lower() for i in ('configuration', 'setup')]):
		return "Setup"
	elif 'reboot' in short_description.lower():
		return "Rebooting"
	elif 'clean' in short_description.lower():
		return "Cleaning"
	return 'Other'


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
		'task_id': data['task_id'],
		'short_description': data['short_description'],
		'request_number': data['request_number'],
		'analyst_lan_id': data['analyst_lan_id'],
		'datetime': data['datetime'],
		'weekday': data['datetime'].apply(get_weekday),
		'action': data['short_description'].apply(get_action)
	})


def get_output_schema():   
	return pd.DataFrame({
		'task_id': prep_string(),
		'short_description': prep_string(),
		'request_number': prep_string(),
		'analyst_lan_id': prep_string(),
		'datetime': prep_datetime(),
		'weekday': prep_string(),
		'action': prep_string()
	})

