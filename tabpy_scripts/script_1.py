
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd


def get_experience(row):
	role, years_at_experian = row['role'], row['years_at_experian']
	
	level = role.split()[-1]
	if level == "III":

		level_label = "high"

	elif level == "II":

		level_label = "medium"

	else:

		level_label = "low"


	if level_label == "high":

		if years_at_experian > 3:
			return "Very high"

		return "High"

	elif level_label == "medium":

		if years_at_experian > 2:
			return "Medium high"

		return "Medium low"
	
	return "Low" # level_label == "low"


def get_years_at_experian(hiring_date, pattern="%Y-%m-%dT%H:%M:%S"):
	hiring_date = datetime.strptime(hiring_date[:19], pattern)
	return relativedelta(datetime.now(), hiring_date).years


def add_experience(data):
	data['years_at_experian'] = data['hiring_date'].apply(get_years_at_experian)
	return pd.DataFrame({
		'lan_id': data['lan_id'],
		'name': data['name'],
		'role': data['role'],
		'experience': data[['role', 'years_at_experian']].apply(get_experience, axis=1)
	})


def get_output_schema():   
	return pd.DataFrame({
		'lan_id': prep_string(),
		'name': prep_string(),
		'role': prep_string(),
		'experience': prep_string()
	})

