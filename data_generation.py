
from datetime import timedelta
import random
import rstr
import names
import randomname


class Rule(object):

	"""
		Class to abstract the generation of artificial data from static rules like \
		(set, list, unique value...) and random generation using random module \
		and `reverse` regular expressions.
	"""


	def __init__(
		self,
		rule=None,
		is_regex=False,
		is_name=False,
		is_datetime=False,
		start_datetime=None,
		end_datetime=None
	):

		# check parameters data type
		if not isinstance(is_regex, bool):
			raise TypeError("is_regex must be boolean")

		if not isinstance(is_name, bool):
			raise TypeError("is_name must be boolean")

		if not isinstance(is_datetime, bool):
			raise TypeError("is_datetime must be boolean")

		# initialize default instance attributes
		self.start_datetime = None
		self.end_datetime = None
		if is_datetime:

			if start_datetime is None or end_datetime is None:
				raise Exception("Missing start_datetime and end_datetime")

			self.start_datetime = start_datetime
			self.end_datetime = end_datetime
			self.rule_type = "datetime"

		elif is_name:

			if rule or is_regex:
				other_params = []
				
				if rule is not None:
					other_params += ["rule is not None"]
				
				if is_regex:
					other_params += ["is_regex is True"]

				raise Exception("is_name is True, but " + " and ".join(other_params))

			self.rule_type = "name"

		elif isinstance(rule, list) or isinstance(rule, set):

			if is_regex:
				raise Exception("is_regex is True but a list or dictionary was given")

			self.rule_type = 'select from list/set'

		elif is_regex:
			self.rule_type = 'regex'

		elif isinstance(rule, int) or isinstance(rule, str):
			self.rule_type = 'unique value'

		else:

			params = {k: v for k,v in locals().items() if k != "self"}
			params = ", ".join([str(k) + " = " + str(v) for k, v in params.items()])
			raise NotImplementedError("rules based in a %s is not implemented yet" % params)

		# store given parameters as instance attributes
		self.rule = rule
		self.is_regex = is_regex
		self.is_name = is_name


	def generate(self, global_lookup=None):

		if global_lookup:
			return random.choice(global_lookup)

		elif self.rule_type == "datetime":
			return self.random_date(
				start=self.start_datetime,
				end=self.end_datetime
			)

		elif self.rule_type == "name":
			return names.get_full_name()

		elif self.rule_type == "select from list/set":
			return random.choice(self.rule)

		elif self.rule_type == 'regex':
			return rstr.xeger(self.rule)

		elif self.rule_type == 'unique value':
			return self.rule
		
		raise NotImplementedError("rules based in a %s is not implemented yet" % type(self.rule))
	

	def random_date(self, start, end):
	    """
		    Generate a random datetime between two datetime objects.
	    """
	    delta = end - start
	    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
	    random_second = random.randrange(int_delta)
	    return start + timedelta(seconds=random_second)
