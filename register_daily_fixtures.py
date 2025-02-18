import api_calls
import utils

yesterday = utils.date_of_yesterday()
api_calls.register_fixtures(yesterday)