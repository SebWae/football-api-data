import dags.mymodules.api_calls as api_calls
import dags.mymodules.utils as utils

yesterday = utils.date_of_yesterday()
api_calls.register_fixtures(yesterday)