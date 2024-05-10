import requests
import os
import ssl
import subprocess
from dotenv import load_dotenv

load_dotenv()

def get_data_api() -> str:
    """
    Gets user data from O'Reilly API.

    Args:
        None
    Returns:
        String with user data in JSON formatting.
    """

    account_identifier = os.getenv("ACCOUNT_IDENTIFIER")
    api_key = os.getenv("OREILLY_API_KEY")

    headers={"Authorization": f"Token {api_key}"}

    requests.packages.urllib3.disable_warnings() 
    
    data = requests.get(f"https://learning.oreilly.com/api/v3/insights/accounts/{account_identifier}/user-activity", verify=False ,headers=headers).text

    return(data)

