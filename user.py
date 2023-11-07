import pandas as pd
import requests
import s3fs
import logging
from datetime import datetime



user_count=100
nationality='US'
format='json'
bucket_name="vinay-aiflow-project-1"
timestamp = datetime.now().strftime("%Y%m%d%H%M%S") 
csv_file=f"s3://{bucket_name}/user_data_{timestamp}.csv"
pd.set_option('display.width', 100)

def user_api():
    try:
        api_url = f"https://randomuser.me/api/?results={user_count}&nat={nationality}&format={format}"
        response = requests.get(api_url)
        data = response.json()

        user_list=[]
        for user in data['results']:
            user_data={
                "FirstName": user['name']['first'],
                "LastName": user['name']['last'],
                "Gender":user['gender'],
                "Country":user['location']['country'],
                "State":user['location']['state'],
                "UserName":user['login']['username'],
                "Email":user['email']
                
            }
            user_list.append(user_data)
        
        df=pd.DataFrame(user_list)
        df.insert(0, 'S.No', range(1, len(df) + 1)) 
        df.to_csv(csv_file,index=False)
    except Exception as e:
        logging.error("An error occurred {str(e)}")