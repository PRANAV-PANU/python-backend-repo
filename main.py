from google.cloud import firestore, bigquery
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import base64
import os
from typing import Optional
# # Set the path to your service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "lloyds-hack-grp-50-7c44e0c51e30.json"

# # Initialize a BigQuery client
client = bigquery.Client()

# Path to your service account key file
service_account_path = 'lloyds-hack-grp-50-519054f38a1d.json'
# Initialize Firestore client
db = firestore.Client.from_service_account_json(service_account_path)  

app = FastAPI()

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class UserData(BaseModel):
    email: str
    password: str 

class ApplicationData(BaseModel):
    api_key : Optional[str] = None
    domain: str
    user_id: str 
    name: str

@app.post('/login')
def login(userData:UserData):
    return login_user(getattr(userData,'email'),getattr(userData,'password'))

def login_user(email,password):
    user_doc = db.collection('users').document('email')
    doc = user_doc.get()

    if not doc.exists:
        return {"error": "User does not exist"}, 400
    
    if password == 'abc':
        return doc.to_dict()
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid User",
            headers={"Custom-Header": "CustomHeaderValue"}
        )

@app.get('/applications')
def application_data(user_id:str):
    print("user_id: "+user_id)
    return get_application_data(user_id)

def get_application_data(user_id):
    collection_name='applications'
    field_name = 'user_id'
    collection_ref = db.collection(collection_name)
    query = collection_ref.where(field_name,"==",user_id)
    # Print the results
    value =  query.stream()
    app_list = []
    for val in value:
        app_list.append(val.to_dict())

    return {"message":app_list}

@app.post('/applications')
def add_application(applicationData: ApplicationData):
    return add_application_data(applicationData)

def add_application_data(application_data):
    application_data.api_key = base64.b32encode(os.urandom(16)).decode('utf-8')
    print(application_data)
    collection_ref = db.collection('applications')
    # Add a new document with an automatically generated ID
    doc_ref = collection_ref.add(application_data.dict())
    return {"message":"Document Added Successfully"}

@app.get('/get-application-data')
def application_data(api_key:str):
    query = """
    SELECT
    SUM(CASE WHEN EventRating = 'good' THEN 1 ELSE 0 END) AS good_event_count,
    SUM(CASE WHEN EventRating = 'bad' THEN 1 ELSE 0 END) AS bad_event_count,
    SUM(CASE WHEN EventRating = 'neutral' THEN 1 ELSE 0 END) AS neutral_event_count
    FROM
        `lloyds-hack-grp-50.eventData.dataTable`
    WHERE
        APIKey = '{0}';
    """.format(api_key)

    # Run the query
    query_job = client.query(query)  # API request

    # Process the results
    results = query_job.result()  # Waits for the job to complete

    # Print the results
    for row in results:
        return {"good": row.good_event_count, "bad": row.bad_event_count, "neutral":row.neutral_event_count}

@app.get('/get-session-level-data')
def application_data(api_key:str):
    query = """
    WITH latest_sessions AS (
    SELECT 
        sessionId,
        MAX(timestamp_start) AS last_event_time
    FROM 
        `lloyds-hack-grp-50.eventData.event_data_table`
    WHERE APIKey = @api_key
    GROUP BY 
        sessionId
    ORDER BY 
        last_event_time DESC
    LIMIT 50
    )

    SELECT
        ud.sessionId,
        COUNT(CASE WHEN ud.eventRating = 'good' THEN 1 END) AS good_event_count,
        COUNT(CASE WHEN ud.eventRating = 'bad' THEN 1 END) AS bad_event_count
    FROM
        `lloyds-hack-grp-50.eventData.event_data_table` ud
    JOIN
        latest_sessions ls
    ON
        ud.sessionId = ls.sessionId
    GROUP BY
        ud.sessionId
    ORDER BY
        ud.sessionId""".format(api_key)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("api_key", "STRING", api_key)
        ]
    )

    query_job = client.query(query, job_config=job_config)

    results = query_job.result()

    
    result_value = []
    for row in results:
        value = {
            'session_id' : row['sessionId'],
            'good_event_count': row['good_event_count'],
            'bad_event_count': row['bad_event_count']
        }
        result_value.append(value)

    return {"value": result_value}