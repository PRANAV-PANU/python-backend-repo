from google.cloud import firestore
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

# Path to your service account key file
service_account_path = 'lloyds-hack-grp-50-98938c24feeb.json'
# Initialize Firestore client
db = firestore.Client.from_service_account_json(service_account_path)  

app = FastAPI()


def login_user(email,password):
    user_doc = db.collection('users').document('evk7p33pLRG0HjxZ12lK')
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
    # Execute the query and get the results
    # results = query.stream()
    # Print the results
    value =  query.stream()
    app_list = []
    for val in value:
        app_list.append(val.to_dict())

    return {"message":app_list}
    # if not doc.exists:
    #     return {"error": "User does not exist"}, 400
    
    # if password == 'abc':
    #     return doc.to_dict()
    # else:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="Invalid User",
    #         headers={"Custom-Header": "CustomHeaderValue"}
    #     )
    


class UserData(BaseModel):
    email: str
    password: str 

# @app.post('')

# @app.put('')

@app.post('/login')
def login(userData:UserData):
    return login_user(getattr(userData,'email'),getattr(userData,'password'))