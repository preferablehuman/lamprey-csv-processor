import functions_framework
import pandas as pd
import io
import google.cloud.storage as gc
import numpy as np
from google.cloud.sql.connector import Connector
import pg8000
import sqlalchemy

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    location = request.get_json()["name"]
    client = gc.Client()
    blob = client.get_bucket("lamprey-pipeline-group3-textstore").blob(location)
    blob.download_to_filename("current_file.csv")
    contents = blob.download_as_bytes()

    byte_io = io.BytesIO(contents)
    data = pd.read_csv(byte_io)

    data = data.drop_duplicates()
    data = data.sort_values(by = ['TAG_ID', 'TIMESTAMP'])

    antenna_clean = np.array([])
    id_clean = np.array([])
    time_clean = np.array([])
    data_clean = pd.DataFrame()

    for index in range(1, len(data)):
        if data['ANTENNA'].iloc[index] == data['ANTENNA'].iloc[index-1] and data['TAG_ID'].iloc[index] == data['TAG_ID'].iloc[index-1]:
            if pd.Period(data['TIMESTAMP'].iloc[index]) - pd.Period(data['TIMESTAMP'].iloc[index-1]) > pd.Timedelta(10, 's'):
                antenna_clean = np.append(antenna_clean, str(data['ANTENNA'].iloc[index]))
                id_clean = np.append(id_clean, str(data['TAG_ID'].iloc[index]))
                time_clean = np.append(time_clean, data['TIMESTAMP'].iloc[index])
        else:
            antenna_clean = np.append(antenna_clean, str(data['ANTENNA'].iloc[index]))
            id_clean = np.append(id_clean, str(data['TAG_ID'].iloc[index]))
            time_clean = np.append(time_clean, data['TIMESTAMP'].iloc[index])

    data_clean['ANTENNA'] = antenna_clean
    data_clean['TAG_ID'] = id_clean
    data_clean['TIMESTAMP'] = time_clean
    data_clean = data_clean.sort_values(by = ['TIMESTAMP'])
    conn = getEngine()
    print("writing df to table")
    data_clean.to_sql(con=conn, name="LAMPREY_DETECTION", index=False, if_exists="append")

    print(data_clean.head())

    return "CSV processed", 200

def getEngine():
    print("getting sql instance")
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]

    connector = Connector()

    def getconn():
        conn = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
        )
        return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    print("secured connection")
    return engine
