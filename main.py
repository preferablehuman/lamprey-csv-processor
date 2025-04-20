import functions_framework
import pandas as pd
import io
import google.cloud.storage as gc
import numpy as np

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

    client = gc.Client()
    blob = client.get_bucket("lamprey-pipeline-group3-textstore").blob("040224PitDetectionData.csv")
    # blob.download_to_filename("current_file.csv")
    contents = blob.download_as_bytes()
    # return cv2.VideoCapture("current_file.mp4")
    # return cv2.VideoCapture(contents)

    byte_io = io.BytesIO(contents)
    data = pd.read_csv(byte_io)
    # df = pd.read_excel(byte_io)

    data = data.drop_duplicates()
    data = data.sort_values(by = ['Tag ID', 'Date & Time'])

    antenna_clean = np.array([])
    id_clean = np.array([])
    time_clean = np.array([])
    data_clean = pd.DataFrame()

    for index in range(1, len(data)):
        if data['Antenna'].iloc[index] == data['Antenna'].iloc[index-1] and data['Tag ID'].iloc[index] == data['Tag ID'].iloc[index-1]:
            if pd.Period(data['Date & Time'].iloc[index]) - pd.Period(data['Date & Time'].iloc[index-1]) > pd.Timedelta(10, 's'):
                antenna_clean = np.append(antenna_clean, str(data['Antenna'].iloc[index]))
                id_clean = np.append(id_clean, str(data['Tag ID'].iloc[index]))
                time_clean = np.append(time_clean, data['Date & Time'].iloc[index])
        else:
            antenna_clean = np.append(antenna_clean, str(data['Antenna'].iloc[index]))
            id_clean = np.append(id_clean, str(data['Tag ID'].iloc[index]))
            time_clean = np.append(time_clean, data['Date & Time'].iloc[index])

    data_clean['Antenna'] = antenna_clean
    data_clean['Tag ID'] = id_clean
    data_clean['Date & Time'] = time_clean
    data_clean = data_clean.sort_values(by = ['Date & Time'])

    print(data_clean.head())

    request_json = request.get_json(silent=True)
    request_args = request.args

    return request_json
