import os
from flask import Flask, Response, jsonify, make_response, request
from flask_cors import CORS
import requests
from redis import Redis
from io import BytesIO
import gzip

app = Flask(__name__)
CORS(app)

# Caching
cache = Redis(host='redis', port=6379)
file_server_host = os.getenv("FILE_SERVER_HOST")

# Compression
# def compress(data):
#     buf = BytesIO()
#     with gzip.GzipFile(fileobj=buf, mode='w') as f:
#         f.write(data)
#     return buf.getvalue()

def sendFile(filename,file_content):
    response = make_response(file_content)
    response.headers.set('Content-Disposition', 'attachment', filename=filename)
    response.headers.set('Content-Type', 'text/plain')   
    return response

@app.route('/api/fileserver/<filename>', methods=['GET'])
def get_file(filename):
    try:
        cached_data = cache.get(filename)
        if cached_data:
            return sendFile(filename,cached_data)
            # return sendFile(filename,gzip.decompress(cached_data).decode())
        file_url = f"{file_server_host}/api/fileserver/{filename}"
        file_response = requests.get(file_url)
        if file_response.status_code == 200:
            cache.set(filename, file_response.content)
            # return sendFile(filename,gzip.decompress(file_response.content).decode())
            return sendFile(filename,file_response.content)
        else:
            return file_response.content, file_response.status_code
    except Exception as e:
        error_message = "Error: {}".format(str(e))
        return jsonify({"error": error_message}), 500

@app.route('/api/fileserver/<filename>', methods=['PUT'])
def put_file(filename):
    try:
        data = request.get_data()
        file_url = f"{file_server_host}/api/fileserver/{filename}"
        headers = {'Content-Encoding': 'gzip'}
        # compressed_data = compress(data)
        file_response = requests.put(file_url, data=data,headers=headers)
        if file_response.status_code == 200:
            cache.delete(filename)
            cache.set(filename, data)
            return "file is created", 200
        else:
            return file_response.content, file_response.status_code
    except Exception as e:
        error_message = "Error: {}".format(str(e))
        return jsonify({"error": error_message}), 500

@app.route('/api/fileserver/<filename>', methods=['DELETE'])
def delete_file(filename):
    try:
        file_url = f"{file_server_host}/api/fileserver/{filename}"
        file_response = requests.delete(file_url)
        if file_response.status_code == 200:
            cache.delete(filename)
            return "", 200
        else:
            return file_response.content, file_response.status_code
    except Exception as e:
        error_message = "Error: {}".format(str(e))
        return jsonify({"error": error_message}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080,debug=True)
