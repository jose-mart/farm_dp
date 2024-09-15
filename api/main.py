from flask import Flask, request, jsonify
from swagger.swagger import swagger_ui, SWAGGER_URL
from apis.routes import api_farm
import sys

# API entry point
app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({
        "Message": "API up and running."
    })

app.register_blueprint(swagger_ui, url_prefix=SWAGGER_URL)
app.register_blueprint(api_farm, url_prefix="/api")

if __name__ == '__main__':
    print(sys.executable)
    app.run(debug=True)