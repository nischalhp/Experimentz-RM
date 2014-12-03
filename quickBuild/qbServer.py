from flask import Flask
import quick_build

app = Flask(__name__)

userData = {}

@app.route('/')
def hello_world():
	return 'Hello World'

if __name__=='__main__':
        userData = prepareUserData() 
	app.run(debug=True)

