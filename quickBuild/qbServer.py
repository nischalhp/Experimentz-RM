from flask import Flask,request
from QuickBuild import QuickBuild 
import datetime
import operator

app = Flask(__name__)

userData = {}

@app.route('/')
def hello_world():
	return 'Hello World'

@app.route('/quickbuild',methods=['GET'])
def getUserData(): 
    now = datetime.datetime.now()
    week = 0
    if now.day <= 7:
        week = 1
    elif now.day > 7 and now.day <= 14:
        week = 2
    elif now.day > 14 and now.day <= 21:
        week = 3
    elif now.day > 21:
        week =4

    memberProduct = userData[request.args.get('userid')]
    products = memberProduct.getFrequencyOfProductPerWeek(week)
    products = sorted(products.items(), key=operator.itemgetter(1), reverse=True)
    avgCartSize = memberProduct.getAverageBasketSize()
    print avgCartSize
    output = {}
    for product_sorted in products:
        if len(output) < avgCartSize:
             output[product_sorted[0]] = product_sorted[1] 
        else:
            break
    print output
    return str(output)

if __name__=='__main__':
    userData = QuickBuild().user_based_freq_recommendation() 
    app.run(host='0.0.0.0',debug=True)

