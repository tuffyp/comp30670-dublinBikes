from flask import Flask, render_template, jsonify, json
import csv

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')


'''@app.route('/getHistorical')
def getHistorical():
    csvfile= open('../bikesproject/static/data/bikesHistory.csv')
    jsonlist=[]
    reader= csvfile.reader(csvfile)
    for row in reader:
        jsonlist.append(row)
    return jsonify(jsonlist)'''

'''@app.route('/getHistorical')
def getHistorical():
    csv_file= open('../bikesproject/static/data/bikesHistory.csv', 'r')
    csv_reader = csv.reader(csv_file)
    jsonlist=[]
    for row in csv_reader:
        jsonlist.append(row)
    return jsonify(jsonlist)'''
        

if __name__ == '__main__':
    app.run()
