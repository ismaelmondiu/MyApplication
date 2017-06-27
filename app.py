#!flask/bin/python
from flask import Flask, jsonify,abort,make_response,request,url_for
#from flask_pymongo import PyMongo
from pymongo  import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import json,requests,logging,re,sys
from logging.handlers import RotatingFileHandler
client = MongoClient()
client.admin.authenticate('tom', 'jerry')
mydb = client.offices
my_collection = mydb.offices

app = Flask(__name__)
formatter = logging.Formatter('%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')
handler = RotatingFileHandler('/opt/opendata/logs/supervision.log', maxBytes=2000000, backupCount=10)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
app.logger.addHandler(handler)
app.logger.setLevel(logging.DEBUG)
app.logger.info('Lancement du script')


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

def make_public_task(task):
    new_task = {}
    for field in task:
        if field == 'id':
            new_task['uri'] = url_for('get_task', task_id=task['id'], _external=True)
        else:
            new_task[field] = task[field]
    return new_task

@app.route('/offices/<string:office_id>', methods=['PUT'])
def update_office(office_id):
    if len(office_id) == 0:
        abort(404)
    if not request.json:
        abort(400)
    if 'title' in request.json and type(request.json['title']) != unicode:
        abort(400)
    if 'site' in request.json and type(request.json['site']) is not unicode:
        abort(400)
    my_collection.update_one({'_id': ObjectId( office_id)},{'$set': {'site' :request.json['site'], 'title' : request.json['title']}})
    return jsonify({'result': True})

@app.route('/offices/<string:office_id>', methods=['DELETE'])
def delete_office(office_id):
    if len(office_id) == 0:
        abort(404)
    my_collection.delete_one({'_id': ObjectId( office_id)})
    return jsonify({'result': True})

@app.route('/offices', methods=['POST'])
def create_office():
    if not request.json or not 'title' in request.json:
        abort(400)
    office = {
        'title': request.json['title'],
        'site': request.json.get('site', ""),
        'description': request.json.get('description', "")
    }
    my_collection.insert_one(office).inserted_id
    return jsonify({'office':str( office['_id'])}), 201

@app.route('/offices', methods=['GET'])
def get_offices():
    output = []
    for s in my_collection.find():
        output.append({'id' : str(s['_id']), 'site' : s['site'], 'title' : s['title'] , 'description' : s['description']})
    return jsonify({'offices': output })

@app.route('/offices/<string:office_id>', methods=['GET'])
def get_office(office_id):
    document = my_collection.find_one({'_id': ObjectId(office_id)})
    output = {'id' : str(document['_id']),'name' : document['site'], 'title' : document['title']}
    return jsonify({'office': output})

@app.route('/offices/geodata', methods=['POST'])
def add_location():
    location = {
        'address' : request.json['location_address'],
        'latitude' : request.json['latitude'],
        'longitude' : request.json['longitude'],
        'datetime' : datetime.utcnow()
    }
    my_collection = mydb.address
    my_collection.insert_one(location).inserted_id
    return jsonify({'location':str(location['_id'])}), 201

@app.route('/schoolsbylocation/<float:latitude>/<float:longitude>/<string:tetab>', methods=['GET'])
def get_addresses(latitude,longitude,tetab):
    if not latitude or not longitude or not tetab:
        abort(404)
    id_projet = gettetab(tetab)
    if id_projet == None:
        abort(404)
    mydb = client.opendata
    my_collection = mydb.adresses
    output = []
    address = my_collection.find_one({'fields.geom' : { '$near' :{ '$geometry' :
                   { 'type' : "Point",  'coordinates': [longitude ,latitude ] },'$minDistance': 0,'$maxDistance':1000 } } } )

    longitude = address['fields']['geom']['coordinates'][0]
    latitude = address['fields']['geom']['coordinates'][1]
    school =  get_schools(latitude,longitude,id_projet)
    school[0]['origadress'] = address["fields"]["l_adr"] + ',' + formatzipcode( address["fields"]["c_ar"])
    return jsonify({'schools': school})

@app.route('/schoolsbystring/<string:adress>/<string:tetab>', methods=['GET'])
def get_schoolsbystring(adress,tetab):
    if not adress or not tetab:
        abort(404)  
    id_projet = gettetab(tetab)
    if id_projet == None:
        abort(404)
    app.logger.info('Lancementestt du script')

    mydb = client.opendata
    my_collection = mydb.adresses
    app.logger.info('adressevant: ' + adress)
    cursor = my_collection.find({"$text": {"$search": adress}},{"score": {"$meta": "textScore"}})
    cursor.sort([('score', {'$meta': 'textScore'})]).limit(1)
    adress_x_y = cursor.next()
    if adress_x_y:
         app.logger.info('adresseapres: ' +  adress_x_y["fields"]["l_adr"])
         results = get_schools( adress_x_y['fields']['geom_x_y'][0], adress_x_y['fields']['geom_x_y'][1],id_projet) 
         results[0]['origadress'] = adress_x_y["fields"]["l_adr"] + ',' + formatzipcode( adress_x_y["fields"]["c_ar"])
         return jsonify({'schools' : results } )

@app.route('/schools/<float:latitude>/<float:longitude>/<string:tetab>', methods=['GET'])
def get_schools(latitude,longitude,tetab):
    if not latitude or not longitude or not tetab:
        abort(404)
    mydb = client.opendata
    my_collection = mydb.secteursescolaires
    output = []
    for s in my_collection.find(
    { '$and': [
    { "fields.geo_shape" : {
                          "$geoIntersects": { 
                                          "$geometry" : {
                                                       "type": "Point", "coordinates" : [  longitude , latitude ]
                                                      }
                                         }
                          }    
    },{"fields.annee_scol" : "2017-2018"}, {"fields.id_projet" : tetab }]} , { "fields.id_projet":1,"fields.lib_etab_1": 1, "fields.adr_etab_1":1 ,"geometry" : 1 , "_id":0 }): 
        output.append({'libelle' :  s["fields"]['lib_etab_1'], 'adresse' : s["fields"]['adr_etab_1'],'latitude': s['geometry']['coordinates'][0], 'longitude' : s['geometry']['coordinates'][1]})
    if len(output) > 0:
         return output
    else:
        return jsonify({'schools' : "No results" } )
@app.route('/adresslist/<string:searchtext>', methods=['GET'])
def get_adresslist(searchtext):
    if not searchtext:
        abort(404)
    mydb = client.opendata
    my_collection = mydb.adresses
    output = []
    searchtext = prepareaddress2(searchtext)
    app.logger.info('prepared text: ' + searchtext)

    #for s in my_collection.find({
    #                          "$text": {"$search": searchtext}
    #                           },
    #                            {
    #                             "score": {"$meta": "textScore"}}
    #                             ).sort({"score":{"$meta":"textScore"}})):
    cursor = my_collection.find({"$text": {"$search": searchtext}},{"score": {"$meta": "textScore"}})
    cursor.sort([('score', {'$meta': 'textScore'})]).limit(10)
    for s in cursor:
        myadress = s["fields"]['l_adr'] + ',' + formatzipcode(s["fields"]['c_ar']) 
        output.append({'adresse': myadress ,'score' : s["score"]})
    return jsonify({'adresslist' : output } )
def formatzipcode(c_ar):
    if c_ar >= 10:
        return '750' + str(c_ar)
    else:
        return '7500' + str(c_ar)

def prepareaddress(sadress):
    sadress = sadress.upper()
    sprepared = sadress.replace(' AVENUE ',' AV ',1)
    sprepared = sadress.replace(' BOULEVARD ',' BD ',1)
    sprepared = sadress.replace(' SQUARE ',' SQ ',1)
    sprepared = sadress.replace(' ALLEE ',' ALL ',1)
    return sprepared

def gettetab(tetab):
    if tetab == 'MATERNELLES':
        id_projet = 'MATERNELLES (version 2017/2018)'
    elif tetab == 'ELEMENTAIRES':
        id_projet = 'ELEMENTAIRES (version 2017/2018)'
    elif tetab == 'COLLEGES':
        id_projet = 'COLLEGES (version 2017/2018)'
    return id_projet 

def prepareaddress2(adress):
    uadress = adress.upper()
    
    app.logger.info('uadress :' + uadress)
    regex = re.compile(r"(^\"\d+\s)(?P<id>AVENUE\b|AVENU\b|AVEN\b|AVE\b)(.+)?(\"$)")
    #regex = re.compile(r"(\d+)")
    result = re.match(regex,uadress)
    if result != None:
        app.logger.info('find')
        if result.group(1):
            newadress = result.group(1) + 'AV'
        if result.group(3):
            newadress += result.group(3)
        if result.group(4):
            newadress += result.group(4)
        #newadress = regex.sub(r"\1AV\3\4", uadress)
        return newadress
    else:
        app.logger.info('not find')
        regex = re.compile(r"(^\")(?P<id>AVENUE|AVENU|AVEN|AVE)(.+)?(\")")
        result = re.match(regex,uadress)
        if result:
            app.logger.info('find2')
            if result.group(1):
                newadress = result.group(1)+ 'AV'
            if result.group(2):
                newadress += result.group(3)
            if result.group(4):
                 newadress += result.group(4)
            #newadress = regex.sub(r"AV",uadress)
            return newadress
        #ce n'est pas une avenue
        else:
            app.logger.info('not avenue')
            regex = re.compile(r"(^\")(\d\s)?(?P<id>BOULEVARD|BOULEVAR|BOULEVA|BOULEV|BOULE|BOUL)(.+)?(\")")
            result = re.match(regex,uadress)
            if result:
                 app.logger.info('findBoulevard')
                 if result.group(1):
                     newadress = result.group(1) 
                 if result.group(2):
                     newadress += result.group(2)
                 if result.group(3):
                      newadress += 'BD'
                 if result.group(4):
                     newadress += result.group(4)
                 if result.group(5):
                     newadress += result.group(5)
                 return newadress
            else:
                app.logger.info('not boulevard')
                regex = re.compile(r"(^\")(\d\s)?(?P<id>SQUARE|SQUAR|SQUA|SQU)(.+)?(\")")
                result = re.match(regex,uadress)
                if result:
                     app.logger.info('findSquere')
                     if result.group(1):
                         newadress = result.group(1)
                     if result.group(2):
                         newadress += result.group(2)
                     if result.group(3):
                         newadress += 'SQ'
                     if result.group(4):
                         newadress += result.group(4)
                     if result.group(5):
                         newadress += result.group(5)
                     return newadress
                else:
                    app.logger.info('not square')
                    regex = re.compile(r"(^\")(\d\s)?(?P<id>ALLEE|ALLE)(.+)?(\")")
                    if result:
                        app.logger.info('findAllee')
                        if result.group(1):
                            newadress = result.group(1)
                        if result.group(2):
                            newadress += result.group(2)
                        if result.group(3):
                            newadress += 'SQ'
                        if result.group(4):
                             newadress += result.group(4)
                        if result.group(5):
                             newadress += result.group(5)
                        return newadress
                    else:
                        app.logger.info('not allee') 


         
    app.logger.info('not find2')
    return uadress


@app.route('/')
def index():
    return "Hello, World!"

if __name__ == '__main__':
    #app.run(debug=True, host="0.0.0.0" , port=8080)
    app.run()
