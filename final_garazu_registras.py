import json
from collections import OrderedDict
import re
import werkzeug
import redis
from flask import (Flask, request, jsonify, abort)
licenseRegex = "^[A-Z0-9]{1,7}$"

def create_app(): 
    app = Flask(__name__)
   
    redisClient = redis.Redis(host='localhost', port=6379, decode_responses=True) 

    def garageKey(garageId):
        return f'Garage:{garageId}'

    def spotKey(garageId, spotNo):
        return str(garageKey(garageId) + ":" + spotNo)
    
    # cia funkcija tikrinimui, ar jau egzistuoja duomenu bazeje objektas su pateikiamu garageId
    def search(garageId):
        return redisClient.exists(garageKey(garageId))

    def validate_license_number(licenseNo):
        if re.match(licenseRegex, licenseNo):
            return True
        else:
            return False

    # UZREGISTRUOTI GARAZA
    @app.route('/garage', methods=['PUT'])
    def register_garage():
        reqBody = request.json
        garageId = str(reqBody.get("id"))
        if search(garageId) is not None:
            
            spotsCount = int(reqBody.get("spots"))
            garageAddress = str(reqBody.get("address"))
            garageData = str(spotsCount) + ":" + garageAddress
            redisClient.set(garageKey(garageId), garageData)

            return { "message": "Garazas sekmingai sukurtas sitemoje"}, 201
        else:
            return { "message": "Registruojant garaza nenurodyta id, spots ar address laukai" }, 400

    # GAUTI GARAZO INFORMACIJA
    @app.route('/garage/<garageId>', methods=['GET'])
    def get_garage_info(garageId):
        if search(garageId):
            dataString = redisClient.get(garageKey(garageId))
            data = dataString.split(':')
    # OrderedDict: Used to return the data in a specific order, starting with id, then spots, and finally address.
    # json.dumps(result): Converts the OrderedDict into a JSON string while preserving the key order.
    # app.response_class: Constructs a custom response using the JSON string. It ensures the mimetype='application/json' so that the response is properly recognized as JSON.
            result = OrderedDict([
                ("id", garageId),
                ("spots", data[0]),
                ("address", data[1])
            ])
            return app.response_class(
                response=json.dumps(result),
                status=200,
                mimetype='application/json'
            )
        else:
            return {"message": "Garazas tokiu ID nerastas"}, 404
    
    # GAUTI BENDRA VIETU SKAICIU GARAZE    
    @app.route('/garage/<garageId>/configuration/spots', methods=['GET'])
    def get_spots(garageId):
        if search(garageId):
            spotsCount = redisClient.get(garageKey(garageId))
            if(spotsCount != None):
                data = spotsCount.split(':')
                return jsonify({"spots": data[0]}), 200
        else:
            return { "message": "Garazas su tokiu ID nerastas"}, 404
    
    # PAKEISTI GARAZO VIETU SKAICIU    
    @app.route('/garage/<garageId>/configuration/spots', methods=['POST'])
    def update_spots(garageId):
        if search(garageId):
            reqBody = request.json
            newSpotsCount = int(reqBody.get("spots"))
            dataString = redisClient.get(garageKey(garageId))
            data = dataString.split(':')

            if (newSpotsCount < 0):
                return { "message": "Pateiktas neteisingas skaicius (vietu skaicius turi buti teigiamas skaicius)"}, 400
            else:
                redisClient.set(garageKey(garageId), str(newSpotsCount) + ':' + data[1])
                return { "message": "Vietu skaicius pakeistas sekmingai"}, 200
        else:
            return { "message": "Garazas tokiu ID nerastas"}, 404

    # UZREGISTRUOTI UZIMTA VIETA GARAZE   cia nereikia spausdinti automobilio numerio
    @app.route('/garage/<garageId>/spots/<spotNo>', methods=['POST'])
    def occupied_spot(garageId, spotNo):
        if search(garageId):
            spotsCount = int(redisClient.get(garageKey(garageId)).split(':')[0])
            if int(spotNo) <= spotsCount and int(spotNo) > 0:
                reqBody = request.json
                licenseNo = reqBody.get("licenseNo")
                if validate_license_number(licenseNo):
                    redisClient.set(spotKey(garageId, spotNo), licenseNo)
                    print(spotKey(garageId, spotNo))
                    return {"message": "Vieta uzregistruotas sekmingai"}, 200
                else:
                    return { "message": "Neteisingas automobilio numeris"}, 404
            else:
                return { "message": "Vieta nerasta"}, 404
        else:
            return { "message": "Garazas nerastas"}, 404

    # GAUTI AUTOMOBILIO NUMERI, KURIS UZIMA VIETA
    @app.route('/garage/<garageId>/spots/<spotNo>', methods=['GET'])
    def get_license(garageId, spotNo):
        if search(garageId):
            spotsCount = int(redisClient.get(garageKey(garageId)).split(':')[0])
            if int(spotNo) <= spotsCount and int(spotNo) > 0:
                licenseNo = redisClient.get(spotKey(garageId, spotNo))
                if licenseNo: 
                    return jsonify({"license": licenseNo}), 200
                else:
                    return { "message": "Vieta laisva"}, 201 
            else:
                return {"message": "Tokios vietos garaze nera"}, 404
        else:
            return { "message": "Tokio garazo nera"}, 404

    # PAZYMETI VIETA KAIP LAISVA
    @app.route('/garage/<garageId>/spots/<spotNo>', methods=['DELETE'])
    def delete_spot(garageId, spotNo):
        if search(garageId):
            spotsCount = int(redisClient.get(garageKey(garageId)).split(':')[0])
            if int(spotNo) <= spotsCount and int(spotNo) > 0:
                licenseNo = redisClient.get(spotKey(garageId, spotNo))
                if licenseNo:
                    redisClient.delete(spotKey(garageId, spotNo))
                    return { "message": "Vieta atlaisvinta sekmingai"}, 200
                else:
                    return { "message": "Vieta buvo laisva"}, 400
            else:
                return {"message": "Tokios vietos garaze nera"}, 400
        else:
            return { "message": "Garazas nerastas"}, 404

    #GAUTI LAISVU IR UZIMTU VIETU SKAICIU GARAZE
    @app.route('/garage/<garageId>/status', methods=['GET'])
    def get_spots_info(garageId):
        if search(garageId):
            totalSpots = int(redisClient.get(garageKey(garageId)).split(':')[0])
            occupiedSpotsKeys = redisClient.scan_iter(f"{garageKey(garageId)}:*")
    # scan_iter(): This is a Redis method that iterates through keys matching a specific pattern (in this case, all occupied spots in the garage).
            occupiedSpots = len(list(occupiedSpotsKeys))
            freeSpots = totalSpots - occupiedSpots
            return jsonify({"freeSpots": freeSpots, "occupiedSpots": occupiedSpots}), 200
        else:
            return { "message": "Garazas tokiu ID nerastas"}, 400

    return app

