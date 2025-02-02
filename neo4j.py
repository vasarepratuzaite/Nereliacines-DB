import werkzeug
from flask import (Flask, request, jsonify, abort)
from py2neo import Graph

graph = Graph("bolt://localhost:7687", auth=("neo4j", "newpassword"))

def create_app():
    app = Flask(__name__)

    # REGISTER A NEW CITY
    @app.route('/cities', methods=['PUT'])
    def register_city():
        req = request.get_json()
        name = req.get("name")
        country = req.get("country")

        if not name or not country:
            return jsonify({"message": "Could not register the city. Mandatory attributes are missing"}), 400
        
        # PAtikriname, ar miestas jau egzistuoja
        query_check = """
        MATCH (c:City {name: $name, country: $country})
        RETURN c
        """
        exists = graph.run(query_check, name=name, country=country).data()

        if exists:
            return jsonify({"message": "Could not register the city, it already ezists"}), 400

        query_create = """
        CREATE (c:City {name: $name, country: $country})
        RETURN c
        """
        graph.run(query_create, name=name, country=country)

        return jsonify({"message": "City registered succesfully"}), 204


    # GET CITIES
    # Get all cities in the system. Can be filtered by country
    @app.route('/cities', methods=['GET'])
    def get_cities():
        country = request.args.get("country")

        # Jei nurodyta konkreti salis, rodome tos salies miestus
        if country:
            query = """
            MATCH (c:City {country: $country})
            RETURN c.name AS name, c.country AS country
            """
            cities = graph.run(query, country=country).data()

        else:
            query = """
            MATCH (c:City)
            RETURN c.name AS name, c.country AS country
            """
            cities = graph.run(query).data()

        return jsonify(cities), 200


    # GET CITY
    @app.route('/cities/<name>', methods=['GET'])
    def get_city(name):
        # Patikriname, ar miestas egzistuoja
        query_check = """
        MATCH (c:City {name: $name})
        RETURN c.name AS name, c.country AS country"""

        result = graph.run(query_check, name=name).data()

        # Jei nera rezultato, graziname klaidos pranesima
        if not result:
            return jsonify({"message": "City not found"}), 404
        
        # Jei miestas rastas, graziname informacija apie ji
        return jsonify(result[0]), 200


    # REGISTER AN AIRPORT
    @app.route('/cities/<name>/airports', methods=['PUT'])
    def register_airport(name):
        req = request.get_json()
        code = req.get("code")
        airport_name = req.get("name")
        number_of_terminals = req.get("numberOfTerminals")
        address = req.get("address")

        # Patikriname, ar visi butini parametrai yra pateikti
        if not code or not airport_name or not number_of_terminals or not address:
            return jsonify({"message": "Airport could not be created due to missing data"}), 400

        # Patrikriname, ar nurodytas miestas egzistuoja
        city_query = """
        MATCH (c:City {name: $name})
        RETURN c
        """
        city = graph.run(city_query, name=name).data()

        if not city:
            return jsonify({"message": "City not found"}), 404

        # Patikriname, ar oro uostas su tokiu kodu jau egzistuoja
        airport_query = """
        MATCH (a:Airport {code: $code})
        RETURN a
        """

        airport_exists = graph.run(airport_query, code=code).data()

        if airport_exists:
            return jsonify({"message": "Could not register the airport, it already exists"}), 400

        # Sukuriame nauja oro uosta ir susiejame ji su miestu
        create_airport_query = """
        MATCH (c:City {name: $name})
        CREATE (a:Airport {code: $code, name: $airport_name, numberOfTerminals: $number_of_terminals, address: $address})
        CREATE (c)-[:HAS_AIRPORT]->(a)
        RETURN a
        """
        graph.run(create_airport_query, name=name, code=code, airport_name=airport_name, number_of_terminals=number_of_terminals, address=address)

        return jsonify({"message": "Airport created"}), 204

    # GET AIRPORTS IN A CITY
    @app.route('/cities/<name>/airports', methods=['GET'])
    def get_airports_in_a_city(name):
        # Patikriname, ar nurodytas miestas egzistuoja
        city_query = """
        MATCH (c:City {name: $name})
        RETURN c
        """

        city = graph.run(city_query, name=name).data()

        if not city:
            return jsonify({"message": "City not found"}), 404

        # Gauti visus oro uostus, susijusius su miestu
        airports_query = """
        MATCH (c:City {name: $name})-[:HAS_AIRPORT]->(a:Airport)
        RETURN a.code AS code, a.name AS name, a.numberOfTerminals AS numberOfTerminals, a.address AS address
        """
        airpots = graph.run(airports_query, name=name).data()

        if not airpots:
            return jsonify({"message": "No airports found in the city"}), 404

        return jsonify(airpots), 200


    # GET AIRPORT
    @app.route('/airports/<code>', methods=['GET'])
    def get_airport(code):
        # Patikriname, ar oro uostas su tokiu kodu egzistuoja ir susijusiu miestu
        query = """
        MATCH (a:Airport {code: $code})<-[:HAS_AIRPORT]-(c:City)
        RETURN a.code AS code, a.name AS name, a.numberOfTerminals AS numberOfTerminals, a.address AS address, c.name AS city_name
        """
        result = graph.run(query, code=code).data()

        if not result:
            return jsonify({"message": "Airport not found"}), 404
        
        airport = result[0]  # Paimame pirma elementa, nes result yra sarasas

        return jsonify({
            "code": airport["code"],
            "city": airport["city_name"],
            "name": airport["name"],
            "numberOfTerminals": airport["numberOfTerminals"],
            "address": airport["address"]
        }), 200

    # REGISTER NEW FLIGHT
    # Register a new flight between two cities. Flights are directional. Meaning that if there is a flight
    # from Vilnius to Kaunas, it does not imply the flight from Kaunas to Vilnius
    @app.route('/flights', methods=['PUT'])
    def register_new_flight():
        req = request.get_json()
        number = req.get("number")
        fromAirport = req.get("fromAirport")
        toAirport = req.get("toAirport")
        price = req.get("price")
        flightTimeInMinutes = req.get("flightTimeInMinutes")
        operator = req.get("operator")

        if not number or not fromAirport or not toAirport or not price or not flightTimeInMinutes or not operator:
            return jsonify({"message": "Flight could not be created due to missing data"}), 400

        # Patikriname, ar abu oro uostai egzistuoja
        from_airport_query = """
        MATCH (a:Airport {code: $fromAirport})
        RETURN a
        """
        from_airport_exists = graph.run(from_airport_query, fromAirport=fromAirport).data()

        to_airport_query = """
        MATCH (a:Airport {code: $toAirport})
        RETURN a
        """
        to_airport_exists = graph.run(to_airport_query, toAirport=toAirport).data()

        if not from_airport_exists or not to_airport_exists:
            return jsonify({"message": "One or both airports not found"}), 404

        # Sukuriame skrydi ir siejame ji su abiem oro uostais
        fligth_query = """
        MATCH (from:Airport {code: $fromAirport}), (to:Airport {code: $toAirport})
        CREATE (f:Flight {number: $number, price: $price, flightTimeInMinutes: $flightTimeInMinutes, operator: $operator})
        CREATE (from)-[:HAS_FLIGHT]->(f)-[:GOES_TO]->(to)
        RETURN f
        """
        graph.run(fligth_query, fromAirport=fromAirport, toAirport=toAirport, number=number, price=price, flightTimeInMinutes=flightTimeInMinutes, operator=operator)

        return jsonify({"message": "Flight created"}), 204

    # GET FULL FLIGHT INFORMATION
    @app.route('/flights/<number>', methods=['GET'])
    def get_full_flight_info(number):
        # Patikriname, ar skrydis egzistuoja
        query = """
        MATCH (f:Flight {number: $number})<-[:HAS_FLIGHT]-(from:Airport)<-[:HAS_AIRPORT]-(from_city:City),
              (f)-[:GOES_TO]->(to:Airport)<-[:HAS_AIRPORT]-(to_city:City)
        RETURN f.number AS number, f.price AS price, f.flightTimeInMinutes AS flightTimeInMinutes, f.operator AS operator,
               from.code AS from_airport_code, from_city.name AS from_city_name, to.code AS to_airport_code, to_city.name AS to_city_name
        """
        result = graph.run(query, number=number).data()

        if not result:
            return jsonify({"message": "Flight not found"}), 404
        
        flight_info = result[0] # Paimame tik pirma rezultata, nes jis yra tik vienas

        return jsonify({
            "number": flight_info["number"],
            "fromAirport": flight_info["from_airport_code"],
            "fromCity": flight_info["from_city_name"],
            "toAirport": flight_info["to_airport_code"],
            "toCity": flight_info["to_city_name"],
            "price": flight_info["price"],
            "flightTimeInMinutes": flight_info["flightTimeInMinutes"],
            "operator": flight_info["operator"]
        }), 200
    
    # FIND FLIGHTS TO AND FROM CITY
    # Find flights between two cities. Will not search for flights with more than 3 stops
    @app.route('/search/flights/<fromCity>/<toCity>', methods=['GET'])
    def find_flights(fromCity, toCity):
        # Patikriname, ar abu miestai egzistuoja
        city_query = """
        MATCH (from_city:City {name: $fromCity}), (to_city:City {name: $toCity})
        RETURN from_city, to_city
        """
        cities = graph.run(city_query, fromCity=fromCity, toCity=toCity).data()

        if not cities:
            return jsonify({"message": "One or both cities not found"}), 404
        
        # Rasti oro uostus susijusius su miestais
        airports_query = """
        MATCH (from_city:City {name: $fromCity})-[:HAS_AIRPORT]->(from:Airport),
              (to_city:City {name: $toCity})-[:HAS_AIRPORT]->(to:Airport)
        RETURN from.code AS from_airport_code, to.code AS to_airport_code
        """
        airports = graph.run(airports_query, fromCity=fromCity, toCity=toCity).data()

        if not airports:
            return jsonify({"message": "No airports found in one or both cities"}), 404
        
        all_flights = []
        
        # Ieskome skrydziu tarp siu oro uostu su ne daugiau kaip 3 sustojimais
       # for airport in airports:
        flight_query = """
        MATCH (from_city:City {name: $fromCity})-[:HAS_AIRPORT]->(from:Airport),
            (to_city:City {name: $toCity})-[:HAS_AIRPORT]->(to:Airport)
        MATCH path = (from)-[:HAS_FLIGHT*1..3]->(to)
        WITH path, nodes(path) AS airports_in_path
        WHERE SIZE(airports_in_path) - 2 <= 3
        UNWIND nodes(path) AS airport_node
        MATCH (f:Flight)-[:GOES_TO]->(to)
        WHERE airport_node = "from" OR airport_node = "to"
        RETURN f.number AS flight_number, SUM(f.price) AS price, SUM(f.flightTimeInMinutes) AS flightTimeInMinutes,
            SIZE(airports_in_path) - 2 AS stop_count
        """
            #from_airport_code = airport['from_airport_code']
            #to_airport_code = airport['to_airport_code']

        flights = graph.run(flight_query, fromCity=fromCity, toCity=toCity). data()

        all_flights.extend(flights)
        
        if not all_flights:
            return jsonify({"message": "Flights not faund"}), 404
        
        return jsonify(all_flights), 200


    # CLEANUP
    @app.route('/cleanup', methods=['POST'])
    def cleanup():
        try:
            # Triname visus mazgus ir rysius
            query = """
            MATCH (n) DETACH DELETE n
            """
            graph.run(query)

            return jsonify ({"message": "Cleanup successful"}), 200
        except Exception as e:
            return jsonify({"message": f"Cleanup faild: {str(e)}"}), 500

    return app

