import os
from flask import Flask, request, jsonify, abort, send_from_directory
from flask_cors import CORS
import json
import pymongo
import redis
from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png'}

def create_app():
    app = Flask(__name__)
    CORS(app) # Leidzia kirsti narsukles uzklausas (naudinga Frontend)
    app.config['UPLOAD_FOLDER'] = './uploads' # Katalogas paveiksleliams saugoti

    # Sukuriame kataloga, jei jo nera
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

    # Redis ir MongoDB konfiguracija
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017')

    db = mongo_client["food_erdering"]
    collection_restaurants = db["restaurants"]
    collection_customers = db["customers"]
    collection_counters = db["counters"] # New collection for counters

    #collection_restaurants.drop_index("menu.name_text_menu.description_text")

    # Indeksas pilno teksto paieskai
    collection_restaurants.create_index([("name", "text"), ("menu.name", "text"), ("menu.description", "text")], name="combined_search_index")
    # Indeksai spartesnei rekomendaciju paieskai
    collection_restaurants.create_index("menu._id")
    collection_customers.create_index("orders.items.menu_item_id")

    # Initialize the counters collection if it doesn't exist
    def initialize_counters():
        if not collection_counters.find_one({"_id": "restaurant_id"}):
            collection_counters.insert_one({"_id": "restaurant_id", "seq": 0})
        if not collection_counters.find_one({"_id": "customer_id"}):
            collection_counters.insert_one({"_id": "customer_id", "seq": 0})
        if not collection_counters.find_one({"_id": "order_id"}):
            collection_counters.insert_one({"_id": "order_id", "seq": 0})
        if not collection_counters.find_one({"_id": "menu_id"}):
            collection_counters.insert_one({"_id": "menu_id", "seq": 0})

    initialize_counters()

    # Function to get the next sequence value
    def get_next_sequence(counter_id):
        sequence = collection_counters.find_one_and_update(
            {"_id": counter_id},
            {"$inc": {"seq": 1}},
            upsert = True,
            return_document=pymongo.ReturnDocument.AFTER
        )
        return str(sequence["seq"]) # Return as a string

    # === PAVEIKSLELIO IKELIMAS ===
    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    @app.route('/upload-image', methods=['POST'])
    def upload_image():
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if not allowed_file(file.filename):
            return jsonify({"error": "Invalid file type. Only .jpg, /jpeg, ad .png files are allowed"}), 400
        
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)  # Ikeliamas paveikslelis i serveri

            # Graziname URL i paveiksleli
            image_url = f"http://localhost:8080/uploads/{filename}"
            return jsonify({"message": "Image uploaded successfully!", "image_url": image_url}), 201

    # ===== RESTORANAI =====
    @app.route('/restaurants', methods=['PUT'])
    def register_reastaurant():
        req = request.get_json()

        if 'name' not in req or 'address' not in req or 'working_hours' not in req:
            return jsonify({"message": "Invalid input. Mandatory attributes are missing "}), 400
        
        id = get_next_sequence("restaurant_id")  # Generate a new unique restaurant ID

        restaurant = {
            "_id": id,
            "name": req["name"],
            "address": req["address"],
            "working_hours": req["working_hours"],
            "menu": []
        }

        collection_restaurants.insert_one(restaurant)
        return jsonify({"message": "Restaurant registered successfully!", "id": id}), 201
    
    @app.route('/restaurants', methods=['GET'])
    def get_restaurants():
        # Fetch restaurants from the collection
        restaurants = list(collection_restaurants.find({}))
        if restaurants:
            # Return the JSON response
            return jsonify(restaurants), 200
        
        return jsonify({"message": "No restaurants were found"}), 404
    
    @app.route('/restaurants/<restaurantId>', methods=['DELETE'])
    def del_restaurant(restaurantId):
        restaurant = collection_restaurants.find_one({"_id": restaurantId})

        if restaurant:
            collection_restaurants.delete_one({"_id": restaurantId})
            return jsonify({"message": "Restaurant deleted"}), 204
        else:
            return jsonify({"message": "Restaurant not found"}), 404

    # ==== MENIU ====
    @app.route('/restaurants/<restaurantId>/menu', methods=['PUT'])
    def add_menu_item(restaurantId):
        req = request.get_json()

        if 'name' not in req or 'description' not in req or 'price' not in req:
            return jsonify({"message": "Invalid input, mising name, description or price"}), 400
        elif not isinstance(req['price'], (int, float)) or req['price'] <= 0:
            return jsonify({"message": "Price must be a positive number"}), 400

        menu_item_id = get_next_sequence("menu_id")

        menu_item = {
            "_id": menu_item_id,
            "name": req["name"],
            "description": req["description"],
            "price": req["price"],
            "image_url": req["image_url"]   # Cia saugome paveikslelio URL
        }

        collection_restaurants.update_one(
            {"_id": restaurantId},
            {"$push": {"menu": menu_item}}
        )
        return jsonify({"message": "Menu item added successfully!"}), 201
    
    @app.route('/restaurants/<restaurantId>/menu', methods=['GET'])
    def get_menu(restaurantId):

        restaurant = collection_restaurants.find_one({"_id": restaurantId})
        if not restaurant:
            return jsonify({"message": "Restaurant not found"}), 404
        elif not restaurant["menu"]:
            return jsonify({"message": "Menu not found"}), 404
        
        return jsonify(restaurant["menu"]), 200
    
    # ==== STATINIS PAVEIKSLELIO PATEIKIMAS ====
    @app.route('/uploads/<filename>', methods=['GET'])
    def serve_image(filename):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)
    
    # ==== KLIENTAI ====
    @app.route('/customers', methods=['PUT'])
    def register_customer():
        req = request.get_json()

        if 'first_name' not in req or 'last_name' not in req or 'phone_number' not in req:
            return jsonify({"message": "Invalid input. Mandatory attributes are missing"}), 400
        
        id = get_next_sequence("customer_id")

        customer = {
            "_id": id,
            "first_name": req["first_name"],
            "last_name": req["last_name"],
            "phone_number": req["phone_number"],
            "orders": []
        }
        collection_customers.insert_one(customer)
        return jsonify(customer), 200
    
    @app.route('/customers', methods=['GET'])
    def get_customers():
        # Fetch customers from the collection
        customers = list(collection_customers.find({}))
        if customers:
            # Return the JSON response
            return jsonify(customers), 200
        
        return jsonify({"message": "No customers were found"}), 404
    
    @app.route('/customers/<customerId>', methods=['DELETE'])
    def del_customer(customerId):
        customer = collection_customers.find_one({"_id": customerId})

        if customer:
            collection_customers.delete_one({"_id": customerId})
            return jsonify({"message": "Customer deleted"}), 204
        else:
            return jsonify({"message": "Customer not found"}), 404

    # ==== UZSAKYMAI ====
    @app.route('/orders/<customerId>', methods=['PUT'])
    def create_order(customerId):
        req = request.get_json()

        # Validate mandatory attributes
        if not req or 'items' not in req or not isinstance(req['items'], list) or not req['items']:
            return jsonify({"message": "Invalid input. 'items' is required and must be a non-empty list"}), 400
        
        if 'order_type' not in req:
            return jsonify({"message": "Invalid input. 'order_type' ir required"}), 400
        
        order_type = req['order_type'].lower()

        # Check for valid order types
        valid_order_types = ['pickup', 'delivery']
        if order_type not in valid_order_types:
            return jsonify({"message": f"Invalid 'order_type. Must be one of {valid_order_types}."}), 400
        
        if order_type == 'delivery':
            if 'address' not in req or not req["address"]:
                return jsonify({"message": "Invalid input. 'address' is required for 'delivery' orders."}), 400
            
        else:
            # If 'pickup', address should not be required
            req["address"] = None # Default to None for clarity in storage

        for item in req['items']:
            if 'restaurant_id' not in item or 'menu_item_id' not in item:
                return jsonify({"message": "Each item must include 'restaurant_id' and 'menu_item_id'"}), 400

            if 'quantity' not in item or not isinstance(item['quantity'], int) or item['quantity'] <= 0:
                return jsonify({"message": "Each item must include valid 'quantity' greaten than 0"}), 400

            restaurant = collection_restaurants.find_one({"_id": item['restaurant_id']})
            if not restaurant:
                return jsonify({"message": f"Restaurant with ID {item['restaurant_id']} not found"}), 404

            menu_item_exists = any(menu_item["_id"] == item['menu_item_id'] for menu_item in restaurant["menu"])
            if not menu_item_exists:
                return jsonify({
                    "message": f"Menu item ID {item['menu_item_id']} does not exist in restaurant {item['restaurant_id']}"
                }), 400

        id = get_next_sequence("order_id")
        
        # Build the order object
        order = {
            "_id": id,
            "items": req["items"],
            "order_type": order_type,
            "address": req["address"]
        }

        # Insert the order into the database
        result = collection_customers.update_one(
            {"_id": customerId},
            {"$push": {"orders": order}}
        )
        
        if result.matched_count == 0:
            return jsonify({"message": "Customer not found"}), 404

        # Isvalome Redis cache siam klientui
        redis_key = f"recommendations:{customerId}"
        redis_client.delete(redis_key)

        # Return success message with the order ID
        return jsonify({"message": "Order created successfully!", "order_id": id}), 201

    @app.route('/customers/<customerId>/orders', methods=['GET'])
    def get_order(customerId):
        # Fetch orders for the specified customer ID
        customer = collection_customers.find_one({"_id": customerId})

        if not customer:
            return jsonify({"message": "Customer not found"}), 404
        elif not customer["orders"]:
            return jsonify({"message": "No orders were found for this customer"}), 404

        enriched_orders = []
        for order in customer.get("orders", []):
            enriched_items = []
            total_price = 0
            for item in order["items"]:
                # Surandame restorana pagal ID
                restaurant = collection_restaurants.find_one({"_id": item["restaurant_id"]})
                # Surandame patiekala pagal ID
                menu_item = next(
                    (m for m in restaurant["menu"] if m["_id"] == item["menu_item_id"]),
                    None
                )

                # Jei patiekalas rastas, prideti jo kaina prie bendros sumos
                if menu_item and "price" in menu_item:
                    total_price += menu_item["price"] * item["quantity"]

                # Praturtiname elementa pavadinimais
                enriched_items.append({
                    "restaurant_id": item["restaurant_id"],
                    "restaurant_name": restaurant["name"] if restaurant else "Unknown",
                    "menu_item_id": item["menu_item_id"],
                    "menu_item_name": menu_item["name"] if menu_item else "Unknown",
                    "quantity": item["quantity"]
                })

            enriched_orders.append({
                "order_id": order["_id"],
                "items": enriched_items,
                "total_price": total_price,
                "order_type": order["order_type"],
                "address": order.get("address", "N/A")
            })

        # Return the list of orders
        return jsonify(enriched_orders), 200

    # ==== REKOMENDACIJOS ====
    @app.route('/recomendations/<customerId>', methods=['GET'])
    def get_recommendations(customerId):
        try:
            # Redis raktas pagal klienta
            redis_key = f"recommendations:{customerId}"

            # Tirkriname Redis cache
            cached_data = redis_client.get(redis_key)
            if cached_data:
                return jsonify(eval(cached_data)), 200

            # Jei duomenu nera cache, atliekame MongoDB uzklausa
            customer = collection_customers.find_one({"_id": customerId})
            if not customer:
                return jsonify({"message": "Customer not found"}), 404
            
            pipeline = [
                {"$match": {"_id": customerId}},   # Filtruojame pagal klienta
                {"$unwind": "$orders"},            # Kiekviena uzsakyma atskiriame
                {"$unwind": "$orders.items"},      # Isskirstome patiekalus uzsakymuose
                {"$group": {
                    "_id": "$orders.items.menu_item_id",
                    "count": {"$sum": 1}           # Skaiciuojame, kiek kartu patiekalas uzsakytas
                }},
                {"$sort": {"count": -1}},         # Rikiavimas mazejancia tvarka
                {"$limit": 3}                      # Imame tik 3 populiariausius patiekalus
            ]

            popular_dishes = list(collection_customers.aggregate(pipeline))

            # Sukuriame rekomendaciju sarasa su papildoma informacija
            recommendations = []
            for dish in popular_dishes:
                menu_item_id = dish["_id"]
                count = dish["count"]

                # Randame restorana ir patiekala pagal menu_item_id
                restaurant = collection_restaurants.find_one(
                    {"menu._id": menu_item_id}, {"name": 1, "menu.$": 1}
                )

                if restaurant:
                    menu_item = restaurant["menu"][0]
                    recommendations.append({
                        "menu_item_id": menu_item["_id"],
                        "menu_item_name": menu_item["name"],
                        "restaurant_name": restaurant["name"],
                        "popularity": count
                    })
            
            # Paskutinis uzsakymas
            last_order = customer["orders"][-1] if customer["orders"] else None

            if last_order:
                last_order_items = last_order["items"]
                last_order_details = []
                for item in last_order_items:
                    menu_item_id = item["menu_item_id"]

                    # Randame restorana ir patiekala pagal menu_item_id
                    restaurant = collection_restaurants.find_one(
                        {"menu._id": menu_item_id}, {"name": 1, "menu.$": 1}
                    )

                    if restaurant:
                        menu_item = restaurant["menu"][0]
                        last_order_details.append({
                            "menu_item_id": menu_item["_id"],
                            "menu_item_name": menu_item["name"],
                            "restaurant_name": restaurant["name"],
                            "quantity": item["quantity"]
                        })

            last_order = last_order_details

            # Sukuriame rekomendaciju atsakyma
            response = {
                "order_again": last_order,
                "popular_dishes": recommendations
            }

            # Issaugome rezultata Redis (be TTL)
            redis_client.set(redis_key, str(response))

            return jsonify(response), 200
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/search', methods=['GET'])
    def search_menu():
        query = request.args.get('query', '').strip()
        if not query:
            return jsonify({"message": "Query parameter ir required"}), 400

        # Ieskome restoranu ir patiekalu naudojant tekstini indeksa
        search_results = list(
            collection_restaurants.find(
                {"$text": {"$search": query}}, # Naudojame $text operatoriu
                {"score": {"$meta": "textScore"}, "name": 1, "menu": 1, "address": 1, "working_hours": 1}
            ).sort([("score", {"$meta": "textScore"})]) # Rusiuojame pagal tekstinio atitikimo bala
        )

        # Atidalijame rezultatus i restoranus ir patiekalus
        restaurant_results = []
        dish_results = []

        for restaurant in search_results:
            # Pridedame restorana prie rezultatu
            restaurant_results.append({
                "restaurant_id": str(restaurant["_id"]),
                "name": restaurant["name"],
                "address": restaurant.get("address", ""),
                "working_hours": restaurant.get("working_hours", "")
            })

        # Pridedame patiekalus prie rezultatu
        for dish in restaurant.get("menu", []):
            if query.lower() in (dish.get("name", "").lower() + " " + dish.get("description", "").lower()):
                dish_results.append({
                    "restaurant_id": str(restaurant["_id"]),
                    "restaurant_name": restaurant["name"],
                    "dish_id": dish.get("_id"),
                    "dish_name": dish.get("name"),
                    "description": dish.get("description", ""),
                    "price": dish.get("price", 0)
                })

        # Graziname rezultatus kaip strukturuota JSON atsakyma
        return jsonify({
            "restaurants": restaurant_results,
            "dishes": dish_results
        }), 200
        
    @app.route('/cleanup', methods=['POST'])
    def clear_database():
        # Isvalome visus duomenis is kolekciju
        try:
            collection_restaurants.delete_many({})
            collection_customers.delete_many({})
            collection_counters.delete_many({})
            
            # Inicialize counters after cleanup
            initialize_counters()
 
            return jsonify({"message": "Cleanup completed"}), 200
        except Exception as e:
            return jsonify({"message": "An error occurred while clearing the database", "error": str(e)}), 500

    return app

