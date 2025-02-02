import pymongo
import werkzeug
from flask import (Flask, request, jsonify, abort)
from collections import OrderedDict


def create_app():
    app = Flask(__name__)

    client = pymongo.MongoClient('mongodb://localhost:27017')

    db = client["warehouse_database"]
    collection_warehouses = db["warehouses"]
    collection_products = db["products"]
    collection_counters = db["counters"] # New collection for counters

    # Initialize the counters collection if it doesn't exist
    def initialize_counters():
        if not collection_counters.find_one({"_id": "warehouse_id"}):
            collection_counters.insert_one({"_id": "warehouse_id", "seq": 0})
        if not collection_counters.find_one({"_id": "inventory_id"}):
            collection_counters.insert_one({"_id": "inventory_id", "seq": 0})
        if not collection_counters.find_one({"_id": "product_id"}):
            collection_counters.insert_one({"_id": "product_id", "seq": 0})

    initialize_counters()

    # Function to get the next sequence value unique id
    def get_next_sequence(counter_id):
        sequence = collection_counters.find_one_and_update(
            {"_id": counter_id},
            {"$inc": {"seq": 1}},
            upsert = True, 
            return_document=pymongo.ReturnDocument.AFTER
        )
        return str(sequence["seq"]) # Return as a string
    
    # Register a new product
    @app.route('/products', methods=['PUT'])
    def register_product():
        req = request.get_json()

        # or 'category' not in req

        if 'name' not in req or 'price' not in req:
            return jsonify({"message": "Invalid input, missing name or price"}), 400
        elif not isinstance(req['price'], (int, float)) or req['price'] <= 0:
            return jsonify({"message": "Price must be a positive number"}), 400
        elif 'id' not in req:
             
            id = get_next_sequence("product_id")  # Generate a new unique product ID

            product = {
                "_id": id,
                "name": req["name"],
                "price": req["price"],
                "category": req["category"]
            }
            collection_products.insert_one(product)
            return jsonify({"message": "Product registered", "id": id}), 201
        else:
            existing_product = collection_products.find_one({"_id": str(req['id'])})
            if existing_product:
                return jsonify({"message": "Product ID already exists"}), 400
            product = {
                "_id": str(req["id"]),
                "name": req["name"],
                "price": req["price"],
                "category": req['category']
            }
            collection_products.insert_one(product)
            return jsonify({"message": "Product registered", "id": str(req['id'])}), 201

    # List all products, optionally in a category
    @app.route('/products', methods=['GET'])
    def get_products_by_category():
        category = request.args.get('category') # Gauti kategorija is uzklausos parametru

        # Jei kategorija nurodyta, grazinti produktus pagal kategorija
        if category:
            products = list(collection_products.find({"category": category}))
        #  Jei kategorija nera nurodyta, grazinti visus produktus
        else:
            products = list(collection_products.find({}))

        formatted_products = []
        for product in products:
            formatted_product = OrderedDict([
            ("id", str(product["_id"])),  # Pirmas
            ("name", product["name"]),
            ("category", product["category"]),
            ("price", product["price"])  # Paskutinis
        ])
            formatted_products.append(formatted_product)
     
        return jsonify(formatted_products), 200

    # Get product details    
    @app.route('/products/<productId>', methods=['GET'])
    def get_product_details(productId):
        # Ieskome produkto pagal pateikta ID
        product = collection_products.find_one({"_id": productId})
        # Jei produktas rastas, graziname informacija apie ji
        if product:
            # Pasirenkame tik reikiamus laukus
            data = {
                "id": product["_id"],
                "name": product["name"],
                "price": product["price"]
                #"category": product["category"]
            }
            return jsonify(data), 200
        else:
            return jsonify({"message": "Product not found"}), 404

    # Delete product
    @app.route('/products/<productId>', methods=['DELETE'])
    def delete_product(productId):
        product = collection_products.find_one({"_id": productId})
        if product:
            result = collection_products.delete_one({"_id": productId})
            if result.deleted_count > 0:
                return jsonify({"message": "Product deleted"}), 204
            else:
                return jsonify({"message": "Product not fount"}), 404
            

    # Register a new warehouse
    @app.route('/warehouses', methods=['PUT'])
    def register_warehouse():
        req = request.get_json()

        if 'name' not in req or 'location' not in req or 'capacity' not in req:
            return jsonify({"message": "Invalid input, missing name, location or capacity"}), 400
        else:
            if not isinstance(req['capacity'], (int, float)) or req['capacity'] <= 0:
                return jsonify({"message": "Capacity must be positive number"}), 400
            else:
                warehouse_id = get_next_sequence("warehouse_id") # Generate new ID
                warehouse = {
                    "_id": warehouse_id,
                    "name": req["name"],
                    "location": req["location"],
                    "capacity": req["capacity"],
                    "inventory": []  # Inicializuojame inventoriu, kaip tuscia sarasa
                }
                collection_warehouses.insert_one(warehouse)
                return jsonify({"message": "Warehouse registered", "id": warehouse_id}), 201

    # Get warehouse details
    @app.route('/warehouses/<warehouseId>', methods=['GET'])
    def get_warehouse_details(warehouseId):
        warehouse = collection_warehouses.find_one({"_id": warehouseId})
        if warehouse:
            data = {
                "id": str(warehouse["_id"]),
                "name": warehouse["name"],
                "location": warehouse["location"],
                "capacity": warehouse["capacity"]
            }
            return jsonify(data), 200
        else:
            return jsonify({"message": "Warehouse not found"}), 404

    # Delete warehouse and associated inventory
    @app.route('/warehouses/<warehouseId>', methods=['DELETE'])
    def delete_warehouse(warehouseId):
        # Ieskome sandelio pagal jo ID
        warehuose = collection_warehouses.find_one({"_id": warehouseId})

        if warehuose:
            # Istriname sandeli
            collection_warehouses.delete_one({"_id": warehouseId})
            return jsonify({"message": "Warehouse deleted"}), 204
        else:
            return jsonify({"message": "Warehouse not found"}), 404

    # Add product to warehouse invetory
    @app.route('/warehouses/<warehouseId>/inventory', methods=['PUT'])
    def product_to_inventory(warehouseId):
        req = request.get_json()

        # Patikriname, ar nurodyti reikalingi laukai
        if 'productId' not in req or 'quantity' not in req:
            return jsonify({"message": "Invalid input, missing productId or quantity"}), 400
        
        # Patikriname, ar nurodytas sandelis egzistuoja
        warehouse = collection_warehouses.find_one({"_id": warehouseId})
        if not warehouse:
            return jsonify({"message": "Warehouse not found"}), 404
        
        # Patikriname, ar nurodytas produktas egzistuoja
        product = collection_products.find_one({"_id": req['productId']})
        if not product:
            return jsonify({"message": "Product not found"}), 404
        
        # Patikriname, kad idedamos prekes kiekis nevirsytu sandelio talpos ir butu teigiamas skaicius
        requested_quantity = req['quantity']
        if not isinstance(requested_quantity, int) or requested_quantity <= 0 or requested_quantity >= warehouse["capacity"]:
            return jsonify({"message": "Quantity must be a positive integer and less or equal to warehouse capacity"}), 400    
        
        # Sugeneruojamas inventoriaus ID
        inventory_item_id = get_next_sequence("inventory_id")
        
        # Pridedame produkta i inventoriu
        new_invetory_item = {
            "_id": inventory_item_id,
            "productId": req['productId'],
            "quantity": requested_quantity
        }
            
        # Update sandelio dokumenta, kad pridetume inventoriu ir sumazintume sandelio talpa
        new_capacity = warehouse["capacity"] - requested_quantity
        collection_warehouses.update_one(
            {"_id": warehouseId},
            {
                #"$set": {"capacity": new_capacity},   
                "$push": {"inventory": new_invetory_item}
            }
        )

        return jsonify({"message": "Product added to inventory", "id": inventory_item_id}), 201

    # Get inventory of products in warehouse
    @app.route('/warehouses/<warehouseId>/inventory', methods=['GET'])
    def get_inventory(warehouseId):
        # Patikriname, ar sandelis ir inventorius egzistuoja
        warehouse = collection_warehouses.find_one({"_id": warehouseId})
        if not warehouse or not warehouse["inventory"]:
            return jsonify({"message": "Warehouse or inventory not found"}), 404
    
        return jsonify(warehouse["inventory"]), 200

    # Get inventory details
    @app.route('/warehouses/<warehouseId>/inventory/<inventoryId>')
    def get_inventory_details(warehouseId, inventoryId):
        # Patikriname, ar sandelis egzistuoja
        warehouse = collection_warehouses.find_one({"_id": warehouseId})
        if not warehouse:
            return jsonify({"message": "Warehouse not found"}), 404 
        
        # Surasti nurodyta inventoriaus elementa
        invetory_item = next((item for item in warehouse["inventory"] if item["_id"] == inventoryId), None)
        if not invetory_item:
            return jsonify({"message": "Inventory not found"}), 404 
        
        return jsonify(invetory_item), 200

    # Remove product from inventory
    @app.route('/warehouses/<warehouseId>/inventory/<inventoryId>', methods=['DELETE'])
    def del_product_from_inventory(warehouseId, inventoryId):
        # Patikriname, ar nurodytas sandelis egzistuoja
        warehouse = collection_warehouses.find_one({"_id": warehouseId})
        if not warehouse:
            return jsonify({"message": "Warehouse not found"}), 404
        
        # Patikriname, ar nurodytas inventoriaus elementas egzistuoja
        inventory_item = next((item for item in warehouse["inventory"] if item["_id"] == inventoryId), None)
        if not inventory_item:
            return jsonify({"message": "Inventory not found"}), 404
        
        # Atstatome sandelio talpa, pridedami salinamame inventoriaus elemente esanciu produktu kieki
        new_capacity = warehouse["capacity"] + inventory_item["quantity"]

        # Pasaliname inventoriaus elementa
        collection_warehouses.update_one(
            {"_id": warehouseId},
            {
                #"$set": {"capacity": new_capacity},
                "$pull": {"inventory": {"_id": inventoryId}}
            }
        )

        return jsonify({"message": "Product removed from inventory"}), 204
        

    # Get total value of products in warehouse
    @app.route('/warehouses/<warehouseId>/value', methods=['GET'])
    def get_warehouse_value(warehouseId):
        # Patikriname, ar nurodytas sandelis egzistuoja
        warehouse = collection_warehouses.find_one({"_id": warehouseId})
        if not warehouse:
            return jsonify({"message": "Warehouse not found"}), 404
        
        # Aggregation pipline
        pipeline = [
            {
                "$match": {
                    "_id": warehouseId
                }
            },
            {
                "$unwind": "$inventory"
            },
            {
                "$lookup": {
                    "from": "products",
                    "localField": "inventory.productId",
                    "foreignField": "_id",
                    "as": "product"
                }
            },
            {
                "$unwind": {
                    "path": "$product",
                    "preserveNullAndEmptyArrays": True # Jei produktas neegzistuoja
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "value": {
                        "$sum": {
                            "$multiply": ["$inventory.quantity", "$product.price"]
                        }
                    }
                }
            }
        ]

        result = list(collection_warehouses.aggregate(pipeline))

        if not result:
            return jsonify({"value": 0}), 200 # Nera inventoriuje produktu
        
        return jsonify({"value": result[0]["value"]}), 200

    # Get statistics on warehouse capacity
    @app.route('/statistics/warehouse/capacity', methods=['GET'])
    def get_warehouse_capacity():
        # Aggregation pipeline
        pipeline = [
            {
                "$group": {
                    "_id": None,  # Grupuojami visi dokumentai kartu
                    "totalCapacity": {"$sum": "$capacity"},  # Visu sandeliu talpu suma
                    "usedCapacity": {
                        "$sum": {
                            "$sum": {
                                 "$reduce": {
                                    "input": {"$ifNull": ["$inventory", []]},  # If inventory is null, use empty array
                                    "initialValue": 0,
                                    "in": {"$add": ["$$value", "$$this.quantity"]}  # Sum quantities of items in inventory
                                }
                            }
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,  # Isvestyje nereikia _id lauko (todel 0)
                    "totalCapacity": 1,
                    "usedCapacity": 1,
                    "freeCapacity": {
                        "$subtract": ["$totalCapacity", "$usedCapacity"]      
                    }
                }
            }
        ]

        result = list(collection_warehouses.aggregate(pipeline))

        if not result:
            response = {
                "usedCapacity": 0,
                "freeCapacity": 0,
                "totalCapacity": 0
            }
        else:
            response = result[0]
        
        return jsonify(response), 200
    
    # Get statistics on product categories
    @app.route('/statistics/products/by/category', methods=['GET'])
    def product_category_stats():
        # Aggregation pipeline
        pipeline = [
            {
                "$group": {
                    "_id": "$category", # Grupuojama pagal kategorija
                    "count": {"$sum": 1} # Susaiciuojamas kiekvienos kategorijos produktu skaicius
                }
            },
            {
                "$project": {
                    "_id": 0, # Isvestyje nereikalingas _id laukas
                    "category": "$_id", # Pervardiname _id lauka i category
                    "count": 1 # Itraukiame count lauka
                }
            }
        ]

        result = list(collection_products.aggregate(pipeline))

        return jsonify(result), 200

    # # Clear the database
    @app.route('/cleanup', methods=['POST'])
    def clear_database():
        # Isvalome visus duomenis is kolekciju
        try:
            collection_products.delete_many({})
            collection_warehouses.delete_many({})
            collection_counters.delete_many({})
            
            # Inicialize counters after cleanup
            initialize_counters()
 
            return jsonify({"message": "Cleanup completed"}), 200
        except Exception as e:
            return jsonify({"message": "An error occurred while clearing the database", "error": str(e)}), 500


    return app
