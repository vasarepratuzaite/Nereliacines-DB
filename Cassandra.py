from cassandra.cluster import Cluster
import werkzeug
from flask import (Flask, request, jsonify, abort)
import uuid
from datetime import datetime

def get_cassandra_session():
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect('chat_app')
    return session

def create_app():
    app = Flask(__name__)
    session = get_cassandra_session()

    # REGISTER A NEW CHANNEL
    @app.route('/channels', methods=['PUT'])
    def register_channel():
        req = request.get_json()

        # Jei pateikiamas id, naudojame ji, jei ne - sugeneruojamas uuid
        if 'id' not in req:
            id = str(uuid.uuid4())
        else:
            id = req.get("id")

        # Patikrinimas, ar id jau egzistuoja
        select = "SELECT * FROM chat_app.channels WHERE id = %s"
        exists = session.execute(select, (id,)).one()

        if exists:
            return jsonify({"message": "The channel with such id already exists."}), 404

        elif 'owner' not in req:
            return jsonify({"message": "Invalid input, missin gname of the owner."}), 400
        else:
            owner = req.get("owner")
            topic = req.get("topic")
        
            query = "INSERT INTO chat_app.channels (id, owner, topic) VALUES (%s, %s, %s) IF NOT EXISTS"
            session.execute(query, (id, owner, topic))

            member_id = str(uuid.uuid4())
            member_insert_query = "INSERT INTO chat_app.members (id, channel_id, member) VALUES (%s, %s, %s) IF NOT EXISTS"
            session.execute(member_insert_query, (member_id, id, owner))

            return jsonify({"id": str(id), "owner": owner, "topic": topic}), 201
               
        
    # GET CHANNEL BY ID
    @app.route('/channels/<channelId>', methods=['GET'])
    def get_channel(channelId):
        query = "SELECT * FROM chat_app.channels WHERE id = %s"
        row = session.execute(query, (channelId,)).one()

        if row:
            return jsonify({"id": str(row.id), "owner": row.owner, "topic": row.topic}), 200
        else:
            return jsonify({"message": "Channel not found"}), 404

    # DELETE CHANNEL BY ID
    @app.route('/channels/<channelId>', methods=['DELETE'])
    def delete_channel(channelId):
        select = "SELECT * FROM chat_app.channels WHERE id = %s"
        exists = session.execute(select, (channelId,)).one()
        
        if exists:
            select1 = "SELECT id FROM chat_app.messages WHERE channel_id = %s"
            message_ids = session.execute(select1, (channelId,))
            if message_ids:
                for row in message_ids:
                    query1 = "DELETE FROM chat_app.messages WHERE channel_id = %s AND id = %s IF EXISTS"
                    session.execute(query1, (channelId, row.id))

            select2 = "SELECT timestamp FROM chat_app.messages_by_channel WHERE channel_id = %s"
            timestamps = session.execute(select2, (channelId,))
            if timestamps:
                for row in timestamps:
                    select4 = "SELECT id FROM chat_app.messages_by_channel WHERE channel_id = %s AND timestamp = %s"
                    author_ids = session.execute(select4, (channelId, row.timestamp))
                    for id in author_ids:
                        query2 = "DELETE FROM chat_app.messages_by_channel WHERE channel_id = %s AND timestamp = %s AND id = %s IF EXISTS"
                        session.execute(query2, (channelId, row.timestamp, id.id))

            select3 = "SELECT member FROM chat_app.members WHERE channel_id = %s"
            member_ids = session.execute(select3, (channelId,))
            for row in member_ids:
                query3 = "DELETE FROM chat_app.members WHERE channel_id = %s AND member = %s IF EXISTS"
                session.execute(query3, (channelId, row.member))

            query = "DELETE FROM chat_app.channels WHERE id = %s IF EXISTS"
            session.execute(query, (channelId,))

            return jsonify({"message": "Channel deleted"}), 204
        else:
            return jsonify({"message": "Channel not found"}), 404
                              

    # ADD MESSAGE TO CHANNEL
    @app.route('/channels/<channelId>/messages', methods=['PUT'])
    def add_message(channelId):
        req = request.get_json()
        id = str(uuid.uuid4())
        text = req.get("text")
        author = req.get("author")
        
        if not text or not author:
            return jsonify({"message": "Invalid input, missing text or author"}), 400
        else:
            # laikas pateikiamas milisekundemis nuo 1970 metu sausio 1 d. 00:00:00 UTC
            timestamp = int(datetime.utcnow().timestamp() * 1000)
            # PRideti zinute i messages lentele
            query = "INSERT INTO chat_app.messages (id, channel_id, text, author, timestamp) VALUES (%s, %s, %s, %s, %s) IF NOT EXISTS"
            session.execute(query, (id, channelId, text, author, timestamp))

            # Prideti zinute i messages_by_cannel lentele
            query_by_channel = "INSERT INTO chat_app.messages_by_channel (id, channel_id, timestamp, text, author) VALUES (%s, %s, %s, %s, %s)  IF NOT EXISTS"
            session.execute(query_by_channel, (id, channelId, timestamp, text, author))

            return jsonify({"message": "Message added"}), 201        

    # GET MESSAGES FROM CHANNEL
    @app.route('/channels/<channelId>/messages', methods=['GET'])
    def get_messages(channelId):
        startAt = request.args.get("startAt")
        author = request.args.get("author")

        # Bazine uzklausa
        query = "SELECT text, author, timestamp FROM chat_app.messages_by_channel WHERE channel_id = %s"
        params = [channelId]

        # jei pateiktas 'startAt', filtruojama pagal timestamp
        if startAt:
            startAt = int(startAt)
            query += " AND timestamp >= %s"
            params.append(startAt)

        # jei pateiktas 'author' filtruojama pagal author
        if author:
            query += " AND author = %s"
            params.append(author)

        rows = session.execute(query, tuple(params))

        messages = []
        for row in rows:
            messages.append({
                "text": row.text,
                "author": row.author,
                "timestamp": row.timestamp
            })

        return jsonify(messages), 200

    # ADD MEMBER TO CHANNEL
    @app.route('/channels/<channelId>/members', methods=['PUT'])
    def add_member(channelId):
        req = request.get_json()
        id = str(uuid.uuid4())
        member = req.get("member")

        select = "SELECT * FROM chat_app.members WHERE channel_id = %s AND member = %s"
        exists = session.execute(select, (channelId, member)).one()

        if exists:
            return jsonify({"message": "The member is already in the channel."}), 400
        elif not member:
            return jsonify({"message": "Invalid input, missing member."}), 400
        else:
            query = "INSERT INTO chat_app.members (id, channel_id, member) VALUES (%s, %s, %s) IF NOT EXISTS"
            session.execute(query, (id, channelId, member))
            return jsonify({"message": "Member added"}), 201

    # GET MEMBERS OF CHANNEL
    @app.route('/channels/<channelId>/members', methods=['GET'])
    def get_members(channelId):
        query = "SELECT * FROM chat_app.members WHERE channel_id = %s"
        rows = session.execute(query, (channelId,))

        if rows:
            members = []
            for row in rows:
                members.append(row.member)

            return jsonify(members), 200
        else:
            return jsonify({"message": "Channel not found"}), 404

    # REMOVE MEMBERS FROM CHANNEL
    @app.route('/channels/<channelId>/members/<member>', methods=['DELETE'])
    def remove_member(channelId, member):
        select = "SELECT member FROM chat_app.members WHERE channel_id = %s AND member = %s"
        member_name = session.execute(select, (channelId, member)).one()


        if not member_name:
           return jsonify({"message": "Member not found"}), 404
        
        # Pasalinti dalyvi is members lenteles
        delete_member_query = "DELETE FROM chat_app.members WHERE channel_id = %s AND member = %s IF EXISTS"
        session.execute(delete_member_query, (channelId, member))

        return jsonify({"message": "Member removed"}), 204
        
    # ISVALYTI LENTELES
    @app.route('/cleanup', methods=['POST'])
    def cleanup():
        session.execute("TRUNCATE chat_app.channels")
        session.execute("TRUNCATE chat_app.messages")
        session.execute("TRUNCATE chat_app.members")
        session.execute("TRUNCATE chat_app.messages_by_channel")
        return jsonify({"message": "cleanup is done!"})

    return app
