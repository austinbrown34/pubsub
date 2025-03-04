from flask import Flask, jsonify, make_response, request, abort
from message_db import MessageDBService
from sns import SNSService
import uuid
import decimal
import flask.json

class MyJSONEncoder(flask.json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            # Convert decimal instances to strings.
            return str(obj)
        return super(MyJSONEncoder, self).default(obj)


app = Flask(__name__)
app.json_encoder = MyJSONEncoder

message_db_service = MessageDBService()
sns_service = SNSService()

@app.route('/')
def pubsub():
    return "PubSub"


@app.route('/v1/messages', methods=['GET'])
def get_messages():
    messages = message_db_service.print_messages(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/category_type_summary', methods=['GET'])
def get_category_type_summary():
    messages = message_db_service.print_category_type_summary(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/stream_summary', methods=['GET'])
def get_stream_summary():
    messages = message_db_service.print_stream_summary(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/stream_type_summary', methods=['GET'])
def get_stream_type_summary():
    messages = message_db_service.print_stream_type_summary(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/type_category_summary', methods=['GET'])
def get_type_category_summary():
    messages = message_db_service.print_type_category_summary(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/type_stream_summary', methods=['GET'])
def get_type_stream_summary():
    messages = message_db_service.print_type_stream_summary(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/type_summary', methods=['GET'])
def get_type_summary():
    messages = message_db_service.print_type_summary(
        **{k: v for k, v in request.args.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/stream/<stream_name>', methods=['GET', 'POST'])
def get_stream_messages(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    messages = message_db_service.get_stream_messages(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/stream/<stream_name>/last', methods=['GET', 'POST'])
def get_last_stream_message(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.get_last_stream_message(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/stream/<stream_name>/version', methods=['GET', 'POST'])
def get_stream_version(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.stream_version(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/stream/<stream_name>/id', methods=['GET', 'POST'])
def get_stream_id(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.id(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/hash_64', methods=['GET', 'POST'])
def hash_64():
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    message = message_db_service.hash_64(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/message_store_version', methods=['GET', 'POST'])
def message_store_version():
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    message = message_db_service.message_store_version(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/stream/<stream_name>/cardinal_id', methods=['GET', 'POST'])
def get_stream_cardinal_id(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.cardinal_id(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/stream/<stream_name>/category', methods=['GET', 'POST'])
def get_stream_category(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.category(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/stream/<stream_name>/is_category', methods=['GET', 'POST'])
def stream_is_category(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.is_category(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/stream/<stream_name>/acquire_lock', methods=['GET', 'POST'])
def stream_acquire_lock(stream_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['stream_name'] = stream_name
    message = message_db_service.acquire_lock(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(message)


@app.route('/v1/category/<category_name>', methods=['GET', 'POST'])
def get_category_messages(category_name):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['category_name'] = category_name
    messages = message_db_service.get_category_messages(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(messages)


@app.route('/v1/write_message', methods=['POST'])
def write_message():
    if not (request.json):
        abort(400)
    id = str(uuid.uuid4())
    request.json['id'] = id
    response = message_db_service.write_message(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/create_topic', methods=['POST'])
def create_topic():
    if not (request.json):
        abort(400)
    response = sns_service.create_topic(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/publish', methods=['POST'])
def publish():
    if not (request.json):
        abort(400)
    response = sns_service.publish(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/subscribe', methods=['POST'])
def subscribe():
    if not (request.json):
        abort(400)
    response = sns_service.subscribe(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/add_permission', methods=['POST'])
def add_permission():
    if not (request.json):
        abort(400)
    response = sns_service.add_permission(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/unsubscribe', methods=['POST'])
def unsubscribe():
    if not (request.json):
        abort(400)
    response = sns_service.unsubscribe(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/delete_topic', methods=['POST'])
def delete_topic():
    if not (request.json):
        abort(400)
    response = sns_service.delete_topic(
        **{k: v for k, v in request.json.items() if v is not None}
    )

    return jsonify(response)


@app.route('/v1/subscription/<subscription_arn>/attributes', methods=['GET', 'POST'])
def subscription_attributes(subscription_arn):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['subscription_arn'] = subscription_arn
    if 'attribute_name' in params and 'attribute_value' in params:
        response = sns_service.set_subscription_attributes(
            **{k: v for k, v in params.items() if v is not None}
        )
    else:
        response = sns_service.get_subscription_attributes(
            **{k: v for k, v in params.items() if v is not None}
        )
    return jsonify(response)


@app.route('/v1/topic/<topic_arn>/attributes', methods=['GET', 'POST'])
def topic_attributes(topic_arn):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['topic_arn'] = topic_arn
    if 'attribute_name' in params and 'attribute_value' in params:
        response = sns_service.set_topic_attributes(
            **{k: v for k, v in params.items() if v is not None}
        )
    else:
        response = sns_service.get_topic_attributes(
            **{k: v for k, v in params.items() if v is not None}
        )
    return jsonify(response)


@app.route('/v1/subscriptions', methods=['GET', 'POST'])
def list_subscriptions():
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    response = sns_service.list_subscriptions(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(response)


@app.route('/v1/topics', methods=['GET', 'POST'])
def list_topics():
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    response = sns_service.list_topics(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(response)


@app.route('/v1/topic/<topic_arn>/subscriptions', methods=['GET', 'POST'])
def list_subscriptions_by_topic(topic_arn):
    params = request.args.copy()
    if request.method == 'POST':
        if not (request.json):
            abort(400)
        params = request.json()
    params['topic_arn'] = topic_arn
    response = sns_service.list_subscriptions_by_topic(
        **{k: v for k, v in params.items() if v is not None}
    )
    return jsonify(response)


if __name__ == '__main__':
    app.run()
