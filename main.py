from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException, TIMESTAMP_NOT_AVAILABLE, \
    OFFSET_INVALID, libversion
import logging
from configparser import ConfigParser
import json
from collections import namedtuple
import argparse
from kafka import BrokerConnection
from kafka.protocol.admin import *
import socket
from kafka import KafkaAdminClient
from six import iteritems


config = ConfigParser()
config.read("config")

logging.basicConfig()


def list_groups(bootstrap_server):
    brokers = bootstrap_server.split(':')[0]
    port = bootstrap_server.split(':')[1]
    bc = BrokerConnection(brokers, port, socket.AF_INET)
    bc.connect_blocking()
    list_groups_request = ListGroupsRequest_v1()
    future = bc.send(list_groups_request)

    while not future.is_done:
        for resp, f in bc.recv():
            f.success(resp)

    for group in future.value.groups:
        print(group)


def describe_group(bootstrap_server, consumer_group_name):
    kafka_admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_server)
    consumer_offset = {}
    for br in kafka_admin_client._client.cluster.brokers():
        this_group_offset = kafka_admin_client.list_consumer_group_offsets(
            group_id=consumer_group_name, group_coordinator_id=1001)
        for (topic, partition), (offset, metadata) in iteritems(this_group_offset):
            consumer_offset[partition] = offset
    namedtuple(consumer_group_name, consumer_offset)
    print(consumer_group_name)

def print_config(config, depth):
    print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigSource(x.source))
            for x in iter(config.synonyms.values())]))


def describe_configs(a, args):
    """ describe configs """

    resources = [ConfigResource(restype, resname) for
                 restype, resname in zip(args[0::2], args[1::2])]

    fs = a.describe_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                print_config(config, 1)

        except KafkaException as e:
            print("Failed to describe {}: {}".format(res, e))
        except Exception:
            raise


def create_topic(kafkaAdmin):
    try:
        topic_config_file = open('topics.json', 'r')
        topics_config = topic_config_file.read()
        topics_config_obj = json.loads(topics_config, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        total_topics = len(topics_config_obj.topics)
        new_topic = [NewTopic(topic.name, num_partitions=topic.partitions, replication_factor=topic.replication_factor)
                     for topic in topics_config_obj.topics]
        fs = kafkaAdmin.create_topics(new_topic)
        for topic, f in fs.items():
            try:
                f.result()
                print("Topic {} created.".format(topic))
            except Exception as e:
                print("Failed to create topic {} with Exception {} ".format(topic,e))
    except FileNotFoundError:
        print(FileNotFoundError)


def delete_topic(kafkaAdmin, topics):
    td = kafkaAdmin.delete_topics(topics, operation_timeout=30)
    for topic, f in td.items():
        try:
            f.result()
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {} : {}".format(topic,e))


def list_all_topics(kafkaAdmin):
    try:
        all_topics = list(kafkaAdmin.list_topics().topics.keys())
        return all_topics
    except Exception as e:
        print(e)


def list_topic(kakfkAdmin, topic_name):
    all_topics = list_all_topics(kafkaAdmin)
    if topic_name[0] == 'all':
        print(all_topics)
    else:
        for topic in topic_name:
            if topic in all_topics:
                try:
                    td = kafkaAdmin.list_topics(topic)
                    print(td.topics)
                except KafkaException:
                    print(KafkaException)
            else:
                print("Topic does't exists.")

# class ConsumerConfig:
#     def __init__(self, topics, group_id, socket_timeout_ms=100, session_timeout_ms=1000):
#         # self.brokers = brokers
#         # self.group_id = group_id
#         self.SOCKET_TIMEOUT_MS = socket_timeout_ms
#         self.SESSION_TIMEOUT_MS = session_timeout_ms


class KConsumer:
    def __init__(self, group_id="Kdefalut", brokers='localhost:32769', session_timeout=10000, socket_timeout=1000):
        self.consumer_config = dict()
        self.consumer_config['bootstrap.servers'] = brokers
        self.consumer_config['group.id'] = group_id
        self.consumer_config['socket.timeout.ms'] = socket_timeout
        self.consumer_config['session.timeout.ms'] = session_timeout
        self.kc = None

    def create_session(self):
        try:
            self.kc = Consumer(self.consumer_config)
        except KafkaError:
            print(KafkaError)

    def consume(self, topic):
        self.kc.subscribe(topics=[topic])
        msg = self.kc.consume(num_messages=1, timeout=-1)
        # if msg.error:
        #     raise Exception(msg.error)
        # else:
        #     print(msg)
    # def lag_report(self):
        print(msg[0].value())
        print(len(msg))
        print(type(msg[0].error()))


if __name__ == "__main__":
    broker = config.get('kafka', 'brokers')
    kafkaAdmin = AdminClient({'bootstrap.servers': broker})
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", '--print-', action='store_true', help='Create the topics')
    parser.add_argument("-c", '--create', action='store_true', help='Create the topics')
    parser.add_argument("-l", '--list', nargs='+', help='list topics')
    parser.add_argument("-d", '--delete', nargs='+', help='Delete the topics')
    parser.add_argument("-g", "--groups", action="store_true", help="List all the groups")
    parser.add_argument("-dg", "--describe-group", nargs="+", help="print the current offsets of given consumer group")
    args = parser.parse_args()
    if args.create:
        create_topic(kafkaAdmin)
    if args.delete:
        delete_topic(kafkaAdmin, args.delete)
    if args.list:
        list_topic(kafkaAdmin, args.list)
    if args.groups:
        list_groups(broker)
    if args.describe_group:
        describe_group(broker, args.describe_group[0])
    # if args.print:
    #     resource_type = input("Please enter the resource type :- ")
    #     resource_name = input("Please enter the resource name :- ")
    #     describe_configs(kafkaAdmin, resource_type)

    consumer_config = {
        'bootstrap.servers': broker
    }
    # consumer = KConsumer(group_id='lag_test1')
    # consumer.create_session()
    # consumer.consume('test')
    # print(args.create)