from kafka import KafkaConsumer
import json
import argparse

# creating arguments for passing parameters from command line
parser = argparse.ArgumentParser(description='Script')
parser.add_argument("--opt1")
parser.add_argument("--topic")

args = parser.parse_args()

opt1_value = args.opt1
topic_name = args.topic

def messageValidator(text):
 if text.find(opt1_value) != -1:
     print(" Found '" + opt1_value + "' ! \n" + text)
     quit()


if __name__ == '__main__':
    # Opening a file
    file = open('myfile.txt', 'w')
    #creating kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers= "localhost:9092",
        auto_offset_reset = "earliest",
        group_id="consumer-group-a"
    )
    #printing produced messages and writing them into the file
    for msg in consumer:
        text = "{}".format(json.loads(msg.value))
        file.write(text + '\n')
        messageValidator(text)
    # Closing file
    file.close()


