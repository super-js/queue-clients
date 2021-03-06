jest.setTimeout(30 * 1000)

import {MqttQueueClient} from "./mqtt";

describe("MqttQueueClient", () => {

    const getClient = () => MqttQueueClient.create('test_client',{
        host: 'localhost',
        port: 1883,
        // waitForConnection: true,
        // waitForConnectionTimeout: 10,
        user: 'user',
        password: 'password'
    })

    it('should connect to the broker', async () => {
        const mqttClient = await getClient();

        expect(mqttClient.isConnected).toBeTruthy();
    })

    it('should close connection to the broker', async () => {
        const mqttClient = await getClient();

        await mqttClient.close();

        expect(mqttClient.isConnected).toBe(false);
    })

    it('should publish a message', async () => {
        const mqttClient = await getClient();

        const topic = await mqttClient.topicManager.addTopic('test/topicNo1', {
            subscribe: true
        });

        topic.on('jsonMessage', console.log);
        topic.on('message', console.log);
        topic.on('stringMessage', console.log);

        const {published} = await topic.publish({
            message: JSON.stringify({test: [213, 43243, 342]})
        });


        await mqttClient.close()

        expect(published && topic.noOfPublishedMessages === 1).toBeTruthy();
    })

    it('should subscribe to a topic', async () => {
    })
})