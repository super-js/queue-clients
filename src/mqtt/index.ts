import {ICreateQueueClient, QueueClient} from "../queue-client";
import * as mqtt from "mqtt";
import {
    IPublishResponse,
    ISubscribeResponse,
    ITopicPublishRequest,
    ITopicSubscribeRequest,
    IUnsubscribeResponse
} from "../topic";

export interface ICreateMqttQueueClient extends ICreateQueueClient {
    protocol?: 'wss' | 'ws' | 'mqtt' | 'mqtts' | 'tcp' | 'ssl' | 'wx' | 'wxs'
}

export interface IMqttQueueClientConstructor extends ICreateMqttQueueClient {
    clientName: string;
}

export class MqttQueueClient extends QueueClient {

    private readonly _mqttClient: mqtt.MqttClient;

    publisher = (request: ITopicPublishRequest) => {
        return new Promise<IPublishResponse>((resolve, reject) => {
            this._mqttClient.publish(request.topicPath, request.message, {
                qos: 1,
                ...(request.options || {})
            }, (err, packet) => {
                if(err) return reject(err);

                return resolve({
                    published : true
                })
            })
        })
    }

    subscriber = (request: ITopicSubscribeRequest) => {
        return new Promise<ISubscribeResponse>((resolve, reject) => {
            this._mqttClient.subscribe(request.topicPath, {
                qos: 1,
                ...(request.options || {})
            }, (err, subscriptionInfo) => {
                if(err) return reject(err);

                return resolve({
                    subscribed : true,
                    subscriptionInfo: subscriptionInfo.length > 0 ? subscriptionInfo[0] : null
                });
            })
        })
    }

    unsubscriber = (topicPath: string) => {
        return new Promise<IUnsubscribeResponse>((resolve, reject) => {
            this._mqttClient.unsubscribe(topicPath, {}, (err, packet) => {
                if(err) return reject(err);

                return resolve({
                    unsubscribed: true
                });
            })
        })
    }

    constructor(options: IMqttQueueClientConstructor) {
        super(options);

        this._mqttClient = mqtt.connect({
            host: this.host,
            port: this.port,
            protocol: options.protocol || 'mqtt',
            username: options.user,
            password: options.password,
            clientId: options.clientName
        });

        this._mqttClient.on('message', (topicPath, message) => {
            this.onMessage(topicPath, message);
        }).on('error', error => this._lastError = error)
    }

    static async create(clientName, options: ICreateMqttQueueClient) {
        const mqttQueueClient = new MqttQueueClient({
            ...options,
            clientName
        });

        if(options.waitForConnection) await mqttQueueClient.waitForConnection(options.waitForConnectionTimeout);

        return mqttQueueClient;
    }

    async close() {
        return new Promise<boolean>((resolve, reject) => {
            this._mqttClient.end(true, {}, () => {
                resolve(true)
            })
        })
    }

    get isConnected(): boolean {
        return this._mqttClient && this._mqttClient.connected;
    }
}