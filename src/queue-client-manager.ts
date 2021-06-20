import {ICreateMqttQueueClient, MqttQueueClient} from "./mqtt";
import {IAddTopicOptions} from "./topic-manager";

export interface IMQTTClients {
    [clientName: string]: MqttQueueClient;
}

export interface IDefaultOptions {
    topics?: { [topicPath: string]: IAddTopicOptions }
}

export interface QueueClientManagerBuildOptions {
    defaultMqttClients: {[clientName: string]: ICreateMqttQueueClient & IDefaultOptions}
}

export class QueueClientManager {

    private _mqttClients: IMQTTClients = {};

    static async build(options?: QueueClientManagerBuildOptions): Promise<QueueClientManager> {
        const queueClientManager = new QueueClientManager();

        if(options?.defaultMqttClients) {
            await Promise.all(
                Object
                    .keys(options.defaultMqttClients)
                    .map(clientName => queueClientManager.addMqttClient(clientName, options.defaultMqttClients[clientName]))
            )
        }

        return queueClientManager;
    }

    public async addMqttClient(clientName: string, options: ICreateMqttQueueClient): Promise<MqttQueueClient> {
        if(this.hasMqttClient(clientName)) return this._mqttClients[clientName];

        const mqttClient = await MqttQueueClient.create(clientName, options);

        this._mqttClients[clientName] = mqttClient;

        return mqttClient;
    }

    public hasMqttClient(clientName: string) {
        return this._mqttClients.hasOwnProperty(clientName);
    }

    public async removeMqttClient(clientName: string): Promise<boolean> {
        if(!this.hasMqttClient(clientName)) return false;

        await this._mqttClients[clientName].close();
        delete this._mqttClients[clientName];

        return true;
    }

    get mqttClients(): IMQTTClients {
        return this._mqttClients;
    }
}